# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import functools
import random
from subprocess import Popen
from typing import Tuple

import dateutil
from confluent_kafka import Producer
from hypothesis import strategies, given, settings
import pytest

from swh.storage import get_storage

from swh.journal.client import JournalClient
from swh.journal.serializers import key_to_kafka, value_to_kafka
from swh.journal.replay import process_replay_objects, is_hash_in_bytearray

from .conftest import OBJECT_TYPE_KEYS
from .utils import MockedJournalClient, MockedKafkaWriter


def test_storage_play(
        kafka_prefix: str,
        kafka_server: Tuple[Popen, int]):
    (_, port) = kafka_server
    kafka_prefix += '.swh.journal.objects'

    storage = get_storage('memory', {})

    producer = Producer({
        'bootstrap.servers': 'localhost:{}'.format(port),
        'client.id': 'test producer',
        'enable.idempotence': 'true',
    })

    now = datetime.datetime.now(tz=datetime.timezone.utc)

    # Fill Kafka
    nb_sent = 0
    nb_visits = 0
    for (object_type, (_, objects)) in OBJECT_TYPE_KEYS.items():
        topic = kafka_prefix + '.' + object_type
        for object_ in objects:
            key = bytes(random.randint(0, 255) for _ in range(40))
            object_ = object_.copy()
            if object_type == 'content':
                object_['ctime'] = now
            elif object_type == 'origin_visit':
                nb_visits += 1
                object_['visit'] = nb_visits
            producer.produce(
                topic=topic, key=key_to_kafka(key),
                value=value_to_kafka(object_),
            )
            nb_sent += 1

    producer.flush()

    # Fill the storage from Kafka
    config = {
        'brokers': 'localhost:%d' % kafka_server[1],
        'group_id': 'replayer',
        'prefix': kafka_prefix,
        'max_messages': nb_sent,
    }
    replayer = JournalClient(**config)
    worker_fn = functools.partial(process_replay_objects, storage=storage)
    nb_inserted = 0
    while nb_inserted < nb_sent:
        nb_inserted += replayer.process(worker_fn)
    assert nb_sent == nb_inserted

    # Check the objects were actually inserted in the storage
    assert OBJECT_TYPE_KEYS['revision'][1] == \
        list(storage.revision_get(
            [rev['id'] for rev in OBJECT_TYPE_KEYS['revision'][1]]))
    assert OBJECT_TYPE_KEYS['release'][1] == \
        list(storage.release_get(
            [rel['id'] for rel in OBJECT_TYPE_KEYS['release'][1]]))

    origins = list(storage.origin_get(
            [orig for orig in OBJECT_TYPE_KEYS['origin'][1]]))
    assert OBJECT_TYPE_KEYS['origin'][1] == \
        [{'url': orig['url']} for orig in origins]
    for origin in origins:
        origin_url = origin['url']
        expected_visits = [
            {
                **visit,
                'origin': origin_url,
                'date': dateutil.parser.parse(visit['date']),
            }
            for visit in OBJECT_TYPE_KEYS['origin_visit'][1]
            if visit['origin'] == origin['url']
        ]
        actual_visits = list(storage.origin_visit_get(
            origin_url))
        for visit in actual_visits:
            del visit['visit']  # opaque identifier
        assert expected_visits == actual_visits

    contents = list(storage.content_get_metadata(
            [cont['sha1'] for cont in OBJECT_TYPE_KEYS['content'][1]]))
    assert None not in contents
    assert contents == OBJECT_TYPE_KEYS['content'][1]


def _test_write_replay_origin_visit(visits):
    """Helper function to write tests for origin_visit.

    Each visit (a dict) given in the 'visits' argument will be sent to
    a (mocked) kafka queue, which a in-memory-storage backed replayer is
    listening to.

    Check that corresponding origin visits entities are present in the storage
    and have correct values.

    """
    queue = []
    replayer = MockedJournalClient(queue)
    writer = MockedKafkaWriter(queue)

    # Note that flipping the order of these two insertions will crash
    # the test, because the legacy origin_format does not allow to create
    # the origin when needed (type is missing)
    writer.send('origin', 'foo', {
        'url': 'http://example.com/',
        'type': 'git',
    })
    for visit in visits:
        writer.send('origin_visit', 'foo', visit)

    queue_size = len(queue)
    assert replayer.max_messages == 0
    replayer.max_messages = queue_size

    storage = get_storage('memory', {})
    worker_fn = functools.partial(process_replay_objects, storage=storage)
    nb_messages = 0
    while nb_messages < queue_size:
        nb_messages += replayer.process(worker_fn)

    actual_visits = list(storage.origin_visit_get('http://example.com/'))

    assert len(actual_visits) == len(visits), actual_visits

    for vin, vout in zip(visits, actual_visits):
        vin = vin.copy()
        vout = vout.copy()
        assert vout.pop('origin') == 'http://example.com/'
        vin.pop('origin')
        vin.setdefault('type', 'git')
        vin.setdefault('metadata', None)
        assert vin == vout


def test_write_replay_origin_visit():
    """Test origin_visit when the 'origin' is just a string."""
    now = datetime.datetime.now()
    visits = [{
        'visit': 1,
        'origin': 'http://example.com/',
        'date': now,
        'type': 'git',
        'status': 'partial',
        'snapshot': None,
    }]
    _test_write_replay_origin_visit(visits)


def test_write_replay_legacy_origin_visit1():
    """Test origin_visit when there is no type."""
    now = datetime.datetime.now()
    visits = [{
        'visit': 1,
        'origin': 'http://example.com/',
        'date': now,
        'status': 'partial',
        'snapshot': None,
    }]
    with pytest.raises(ValueError, match='too old'):
        _test_write_replay_origin_visit(visits)


def test_write_replay_legacy_origin_visit2():
    """Test origin_visit when 'type' is missing from the visit, but not
    from the origin."""
    now = datetime.datetime.now()
    visits = [{
        'visit': 1,
        'origin': {
            'url': 'http://example.com/',
            'type': 'git',
        },
        'date': now,
        'type': 'git',
        'status': 'partial',
        'snapshot': None,
    }]
    _test_write_replay_origin_visit(visits)


def test_write_replay_legacy_origin_visit3():
    """Test origin_visit when the origin is a dict"""
    now = datetime.datetime.now()
    visits = [{
        'visit': 1,
        'origin': {
            'url': 'http://example.com/',
        },
        'date': now,
        'type': 'git',
        'status': 'partial',
        'snapshot': None,
    }]
    _test_write_replay_origin_visit(visits)


hash_strategy = strategies.binary(min_size=20, max_size=20)


@settings(max_examples=500)
@given(strategies.sets(hash_strategy, min_size=0, max_size=500),
       strategies.sets(hash_strategy, min_size=10))
def test_is_hash_in_bytearray(haystack, needles):
    array = b''.join(sorted(haystack))
    needles |= haystack  # Exhaustively test for all objects in the array
    for needle in needles:
        assert is_hash_in_bytearray(needle, array, len(haystack)) == \
            (needle in haystack)
