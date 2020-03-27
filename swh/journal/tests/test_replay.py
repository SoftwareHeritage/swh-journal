# Copyright (C) 2019-2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import functools
import logging
import random
from subprocess import Popen
from typing import Dict, List, Tuple

import dateutil
import pytest

from confluent_kafka import Producer
from hypothesis import strategies, given, settings

from swh.storage import get_storage

from swh.journal.client import JournalClient
from swh.journal.serializers import key_to_kafka, value_to_kafka
from swh.journal.replay import process_replay_objects, is_hash_in_bytearray
from swh.model.hashutil import hash_to_hex
from swh.model.model import Content

from .conftest import OBJECT_TYPE_KEYS, DUPLICATE_CONTENTS
from .utils import MockedJournalClient, MockedKafkaWriter


storage_config = {
    'cls': 'pipeline',
    'steps': [
        {'cls': 'memory'},
    ]
}


def make_topic(kafka_prefix: str, object_type: str) -> str:
    return kafka_prefix + '.' + object_type


def test_storage_play(
        kafka_prefix: str,
        kafka_consumer_group: str,
        kafka_server: Tuple[Popen, int],
        caplog):
    """Optimal replayer scenario.

    This:
    - writes objects to the topic
    - replayer consumes objects from the topic and replay them

    """
    (_, port) = kafka_server
    kafka_prefix += '.swh.journal.objects'

    storage = get_storage(**storage_config)

    producer = Producer({
        'bootstrap.servers': 'localhost:{}'.format(port),
        'client.id': 'test producer',
        'acks': 'all',
    })

    now = datetime.datetime.now(tz=datetime.timezone.utc)

    # Fill Kafka
    nb_sent = 0
    nb_visits = 0
    for (object_type, (_, objects)) in OBJECT_TYPE_KEYS.items():
        topic = make_topic(kafka_prefix, object_type)
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

    caplog.set_level(logging.ERROR, 'swh.journal.replay')
    # Fill the storage from Kafka
    replayer = JournalClient(
        brokers='localhost:%d' % kafka_server[1],
        group_id=kafka_consumer_group,
        prefix=kafka_prefix,
        stop_after_objects=nb_sent,
    )
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

    input_contents = OBJECT_TYPE_KEYS['content'][1]
    contents = storage.content_get_metadata(
            [cont['sha1'] for cont in input_contents])
    assert len(contents) == len(input_contents)
    assert contents == {cont['sha1']: [cont] for cont in input_contents}

    collision = 0
    for record in caplog.records:
        logtext = record.getMessage()
        if 'Colliding contents:' in logtext:
            collision += 1

    assert collision == 0, "No collision should be detected"


def test_storage_play_with_collision(
        kafka_prefix: str,
        kafka_consumer_group: str,
        kafka_server: Tuple[Popen, int],
        caplog):
    """Another replayer scenario with collisions.

    This:
    - writes objects to the topic, including colliding contents
    - replayer consumes objects from the topic and replay them
    - This drops the colliding contents from the replay when detected

    """
    (_, port) = kafka_server
    kafka_prefix += '.swh.journal.objects'

    storage = get_storage(**storage_config)

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
        topic = make_topic(kafka_prefix, object_type)
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

    # Create collision in input data
    # They are not written in the destination
    for content in DUPLICATE_CONTENTS:
        topic = make_topic(kafka_prefix, 'content')
        producer.produce(
            topic=topic, key=key_to_kafka(key),
            value=value_to_kafka(content),
        )

        nb_sent += 1

    producer.flush()

    caplog.set_level(logging.ERROR, 'swh.journal.replay')
    # Fill the storage from Kafka
    replayer = JournalClient(
        brokers='localhost:%d' % kafka_server[1],
        group_id=kafka_consumer_group,
        prefix=kafka_prefix,
        stop_after_objects=nb_sent,
    )
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

    input_contents = OBJECT_TYPE_KEYS['content'][1]
    contents = storage.content_get_metadata(
            [cont['sha1'] for cont in input_contents])
    assert len(contents) == len(input_contents)
    assert contents == {cont['sha1']: [cont] for cont in input_contents}

    nb_collisions = 0

    actual_collision: Dict
    for record in caplog.records:
        logtext = record.getMessage()
        if 'Collision detected:' in logtext:
            nb_collisions += 1
            actual_collision = record.args['collision']

    assert nb_collisions == 1, "1 collision should be detected"

    algo = 'sha1'
    assert actual_collision['algo'] == algo
    expected_colliding_hash = hash_to_hex(DUPLICATE_CONTENTS[0][algo])
    assert actual_collision['hash'] == expected_colliding_hash

    actual_colliding_hashes = actual_collision['objects']
    assert len(actual_colliding_hashes) == len(DUPLICATE_CONTENTS)
    for content in DUPLICATE_CONTENTS:
        expected_content_hashes = {
            k: hash_to_hex(v)
            for k, v in Content.from_dict(content).hashes().items()
        }
        assert expected_content_hashes in actual_colliding_hashes


def _test_write_replay_origin_visit(visits: List[Dict]):
    """Helper function to write tests for origin_visit.

    Each visit (a dict) given in the 'visits' argument will be sent to
    a (mocked) kafka queue, which a in-memory-storage backed replayer is
    listening to.

    Check that corresponding origin visits entities are present in the storage
    and have correct values if they are not skipped.

    """
    queue: List = []
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
    assert replayer.stop_after_objects is None
    replayer.stop_after_objects = queue_size

    storage = get_storage(**storage_config)
    worker_fn = functools.partial(process_replay_objects, storage=storage)

    replayer.process(worker_fn)

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
    """Origin_visit with no types should make the replayer crash

    We expect the journal's origin_visit topic to no longer reference such
    visits. If it does, the replayer must crash so we can fix the journal's
    topic.

    """
    now = datetime.datetime.now()
    visit = {
        'visit': 1,
        'origin': 'http://example.com/',
        'date': now,
        'status': 'partial',
        'snapshot': None,
    }
    now2 = datetime.datetime.now()
    visit2 = {
        'visit': 2,
        'origin': {'url': 'http://example.com/'},
        'date': now2,
        'status': 'partial',
        'snapshot': None,
    }

    for origin_visit in [visit, visit2]:
        with pytest.raises(ValueError, match='Old origin visit format'):
            _test_write_replay_origin_visit([origin_visit])


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
