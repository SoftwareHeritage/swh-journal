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
from kafka import KafkaProducer

from swh.storage import get_storage
from swh.storage.in_memory import ENABLE_ORIGIN_IDS

from swh.journal.client import JournalClient
from swh.journal.serializers import key_to_kafka, value_to_kafka
from swh.journal.replay import process_replay_objects

from .conftest import OBJECT_TYPE_KEYS
from .utils import MockedJournalClient, MockedKafkaWriter


def test_storage_play(
        kafka_prefix: str,
        kafka_server: Tuple[Popen, int]):
    (_, port) = kafka_server
    kafka_prefix += '.swh.journal.objects'

    storage = get_storage('memory', {})

    producer = KafkaProducer(
        bootstrap_servers='localhost:{}'.format(port),
        key_serializer=key_to_kafka,
        value_serializer=value_to_kafka,
        client_id='test producer',
    )

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
            producer.send(topic, key=key, value=object_)
            nb_sent += 1

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
        [{'url': orig['url'], 'type': orig['type']} for orig in origins]
    for origin in origins:
        expected_visits = [
            {
                **visit,
                'origin': origin['id'],
                'date': dateutil.parser.parse(visit['date']),
            }
            for visit in OBJECT_TYPE_KEYS['origin_visit'][1]
            if visit['origin']['url'] == origin['url']
            and visit['origin']['type'] == origin['type']
        ]
        actual_visits = list(storage.origin_visit_get(origin['id']))
        for visit in actual_visits:
            del visit['visit']  # opaque identifier
        assert expected_visits == actual_visits

    contents = list(storage.content_get_metadata(
            [cont['sha1'] for cont in OBJECT_TYPE_KEYS['content'][1]]))
    assert None not in contents
    assert contents == OBJECT_TYPE_KEYS['content'][1]


def test_write_replay_legacy_origin_visit1():
    """Test origin_visit when the 'origin' is just a string."""
    queue = []
    replayer = MockedJournalClient(queue)
    writer = MockedKafkaWriter(queue)

    # Note that flipping the order of these two insertions will crash
    # the test, because the legacy origin_format does not allow to create
    # the origin when needed (type is missing)
    now = datetime.datetime.now()
    writer.send('origin', 'foo', {
        'url': 'http://example.com/',
        'type': 'git',
    })
    writer.send('origin_visit', 'foo', {
        'visit': 1,
        'origin': 'http://example.com/',
        'date': now,
    })

    queue_size = sum(len(partition)
                     for batch in queue
                     for partition in batch.values())

    storage = get_storage('memory', {})
    worker_fn = functools.partial(process_replay_objects, storage=storage)
    nb_messages = 0
    while nb_messages < queue_size:
        nb_messages += replayer.process(worker_fn)

    visits = list(storage.origin_visit_get('http://example.com/'))

    if ENABLE_ORIGIN_IDS:
        assert visits == [{
                'visit': 1,
                'origin': 1,
                'date': now,
            }]
    else:
        assert visits == [{
            'visit': 1,
            'origin': {'url': 'http://example.com/'},
            'date': now,
        }]


def test_write_replay_legacy_origin_visit2():
    """Test origin_visit when 'type' is missing."""
    queue = []
    replayer = MockedJournalClient(queue)
    writer = MockedKafkaWriter(queue)

    now = datetime.datetime.now()
    writer.send('origin', 'foo', {
        'url': 'http://example.com/',
        'type': 'git',
    })
    writer.send('origin_visit', 'foo', {
        'visit': 1,
        'origin': {
            'url': 'http://example.com/',
            'type': 'git',
        },
        'date': now,
    })

    queue_size = sum(len(partition)
                     for batch in queue
                     for partition in batch.values())

    storage = get_storage('memory', {})
    worker_fn = functools.partial(process_replay_objects, storage=storage)
    nb_messages = 0
    while nb_messages < queue_size:
        nb_messages += replayer.process(worker_fn)

    visits = list(storage.origin_visit_get('http://example.com/'))

    if ENABLE_ORIGIN_IDS:
        assert visits == [{
            'visit': 1,
            'origin': 1,
            'date': now,
            'type': 'git',
        }]
    else:
        assert visits == [{
            'visit': 1,
            'origin': {'url': 'http://example.com/'},
            'date': now,
            'type': 'git',
        }]
