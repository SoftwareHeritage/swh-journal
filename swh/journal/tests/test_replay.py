# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import random
from subprocess import Popen
from typing import Tuple

import dateutil
from kafka import KafkaProducer

from swh.storage import get_storage

from swh.journal.serializers import key_to_kafka, value_to_kafka
from swh.journal.replay import StorageReplayer

from .conftest import OBJECT_TYPE_KEYS


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
        'consumer_id': 'replayer',
        'prefix': kafka_prefix,
    }
    replayer = StorageReplayer(**config)
    nb_inserted = replayer.fill(storage, max_messages=nb_sent)
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
