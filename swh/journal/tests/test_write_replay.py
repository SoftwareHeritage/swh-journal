# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import namedtuple

from hypothesis import given, settings, HealthCheck
from hypothesis.strategies import lists

from swh.model.hypothesis_strategies import object_dicts
from swh.storage.in_memory import Storage
from swh.storage import HashCollision

from swh.journal.serializers import (
    key_to_kafka, kafka_to_key, value_to_kafka, kafka_to_value)
from swh.journal.direct_writer import DirectKafkaWriter
from swh.journal.replay import StorageReplayer, OBJECT_TYPES

FakeKafkaMessage = namedtuple('FakeKafkaMessage', 'topic key value')


class MockedDirectKafkaWriter(DirectKafkaWriter):
    def __init__(self):
        self._prefix = 'prefix'


class MockedStorageReplayer(StorageReplayer):
    def __init__(self, object_types=OBJECT_TYPES):
        self._object_types = object_types


@given(lists(object_dicts()))
@settings(suppress_health_check=[HealthCheck.too_slow])
def test_write_replay_same_order(objects):
    queue = []

    def send(topic, key, value):
        key = kafka_to_key(key_to_kafka(key))
        value = kafka_to_value(value_to_kafka(value))
        queue.append(FakeKafkaMessage(topic=topic, key=key, value=value))

    def poll():
        yield from queue

    storage1 = Storage()
    storage1.journal_writer = MockedDirectKafkaWriter()
    storage1.journal_writer.send = send

    for (obj_type, obj) in objects:
        obj = obj.copy()
        if obj_type == 'origin_visit':
            origin_id = storage1.origin_add_one(obj.pop('origin'))
            if 'visit' in obj:
                del obj['visit']
            storage1.origin_visit_add(origin_id, **obj)
        else:
            method = getattr(storage1, obj_type + '_add')
            try:
                method([obj])
            except HashCollision:
                pass

    storage2 = Storage()
    replayer = MockedStorageReplayer()
    replayer.poll = poll
    replayer.fill(storage2)

    for attr_name in ('_contents', '_directories', '_revisions', '_releases',
                      '_snapshots', '_origin_visits', '_origins'):
        assert getattr(storage1, attr_name) == getattr(storage2, attr_name), \
            attr_name


# TODO: add test for hash collision
