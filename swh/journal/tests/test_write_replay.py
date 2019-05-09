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

FakeKafkaMessage = namedtuple('FakeKafkaMessage', 'key value')
FakeKafkaPartition = namedtuple('FakeKafkaPartition', 'topic')


class MockedDirectKafkaWriter(DirectKafkaWriter):
    def __init__(self):
        self._prefix = 'prefix'


class MockedStorageReplayer(StorageReplayer):
    def __init__(self, object_types=OBJECT_TYPES):
        self._object_types = object_types


@given(lists(object_dicts(), min_size=1))
@settings(suppress_health_check=[HealthCheck.too_slow])
def test_write_replay_same_order(objects):
    committed = False
    queue = []

    def send(topic, key, value):
        key = kafka_to_key(key_to_kafka(key))
        value = kafka_to_value(value_to_kafka(value))
        queue.append({
            FakeKafkaPartition(topic):
                [FakeKafkaMessage(key=key, value=value)]
        })

    def poll():
        return queue.pop(0)

    def commit():
        nonlocal committed
        if queue == []:
            committed = True

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
    replayer.commit = commit
    replayer.fill(storage2, max_messages=len(queue))

    assert committed

    for attr_name in ('_contents', '_directories', '_revisions', '_releases',
                      '_snapshots', '_origin_visits', '_origins'):
        assert getattr(storage1, attr_name) == getattr(storage2, attr_name), \
            attr_name


@given(lists(object_dicts(), min_size=1))
@settings(suppress_health_check=[HealthCheck.too_slow])
def test_write_replay_same_order_batches(objects):
    committed = False
    queue = []

    def send(topic, key, value):
        key = kafka_to_key(key_to_kafka(key))
        value = kafka_to_value(value_to_kafka(value))
        partition = FakeKafkaPartition(topic)
        msg = FakeKafkaMessage(key=key, value=value)
        if queue and {partition} == set(queue[-1]):
            # The last message is of the same object type, groupping them
            queue[-1][partition].append(msg)
        else:
            queue.append({
                FakeKafkaPartition(topic): [msg]
            })

    def poll():
        return queue.pop(0)

    def commit():
        nonlocal committed
        if queue == []:
            committed = True

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

    queue_size = sum(len(partition)
                     for batch in queue
                     for partition in batch.values())

    storage2 = Storage()
    replayer = MockedStorageReplayer()
    replayer.poll = poll
    replayer.commit = commit
    replayer.fill(storage2, max_messages=queue_size)

    assert committed

    for attr_name in ('_contents', '_directories', '_revisions', '_releases',
                      '_snapshots', '_origin_visits', '_origins'):
        assert getattr(storage1, attr_name) == getattr(storage2, attr_name), \
            attr_name


# TODO: add test for hash collision
