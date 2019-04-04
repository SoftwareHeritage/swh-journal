# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import namedtuple

from hypothesis import given, settings, HealthCheck
from hypothesis.strategies import lists, one_of, composite

from swh.model.hashutil import MultiHash
from swh.storage.in_memory import Storage
from swh.storage.tests.algos.test_snapshot import snapshots, origins
from swh.storage.tests.generate_data_test import gen_raw_content

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


@composite
def contents(draw):
    """Generate valid and consistent content.

    Context: Test purposes

    Args:
        **draw**: Used by hypothesis to generate data

    Returns:
        dict representing a content.

    """
    raw_content = draw(gen_raw_content())
    return {
        'data': raw_content,
        'length': len(raw_content),
        'status': 'visible',
        **MultiHash.from_data(raw_content).digest()
    }


objects = lists(one_of(
    origins().map(lambda x: ('origin', x)),
    snapshots().map(lambda x: ('snapshot', x)),
    contents().map(lambda x: ('content', x)),
))


@given(objects)
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
        method = getattr(storage1, obj_type + '_add')
        method([obj])

    storage2 = Storage()
    replayer = MockedStorageReplayer()
    replayer.poll = poll
    replayer.fill(storage2)

    for attr in ('_contents', '_directories', '_revisions', '_releases',
                 '_snapshots', '_origin_visits', '_origins'):
        assert getattr(storage1, attr) == getattr(storage2, attr), attr
