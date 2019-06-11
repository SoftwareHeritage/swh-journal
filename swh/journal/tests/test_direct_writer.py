# Copyright (C) 2018-2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import defaultdict
import datetime
import time

from kafka import KafkaConsumer
from subprocess import Popen
from typing import Tuple

from swh.storage import get_storage

from swh.journal.direct_writer import DirectKafkaWriter
from swh.journal.serializers import value_to_kafka, kafka_to_value

from .conftest import OBJECT_TYPE_KEYS


def assert_written(consumer, kafka_prefix):
    time.sleep(0.1)  # Without this, some messages are missing

    consumed_objects = defaultdict(list)
    for msg in consumer:
        consumed_objects[msg.topic].append((msg.key, msg.value))

    for (object_type, (key_name, objects)) in OBJECT_TYPE_KEYS.items():
        topic = kafka_prefix + '.' + object_type
        (keys, values) = zip(*consumed_objects[topic])
        if key_name:
            assert list(keys) == [object_[key_name] for object_ in objects]
        else:
            pass  # TODO

        if object_type == 'origin_visit':
            for value in values:
                del value['visit']
        elif object_type == 'content':
            for value in values:
                del value['ctime']

        for object_ in objects:
            assert kafka_to_value(value_to_kafka(object_)) in values


def test_direct_writer(
        kafka_prefix: str,
        kafka_server: Tuple[Popen, int],
        consumer: KafkaConsumer):
    kafka_prefix += '.swh.journal.objects'

    config = {
        'brokers': 'localhost:%d' % kafka_server[1],
        'client_id': 'direct_writer',
        'prefix': kafka_prefix,
    }

    writer = DirectKafkaWriter(**config)

    for (object_type, (_, objects)) in OBJECT_TYPE_KEYS.items():
        for (num, object_) in enumerate(objects):
            if object_type == 'origin_visit':
                object_ = {**object_, 'visit': num}
            if object_type == 'content':
                object_ = {**object_, 'ctime': datetime.datetime.now()}
            writer.write_addition(object_type, object_)

    assert_written(consumer, kafka_prefix)


def test_storage_direct_writer(
        kafka_prefix: str,
        kafka_server: Tuple[Popen, int],
        consumer: KafkaConsumer):
    kafka_prefix += '.swh.journal.objects'

    config = {
        'brokers': 'localhost:%d' % kafka_server[1],
        'client_id': 'direct_writer',
        'prefix': kafka_prefix,
    }

    storage = get_storage('memory', {'journal_writer': {
        'cls': 'kafka', 'args': config}})

    for (object_type, (_, objects)) in OBJECT_TYPE_KEYS.items():
        method = getattr(storage, object_type + '_add')
        if object_type in ('content', 'directory', 'revision', 'release',
                           'snapshot', 'origin'):
            if object_type == 'content':
                objects = [{**obj, 'data': b''} for obj in objects]
            method(objects)
        elif object_type in ('origin_visit',):
            for object_ in objects:
                object_ = object_.copy()
                origin_id = storage.origin_add_one(object_.pop('origin'))
                del object_['type']
                visit = method(origin=origin_id, date=object_.pop('date'))
                visit_id = visit['visit']
                storage.origin_visit_update(origin_id, visit_id, **object_)
        else:
            assert False, object_type

    assert_written(consumer, kafka_prefix)
