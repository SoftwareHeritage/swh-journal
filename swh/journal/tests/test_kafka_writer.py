# Copyright (C) 2018-2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import defaultdict
import datetime

from confluent_kafka import Consumer, KafkaException
from subprocess import Popen
from typing import List, Tuple

from swh.storage import get_storage

from swh.journal.replay import object_converter_fn
from swh.journal.serializers import (
    kafka_to_key, kafka_to_value
)
from swh.journal.writer.kafka import KafkaJournalWriter

from swh.model.model import Content, Origin, BaseModel

from .conftest import OBJECT_TYPE_KEYS


def assert_written(consumer, kafka_prefix, expected_messages):
    consumed_objects = defaultdict(list)

    fetched_messages = 0
    retries_left = 1000

    while fetched_messages < expected_messages:
        if retries_left == 0:
            raise ValueError('Timed out fetching messages from kafka')

        msg = consumer.poll(timeout=0.01)

        if not msg:
            retries_left -= 1
            continue

        error = msg.error()
        if error is not None:
            if error.fatal():
                raise KafkaException(error)
            retries_left -= 1
            continue

        fetched_messages += 1
        consumed_objects[msg.topic()].append(
            (kafka_to_key(msg.key()), kafka_to_value(msg.value()))
        )

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
            assert object_ in values


def test_kafka_writer(
        kafka_prefix: str,
        kafka_server: Tuple[Popen, int],
        consumer: Consumer):
    kafka_prefix += '.swh.journal.objects'

    config = {
        'brokers': ['localhost:%d' % kafka_server[1]],
        'client_id': 'kafka_writer',
        'prefix': kafka_prefix,
        'producer_config': {
            'message.max.bytes': 100000000,
        }
    }

    writer = KafkaJournalWriter(**config)

    expected_messages = 0

    for (object_type, (_, objects)) in OBJECT_TYPE_KEYS.items():
        for (num, object_) in enumerate(objects):
            if object_type == 'origin_visit':
                object_ = {**object_, 'visit': num}
            if object_type == 'content':
                object_ = {**object_, 'ctime': datetime.datetime.now()}
            writer.write_addition(object_type, object_)
            expected_messages += 1

    assert_written(consumer, kafka_prefix, expected_messages)


def test_storage_direct_writer(
        kafka_prefix: str,
        kafka_server: Tuple[Popen, int],
        consumer: Consumer):
    kafka_prefix += '.swh.journal.objects'

    writer_config = {
        'cls': 'kafka',
        'brokers': ['localhost:%d' % kafka_server[1]],
        'client_id': 'kafka_writer',
        'prefix': kafka_prefix,
        'producer_config': {
            'message.max.bytes': 100000000,
        }
    }
    storage_config = {
        'cls': 'pipeline',
        'steps': [
            {'cls': 'memory', 'journal_writer': writer_config},
        ]
    }

    storage = get_storage(**storage_config)

    expected_messages = 0

    for (object_type, (_, objects)) in OBJECT_TYPE_KEYS.items():
        method = getattr(storage, object_type + '_add')
        if object_type in ('content', 'directory', 'revision', 'release',
                           'snapshot', 'origin'):
            objects_: List[BaseModel]
            if object_type == 'content':
                objects_ = [
                    Content.from_dict({
                        **obj, 'data': b''})
                    for obj in objects
                ]
            else:
                objects_ = [
                    object_converter_fn[object_type](obj)
                    for obj in objects
                ]
            method(objects_)
            expected_messages += len(objects)
        elif object_type in ('origin_visit',):
            for object_ in objects:
                object_ = object_.copy()
                origin_url = object_.pop('origin')
                storage.origin_add_one(Origin(url=origin_url))
                visit = method(origin_url, date=object_.pop('date'),
                               type=object_.pop('type'))
                expected_messages += 1
                storage.origin_visit_update(origin_url, visit.visit, **object_)
                expected_messages += 1
        else:
            assert False, object_type

    assert_written(consumer, kafka_prefix, expected_messages)
