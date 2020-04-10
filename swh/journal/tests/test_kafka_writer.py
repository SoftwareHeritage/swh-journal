# Copyright (C) 2018-2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import defaultdict

from confluent_kafka import Consumer, KafkaException
from subprocess import Popen
from typing import Tuple

from swh.storage import get_storage

from swh.journal.serializers import object_key, kafka_to_key, kafka_to_value
from swh.journal.writer.kafka import KafkaJournalWriter

from swh.model.model import Origin, OriginVisit

from .conftest import TEST_OBJECTS, TEST_OBJECT_DICTS


def consume_messages(consumer, kafka_prefix, expected_messages):
    """Consume expected_messages from the consumer;
    Sort them all into a consumed_objects dict"""
    consumed_messages = defaultdict(list)

    fetched_messages = 0
    retries_left = 1000

    while fetched_messages < expected_messages:
        if retries_left == 0:
            raise ValueError("Timed out fetching messages from kafka")

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
        topic = msg.topic()
        assert topic.startswith(kafka_prefix + "."), "Unexpected topic"
        object_type = topic[len(kafka_prefix + ".") :]

        consumed_messages[object_type].append(
            (kafka_to_key(msg.key()), kafka_to_value(msg.value()))
        )

    return consumed_messages


def assert_all_objects_consumed(consumed_messages):
    """Check whether all objects from TEST_OBJECT_DICTS have been consumed"""
    for object_type, known_values in TEST_OBJECT_DICTS.items():
        known_keys = [object_key(object_type, obj) for obj in TEST_OBJECTS[object_type]]

        (received_keys, received_values) = zip(*consumed_messages[object_type])

        if object_type == "origin_visit":
            for value in received_values:
                del value["visit"]
        elif object_type == "content":
            for value in received_values:
                del value["ctime"]

        for key in known_keys:
            assert key in received_keys

        for value in known_values:
            assert value in received_values


def test_kafka_writer(
    kafka_prefix: str, kafka_server: Tuple[Popen, int], consumer: Consumer
):
    kafka_prefix += ".swh.journal.objects"

    writer = KafkaJournalWriter(
        brokers=[f"localhost:{kafka_server[1]}"],
        client_id="kafka_writer",
        prefix=kafka_prefix,
    )

    expected_messages = 0

    for object_type, objects in TEST_OBJECTS.items():
        writer.write_additions(object_type, objects)
        expected_messages += len(objects)

    consumed_messages = consume_messages(consumer, kafka_prefix, expected_messages)
    assert_all_objects_consumed(consumed_messages)


def test_storage_direct_writer(
    kafka_prefix: str, kafka_server: Tuple[Popen, int], consumer: Consumer
):
    kafka_prefix += ".swh.journal.objects"

    writer_config = {
        "cls": "kafka",
        "brokers": ["localhost:%d" % kafka_server[1]],
        "client_id": "kafka_writer",
        "prefix": kafka_prefix,
    }
    storage_config = {
        "cls": "pipeline",
        "steps": [{"cls": "memory", "journal_writer": writer_config},],
    }

    storage = get_storage(**storage_config)

    expected_messages = 0

    for object_type, objects in TEST_OBJECTS.items():
        method = getattr(storage, object_type + "_add")
        if object_type in (
            "content",
            "directory",
            "revision",
            "release",
            "snapshot",
            "origin",
        ):
            method(objects)
            expected_messages += len(objects)
        elif object_type in ("origin_visit",):
            for obj in objects:
                assert isinstance(obj, OriginVisit)
                storage.origin_add_one(Origin(url=obj.origin))
                visit = method(obj.origin, date=obj.date, type=obj.type)
                expected_messages += 1

                obj_d = obj.to_dict()
                for k in ("visit", "origin", "date", "type"):
                    del obj_d[k]
                storage.origin_visit_update(obj.origin, visit.visit, **obj_d)
                expected_messages += 1
        else:
            assert False, object_type

    consumed_messages = consume_messages(consumer, kafka_prefix, expected_messages)
    assert_all_objects_consumed(consumed_messages)
