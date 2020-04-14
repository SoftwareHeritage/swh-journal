# Copyright (C) 2018-2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import defaultdict

from confluent_kafka import Consumer, Producer, KafkaException

import pytest
from subprocess import Popen
from typing import List, Tuple

from swh.storage import get_storage

from swh.journal.serializers import object_key, kafka_to_key, kafka_to_value
from swh.journal.writer.kafka import KafkaJournalWriter, KafkaDeliveryError

from swh.model.model import Directory, DirectoryEntry, Origin, OriginVisit

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


@pytest.fixture(scope="session")
def large_directories() -> List[Directory]:
    dir_sizes = [1 << n for n in range(21)]  # 2**0 = 1 to 2**20 = 1024 * 1024

    dir_entries = [
        DirectoryEntry(
            name=("%09d" % i).encode(),
            type="file",
            perms=0o100644,
            target=b"\x00" * 20,
        )
        for i in range(max(dir_sizes))
    ]

    return [Directory(entries=dir_entries[:size]) for size in dir_sizes]


def test_write_large_objects(
    kafka_prefix: str,
    kafka_server: Tuple[Popen, int],
    consumer: Consumer,
    large_directories: List[Directory],
):
    kafka_prefix += ".swh.journal.objects"

    # Needed as there is no directories in TEST_OBJECT_DICTS, the consumer
    # isn't autosubscribed to directories.
    consumer.subscribe([kafka_prefix + ".directory"])

    writer = KafkaJournalWriter(
        brokers=["localhost:%d" % kafka_server[1]],
        client_id="kafka_writer",
        prefix=kafka_prefix,
    )

    writer.write_additions("directory", large_directories)

    consumed_messages = consume_messages(consumer, kafka_prefix, len(large_directories))

    for dir, message in zip(large_directories, consumed_messages["directory"]):
        (dir_id, consumed_dir) = message
        assert dir_id == dir.id
        assert consumed_dir == dir.to_dict()


def dir_message_size(directory: Directory) -> int:
    """Estimate the size of a directory kafka message.

    We could just do it with `len(value_to_kafka(directory.to_dict()))`,
    but the serialization is a substantial chunk of the test time here.

    """
    n_entries = len(directory.entries)
    return (
        # fmt: off
        0
        + 1       # header of a 2-element fixmap
        + 1 + 2   # fixstr("id")
        + 2 + 20  # bin8(directory.id of length 20)
        + 1 + 7   # fixstr("entries")
        + 4       # array header
        + n_entries
        * (
            0
            + 1       # header of a 4-element fixmap
            + 1 + 6   # fixstr("target")
            + 2 + 20  # bin8(target of length 20)
            + 1 + 4   # fixstr("name")
            + 2 + 9   # bin8(name of length 9)
            + 1 + 5   # fixstr("perms")
            + 5       # uint32(perms)
            + 1 + 4   # fixstr("type")
            + 1 + 3   # fixstr(type)
        )
        # fmt: on
    )


SMALL_MESSAGE_SIZE = 1024 * 1024


@pytest.mark.parametrize(
    "kafka_server_config_overrides", [{"message.max.bytes": str(SMALL_MESSAGE_SIZE)}]
)
def test_fail_write_large_objects(
    kafka_prefix: str,
    kafka_server: Tuple[Popen, int],
    consumer: Consumer,
    large_directories: List[Directory],
):
    kafka_prefix += ".swh.journal.objects"

    # Needed as there is no directories in TEST_OBJECT_DICTS, the consumer
    # isn't autosubscribed to directories.
    consumer.subscribe([kafka_prefix + ".directory"])

    writer = KafkaJournalWriter(
        brokers=["localhost:%d" % kafka_server[1]],
        client_id="kafka_writer",
        prefix=kafka_prefix,
    )

    expected_dirs = []

    for directory in large_directories:
        if dir_message_size(directory) < SMALL_MESSAGE_SIZE:
            # No error; write anyway, but continue
            writer.write_addition("directory", directory)
            expected_dirs.append(directory)
            continue

        with pytest.raises(KafkaDeliveryError) as exc:
            writer.write_addition("directory", directory)

        assert "Failed deliveries" in exc.value.message
        assert len(exc.value.delivery_failures) == 1

        object_type, key, msg, code = exc.value.delivery_failures[0]

        assert object_type == "directory"
        assert key == directory.id
        assert code == "MSG_SIZE_TOO_LARGE"

    consumed_messages = consume_messages(consumer, kafka_prefix, len(expected_dirs))

    for dir, message in zip(expected_dirs, consumed_messages["directory"]):
        (dir_id, consumed_dir) = message
        assert dir_id == dir.id
        assert consumed_dir == dir.to_dict()


def test_write_delivery_timeout(
    kafka_prefix: str, kafka_server: Tuple[Popen, int], consumer: Consumer
):

    produced = []

    class MockProducer(Producer):
        def produce(self, **kwargs):
            produced.append(kwargs)

    kafka_prefix += ".swh.journal.objects"
    writer = KafkaJournalWriter(
        brokers=["localhost:%d" % kafka_server[1]],
        client_id="kafka_writer",
        prefix=kafka_prefix,
        flush_timeout=1,
        producer_class=MockProducer,
    )

    empty_dir = Directory(entries=[])
    with pytest.raises(KafkaDeliveryError) as exc:
        writer.write_addition("directory", empty_dir)

    assert len(produced) == 1

    assert "timeout" in exc.value.message
    assert len(exc.value.delivery_failures) == 1
    delivery_failure = exc.value.delivery_failures[0]
    assert delivery_failure.key == empty_dir.id
    assert delivery_failure.code == "SWH_FLUSH_TIMEOUT"
