# Copyright (C) 2019-2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import defaultdict
import random
import string
from typing import Collection, Dict, Iterator, Optional

import attr
from confluent_kafka import Consumer, KafkaException, Producer
from confluent_kafka.admin import AdminClient
import pytest

from swh.journal.serializers import kafka_to_key, kafka_to_value, pprint_key
from swh.journal.tests.journal_data import TEST_OBJECTS


def consume_messages(consumer, kafka_prefix, expected_messages):
    """Consume expected_messages from the consumer;
    Sort them all into a consumed_objects dict"""
    consumed_messages = defaultdict(list)

    fetched_messages = 0
    retries_left = 1000

    while fetched_messages < expected_messages:
        if retries_left == 0:
            raise ValueError(
                "Timed out fetching messages from kafka. "
                f"Only {fetched_messages}/{expected_messages} fetched"
            )

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
        assert topic.startswith(f"{kafka_prefix}.") or topic.startswith(
            f"{kafka_prefix}_privileged."
        ), "Unexpected topic"
        object_type = topic[len(kafka_prefix + ".") :]

        consumed_messages[object_type].append(
            (kafka_to_key(msg.key()), kafka_to_value(msg.value()))
        )

    return consumed_messages


def assert_all_objects_consumed(
    consumed_messages: Dict, exclude: Optional[Collection] = None
):
    """Check whether all objects from TEST_OBJECTS have been consumed

    `exclude` can be a list of object types for which we do not want to compare the
    values (eg. for anonymized object).

    """
    for object_type, known_objects in TEST_OBJECTS.items():
        known_keys = [obj.unique_key() for obj in known_objects]

        if not consumed_messages[object_type]:
            return

        (received_keys, received_values) = zip(*consumed_messages[object_type])

        if object_type in ("content", "skipped_content"):
            for value in received_values:
                value.pop("ctime", None)
        if object_type == "content":
            known_objects = [attr.evolve(o, data=None) for o in known_objects]

        for key in known_keys:
            assert key in received_keys, (
                f"expected {object_type} key {pprint_key(key)} "
                "absent from consumed messages"
            )

        if exclude and object_type in exclude:
            continue

        for value in known_objects:
            expected_value = value.to_dict()
            if value.object_type in ("content", "skipped_content"):
                expected_value.pop("ctime", None)
            assert expected_value in received_values, (
                f"expected {object_type} value {value!r} is "
                "absent from consumed messages"
            )


@pytest.fixture(scope="function")
def kafka_prefix():
    """Pick a random prefix for kafka topics on each call"""
    return "".join(random.choice(string.ascii_lowercase) for _ in range(10))


@pytest.fixture(scope="function")
def kafka_consumer_group(kafka_prefix: str):
    """Pick a random consumer group for kafka consumers on each call"""
    return "test-consumer-%s" % kafka_prefix


@pytest.fixture(scope="function")
def object_types():
    """Set of object types to precreate topics for."""
    return set(TEST_OBJECTS.keys())


@pytest.fixture(scope="function")
def privileged_object_types():
    """Set of object types to precreate privileged topics for."""
    return {"revision", "release"}


@pytest.fixture(scope="function")
def kafka_server(
    kafka_server_base: str,
    kafka_prefix: str,
    object_types: Iterator[str],
    privileged_object_types: Iterator[str],
) -> str:
    """A kafka server with existing topics

    Unprivileged topics are built as ``{kafka_prefix}.{object_type}`` with object_type
    from the ``object_types`` list.

    Privileged topics are built as ``{kafka_prefix}_privileged.{object_type}`` with
    object_type from the ``privileged_object_types`` list.

    """
    topics = [f"{kafka_prefix}.{obj}" for obj in object_types] + [
        f"{kafka_prefix}_privileged.{obj}" for obj in privileged_object_types
    ]

    # unfortunately, the Mock broker does not support the CreatTopic admin API, so we
    # have to create topics using a Producer.
    producer = Producer(
        {
            "bootstrap.servers": kafka_server_base,
            "client.id": "bootstrap producer",
            "acks": "all",
        }
    )
    for topic in topics:
        producer.produce(topic=topic, value=None)
    for i in range(10):
        if producer.flush(0.1) == 0:
            break

    return kafka_server_base


@pytest.fixture(scope="session")
def kafka_server_base() -> Iterator[str]:
    """Create a mock kafka cluster suitable for tests.

    Yield a connection string.

    Note: this is a generator to keep the mock broker alive during the whole test
    session.

    see https://github.com/edenhill/librdkafka/blob/master/src/rdkafka_mock.h
    """
    admin = AdminClient({"test.mock.num.brokers": "1"})

    metadata = admin.list_topics()
    brokers = [str(broker) for broker in metadata.brokers.values()]
    assert len(brokers) == 1, "More than one broker found in the kafka cluster?!"

    broker_connstr, broker_id = brokers[0].split("/")
    yield broker_connstr


TEST_CONFIG = {
    "consumer_id": "swh.journal.consumer",
    "stop_after_objects": 1,  # will read 1 object and stop
    "storage": {"cls": "memory", "args": {}},
}


@pytest.fixture
def test_config(
    kafka_server_base: str,
    kafka_prefix: str,
    object_types: Iterator[str],
    privileged_object_types: Iterator[str],
):
    """Test configuration needed for producer/consumer

    """
    return {
        **TEST_CONFIG,
        "object_types": object_types,
        "privileged_object_types": privileged_object_types,
        "brokers": [kafka_server_base],
        "prefix": kafka_prefix,
    }


@pytest.fixture
def consumer(
    kafka_server: str, test_config: Dict, kafka_consumer_group: str
) -> Consumer:
    """Get a connected Kafka consumer.

    """
    consumer = Consumer(
        {
            "bootstrap.servers": kafka_server,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
            "group.id": kafka_consumer_group,
        }
    )
    prefix = test_config["prefix"]
    kafka_topics = [
        f"{prefix}.{object_type}" for object_type in test_config["object_types"]
    ] + [
        f"{prefix}_privileged.{object_type}"
        for object_type in test_config["privileged_object_types"]
    ]
    consumer.subscribe(kafka_topics)

    yield consumer

    consumer.close()
