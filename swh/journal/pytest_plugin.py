# Copyright (C) 2019-2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import random
import string

from typing import Dict, Iterator
from collections import defaultdict

import pytest

from confluent_kafka import Consumer, Producer, KafkaException

from swh.journal.serializers import object_key, kafka_to_key, kafka_to_value
from swh.journal.tests.journal_data import TEST_OBJECTS, TEST_OBJECT_DICTS


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


@pytest.fixture(scope="function")
def kafka_prefix():
    """Pick a random prefix for kafka topics on each call"""
    return "".join(random.choice(string.ascii_lowercase) for _ in range(10))


@pytest.fixture(scope="function")
def kafka_consumer_group(kafka_prefix: str):
    """Pick a random consumer group for kafka consumers on each call"""
    return "test-consumer-%s" % kafka_prefix


@pytest.fixture(scope="session")
def kafka_server() -> Iterator[str]:
    p = Producer({"test.mock.num.brokers": "1"})

    metadata = p.list_topics()
    brokers = [str(broker) for broker in metadata.brokers.values()]
    assert len(brokers) == 1, "More than one broker found in the kafka cluster?!"

    broker_connstr, broker_id = brokers[0].split("/")
    ip, port_str = broker_connstr.split(":")
    assert ip == "127.0.0.1"
    assert int(port_str)

    yield broker_connstr

    p.flush()


TEST_CONFIG = {
    "consumer_id": "swh.journal.consumer",
    "object_types": TEST_OBJECT_DICTS.keys(),
    "stop_after_objects": 1,  # will read 1 object and stop
    "storage": {"cls": "memory", "args": {}},
}


@pytest.fixture
def test_config(kafka_server: str, kafka_prefix: str):
    """Test configuration needed for producer/consumer

    """
    return {
        **TEST_CONFIG,
        "brokers": [kafka_server],
        "prefix": kafka_prefix + ".swh.journal.objects",
    }


@pytest.fixture
def consumer(
    kafka_server: str, test_config: Dict, kafka_consumer_group: str,
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

    kafka_topics = [
        "%s.%s" % (test_config["prefix"], object_type)
        for object_type in test_config["object_types"]
    ]

    consumer.subscribe(kafka_topics)

    yield consumer

    consumer.close()
