# Copyright (C) 2018-2021 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
from typing import Iterable

from confluent_kafka import Consumer, Producer
import pytest

from swh.journal.pytest_plugin import assert_all_objects_consumed, consume_messages
from swh.journal.writer import model_object_dict_sanitizer
from swh.journal.writer.kafka import KafkaDeliveryError, KafkaJournalWriter
from swh.model.model import BaseModel, Directory, Release, Revision
from swh.model.tests.swh_model_data import TEST_OBJECTS


def test_kafka_writer(
    kafka_prefix: str,
    kafka_server: str,
    consumer: Consumer,
    privileged_object_types: Iterable[str],
):
    writer = KafkaJournalWriter(
        brokers=[kafka_server],
        client_id="kafka_writer",
        prefix=kafka_prefix,
        value_sanitizer=model_object_dict_sanitizer,
        anonymize=False,
    )

    expected_messages = 0

    for object_type, objects in TEST_OBJECTS.items():
        writer.write_additions(object_type, objects)
        expected_messages += len(objects)

    consumed_messages = consume_messages(consumer, kafka_prefix, expected_messages)
    assert_all_objects_consumed(consumed_messages)

    for key, obj_dict in consumed_messages["revision"]:
        obj = Revision.from_dict(obj_dict)
        for person in (obj.author, obj.committer):
            assert not (
                len(person.fullname) == 32
                and person.name is None
                and person.email is None
            )
    for key, obj_dict in consumed_messages["release"]:
        obj = Release.from_dict(obj_dict)
        # author is optional for release
        if obj.author is None:
            continue
        for person in (obj.author,):
            assert not (
                len(person.fullname) == 32
                and person.name is None
                and person.email is None
            )


def test_kafka_writer_anonymized(
    kafka_prefix: str,
    kafka_server: str,
    consumer: Consumer,
    privileged_object_types: Iterable[str],
):
    writer = KafkaJournalWriter(
        brokers=[kafka_server],
        client_id="kafka_writer",
        prefix=kafka_prefix,
        value_sanitizer=model_object_dict_sanitizer,
        anonymize=True,
    )

    expected_messages = 0

    for object_type, objects in TEST_OBJECTS.items():
        writer.write_additions(object_type, objects)
        expected_messages += len(objects)
        if object_type in privileged_object_types:
            expected_messages += len(objects)

    consumed_messages = consume_messages(consumer, kafka_prefix, expected_messages)
    assert_all_objects_consumed(consumed_messages, exclude=["revision", "release"])

    for key, obj_dict in consumed_messages["revision"]:
        obj = Revision.from_dict(obj_dict)
        for person in (obj.author, obj.committer):
            assert (
                len(person.fullname) == 32
                and person.name is None
                and person.email is None
            )
    for key, obj_dict in consumed_messages["release"]:
        obj = Release.from_dict(obj_dict)
        # author is optional for release
        if obj.author is None:
            continue
        for person in (obj.author,):
            assert (
                len(person.fullname) == 32
                and person.name is None
                and person.email is None
            )


def test_write_delivery_failure(kafka_prefix: str, kafka_server: str):
    class MockKafkaError:
        """A mocked kafka error"""

        def str(self):
            return "Mocked Kafka Error"

        def name(self):
            return "SWH_MOCK_ERROR"

    class KafkaJournalWriterFailDelivery(KafkaJournalWriter):
        """A journal writer which always fails delivering messages"""

        def _on_delivery(self, error, message):
            """Replace the inbound error with a fake delivery error"""
            super()._on_delivery(MockKafkaError(), message)

    kafka_prefix += ".swh.journal.objects"
    writer = KafkaJournalWriterFailDelivery(
        brokers=[kafka_server],
        client_id="kafka_writer",
        prefix=kafka_prefix,
        value_sanitizer=model_object_dict_sanitizer,
    )

    empty_dir = Directory(entries=())
    with pytest.raises(KafkaDeliveryError) as exc:
        writer.write_addition("directory", empty_dir)

    assert "Failed deliveries" in exc.value.message
    assert len(exc.value.delivery_failures) == 1
    delivery_failure = exc.value.delivery_failures[0]
    assert delivery_failure.key == empty_dir.id
    assert delivery_failure.code == "SWH_MOCK_ERROR"


def test_write_delivery_timeout(kafka_prefix: str, kafka_server: str):

    produced = []

    class MockProducer(Producer):
        """A kafka producer which pretends to produce messages, but never sends any
        delivery acknowledgements"""

        def produce(self, **kwargs):
            produced.append(kwargs)

    writer = KafkaJournalWriter(
        brokers=[kafka_server],
        client_id="kafka_writer",
        prefix=kafka_prefix,
        value_sanitizer=model_object_dict_sanitizer,
        flush_timeout=1,
        producer_class=MockProducer,
    )

    empty_dir = Directory(entries=())
    with pytest.raises(KafkaDeliveryError) as exc:
        writer.write_addition("directory", empty_dir)

    assert len(produced) == 1

    assert "timeout" in exc.value.message
    assert len(exc.value.delivery_failures) == 1
    delivery_failure = exc.value.delivery_failures[0]
    assert delivery_failure.key == empty_dir.id
    assert delivery_failure.code == "SWH_FLUSH_TIMEOUT"


class MockBufferErrorProducer(Producer):
    """A Kafka producer that returns a BufferError on the `n_buffererrors`
    first calls to produce."""

    def __init__(self, *args, **kwargs):
        self.n_buffererrors = kwargs.pop("n_bufferrors", 0)
        self.produce_calls = 0

        super().__init__(*args, **kwargs)

    def produce(self, **kwargs):
        self.produce_calls += 1
        if self.produce_calls <= self.n_buffererrors:
            raise BufferError("Local: Queue full")

        self.produce_calls = 0
        return super().produce(**kwargs)


def test_write_BufferError_retry(kafka_prefix: str, kafka_server: str, caplog):
    writer = KafkaJournalWriter(
        brokers=[kafka_server],
        client_id="kafka_writer",
        prefix=kafka_prefix,
        value_sanitizer=model_object_dict_sanitizer,
        flush_timeout=1,
        producer_class=MockBufferErrorProducer,
    )

    writer.producer.n_buffererrors = 4

    empty_dir = Directory(entries=())

    caplog.set_level(logging.DEBUG, "swh.journal.writer.kafka")
    writer.write_addition("directory", empty_dir)
    records = []
    for record in caplog.records:
        if "BufferError" in record.getMessage():
            records.append(record)

    assert len(records) == writer.producer.n_buffererrors


def test_write_BufferError_give_up(kafka_prefix: str, kafka_server: str, caplog):
    writer = KafkaJournalWriter(
        brokers=[kafka_server],
        client_id="kafka_writer",
        prefix=kafka_prefix,
        value_sanitizer=model_object_dict_sanitizer,
        flush_timeout=1,
        producer_class=MockBufferErrorProducer,
    )

    writer.producer.n_buffererrors = 5

    empty_dir = Directory(entries=())

    with pytest.raises(KafkaDeliveryError):
        writer.write_addition("directory", empty_dir)


def test_write_addition_errors_without_unique_key(kafka_prefix: str, kafka_server: str):
    writer = KafkaJournalWriter(
        brokers=[kafka_server],
        client_id="kafka_writer",
        prefix=kafka_prefix,
        value_sanitizer=model_object_dict_sanitizer,
    )

    with pytest.raises(NotImplementedError):
        writer.write_addition("BaseModel", BaseModel())
