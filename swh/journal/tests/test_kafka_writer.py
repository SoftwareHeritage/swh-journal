# Copyright (C) 2018-2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest
from confluent_kafka import Consumer, Producer

from swh.storage import get_storage

from swh.model.model import Directory, Origin, OriginVisit

from swh.journal.tests.journal_data import TEST_OBJECTS
from swh.journal.pytest_plugin import consume_messages, assert_all_objects_consumed
from swh.journal.writer.kafka import KafkaJournalWriter, KafkaDeliveryError


def test_kafka_writer(kafka_prefix: str, kafka_server: str, consumer: Consumer):
    kafka_prefix += ".swh.journal.objects"

    writer = KafkaJournalWriter(
        brokers=[kafka_server], client_id="kafka_writer", prefix=kafka_prefix,
    )

    expected_messages = 0

    for object_type, objects in TEST_OBJECTS.items():
        writer.write_additions(object_type, objects)
        expected_messages += len(objects)

    consumed_messages = consume_messages(consumer, kafka_prefix, expected_messages)
    assert_all_objects_consumed(consumed_messages)


def test_storage_direct_writer(kafka_prefix: str, kafka_server, consumer: Consumer):
    kafka_prefix += ".swh.journal.objects"

    writer_config = {
        "cls": "kafka",
        "brokers": [kafka_server],
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


def test_write_delivery_failure(
    kafka_prefix: str, kafka_server: str, consumer: Consumer
):
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
        brokers=[kafka_server], client_id="kafka_writer", prefix=kafka_prefix,
    )

    empty_dir = Directory(entries=[])
    with pytest.raises(KafkaDeliveryError) as exc:
        writer.write_addition("directory", empty_dir)

    assert "Failed deliveries" in exc.value.message
    assert len(exc.value.delivery_failures) == 1
    delivery_failure = exc.value.delivery_failures[0]
    assert delivery_failure.key == empty_dir.id
    assert delivery_failure.code == "SWH_MOCK_ERROR"


def test_write_delivery_timeout(
    kafka_prefix: str, kafka_server: str, consumer: Consumer
):

    produced = []

    class MockProducer(Producer):
        """A kafka producer which pretends to produce messages, but never sends any
        delivery acknowledgements"""

        def produce(self, **kwargs):
            produced.append(kwargs)

    kafka_prefix += ".swh.journal.objects"
    writer = KafkaJournalWriter(
        brokers=[kafka_server],
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
