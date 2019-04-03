# Copyright (C) 2018-2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from kafka import KafkaConsumer, KafkaProducer
from subprocess import Popen
from typing import Tuple

from swh.journal.serializers import value_to_kafka, kafka_to_value
from swh.journal.publisher import JournalPublisher

from .conftest import OBJECT_TYPE_KEYS


def assert_publish_ok(publisher: JournalPublisher,
                      consumer_from_publisher: KafkaConsumer,
                      producer_to_publisher: KafkaProducer,
                      test_config: dict,
                      object_type: str):
    """Assert that publishing object in the publisher is reified and
    published in output topics.

    Args:
        publisher (JournalPublisher): publisher to read and write data
        consumer_from_publisher (KafkaConsumer): To read data from the
                                                 publisher
        producer_to_publisher (KafkaProducer): To send data to the publisher
        object_type (str): Object type to look for (e.g content, revision,
                                                    etc...)

    """
    # object type's id label key
    object_key_id, expected_objects = OBJECT_TYPE_KEYS[object_type]
    # objects to send to the publisher
    if object_key_id:
        objects = [{object_key_id: c[object_key_id]}
                   for c in expected_objects]
    else:
        # TODO: add support for origin and origin_visit
        return

    # send message to the publisher
    for obj in objects:
        producer_to_publisher.send(
            '%s.%s' % (test_config['temporary_prefix'], object_type),
            obj
        )

    nb_messages = len(objects)

    for _ in range(nb_messages):
        publisher.poll(max_messages=1)

    # then (client reads from the messages from output topic)
    expected_topic = '%s.%s' % (test_config['final_prefix'], object_type)
    expected_msgs = [
        (
            object_[object_key_id],
            kafka_to_value(value_to_kafka(object_))
        )
        for object_ in expected_objects]

    msgs = list(consumer_from_publisher)
    assert all(msg.topic == expected_topic for msg in msgs)
    assert [(msg.key, msg.value) for msg in msgs] == expected_msgs


def test_publish(
        publisher: JournalPublisher,
        kafka_server: Tuple[Popen, int],
        test_config: dict,
        consumer_from_publisher: KafkaConsumer,
        producer_to_publisher: KafkaProducer):
    """
    Reading from and writing to the journal publisher should work (contents)

    Args:
        journal_publisher (JournalPublisher): publisher to read and write data
        consumer_from_publisher (KafkaConsumer): To read data from publisher
        producer_to_publisher (KafkaProducer): To send data to publisher

    """
    # retrieve the object types we want to test
    object_types = OBJECT_TYPE_KEYS.keys()
    # Now for each object type, we'll send data to the publisher and
    # check that data is indeed fetched and reified in the publisher's
    # output topics
    for object_type in object_types:
        assert_publish_ok(
            publisher, consumer_from_publisher, producer_to_publisher,
            test_config, object_type)
