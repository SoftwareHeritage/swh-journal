# Copyright (C) 2018-2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from kafka import KafkaConsumer, KafkaProducer
from subprocess import Popen
from typing import Tuple, Text
from swh.journal.serializers import value_to_kafka, kafka_to_value
from swh.journal.publisher import JournalPublisher

from .conftest import TEST_CONFIG, OBJECT_TYPE_KEYS


def assert_publish_ok(publisher: JournalPublisher,
                      consumer_from_publisher: KafkaConsumer,
                      producer_to_publisher: KafkaProducer,
                      object_type: Text):
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
    objects = [{object_key_id: c[object_key_id.decode()]}
               for c in expected_objects]

    # send message to the publisher
    for obj in objects:
        producer_to_publisher.send(
            '%s.%s' % (TEST_CONFIG['temporary_prefix'], object_type),
            obj
        )

    nb_messages = len(objects)

    # publisher should poll 1 message and send 1 reified object
    publisher.poll(max_messages=nb_messages)

    # then (client reads from the messages from output topic)
    msgs = []
    for num, msg in enumerate(consumer_from_publisher):
        msgs.append((msg.topic, msg.key, msg.value))

        expected_topic = '%s.%s' % (TEST_CONFIG['final_prefix'], object_type)
        assert expected_topic == msg.topic

        expected_key = objects[num][object_key_id]
        assert expected_key == msg.key

        # Transformation is needed due to our back and forth
        # serialization to kafka
        expected_value = kafka_to_value(value_to_kafka(expected_objects[num]))
        assert expected_value == msg.value


def test_publish(
        publisher: JournalPublisher,
        kafka_server: Tuple[Popen, int],
        kafka_consumer: KafkaConsumer,
        producer_to_publisher: KafkaProducer):
    """
    Reading from and writing to the journal publisher should work (contents)

    Args:
        journal_publisher (JournalPublisher): publisher to read and write data
        kafka_consumer (KafkaConsumer): To read data from the publisher
        producer_to_publisher (KafkaProducer): To send data to the publisher

    """
    # retrieve the object types we want to test
    object_types = OBJECT_TYPE_KEYS.keys()
    # Now for each object type, we'll send data to the publisher and
    # check that data is indeed fetched and reified in the publisher's
    # output topics
    for object_type in object_types:
        assert_publish_ok(
            publisher, kafka_consumer, producer_to_publisher, object_type)
