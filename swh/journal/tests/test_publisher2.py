# Copyright (C) 2018-2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from kafka import KafkaConsumer, KafkaProducer
from typing import Dict, Text

from swh.journal.publisher import JournalPublisher

from .conftest import (
    TEST_CONFIG, CONTENTS, REVISIONS  # , RELEASES, ORIGINS
)


OBJECT_TYPE_KEYS = {
    'content': 'sha1',
    'revision': 'id',
    'release': 'id',
}


def assert_publish(publisher: JournalPublisher,
                   consumer_from_publisher: KafkaConsumer,
                   producer_to_publisher: KafkaProducer,
                   expected_objects: Dict,
                   object_type: Text):
    # object type's id label key
    object_key_id = OBJECT_TYPE_KEYS[object_type].encode()
    # objects to send to the publisher
    objects = [{object_key_id: c[object_key_id.decode()]}
               for c in expected_objects]

    # read the output of the publisher
    consumer_from_publisher.subscribe(
        topics=['%s.%s' % (TEST_CONFIG['final_prefix'], object_type)
                for object_type in TEST_CONFIG['object_types']])

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
        print('#### consumer_from_publisher: msg %s: %s ' % (num, msg))
        print('#### consumer_from_publisher: msg.value %s: %s ' % (
            num, msg.value))
        msgs.append((msg.topic, msg.key, msg.value))

        expected_topic = '%s.content' % TEST_CONFIG['final_prefix']
        assert expected_topic == msg.topic

        expected_key = objects[num][object_key_id]
        assert expected_key == msg.key

        expected_value = expected_objects[num]
        # Transformation is needed due to msgpack which encodes keys and values
        value = {}
        for k, v in msg.value.items():
            k = k.decode()
            if k == 'status':
                v = v.decode()
            value[k] = v

        assert expected_value == value


def test_publish(
        publisher: JournalPublisher,
        consumer_from_publisher: KafkaConsumer,
        producer_to_publisher: KafkaProducer):
    """
    Reading from and writing to the journal publisher should work (contents)

    Args:
        journal_publisher (JournalPublisher): publisher to read and write data
        kafka_consumer (KafkaConsumer): To read data from the publisher
        kafka_producer (KafkaProducer): To send data to the publisher

    """
    expected_objects = CONTENTS
    object_type = 'content'
    assert_publish(publisher, consumer_from_publisher, producer_to_publisher,
                   expected_objects, object_type)
