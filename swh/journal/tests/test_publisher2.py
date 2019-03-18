# Copyright (C) 2018-2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from kafka import KafkaConsumer, KafkaProducer
from swh.journal.publisher import JournalPublisher

from .conftest import (
    TEST_CONFIG, CONTENTS  # , REVISIONS, RELEASES, ORIGINS
)


def test_publisher(
        publisher: JournalPublisher,
        consumer_from_publisher: KafkaConsumer,
        producer_to_publisher: KafkaProducer):
    """
    Reading from and writing to the journal publisher should work

    Args:
        journal_publisher (JournalPublisher): publisher to read and write data
        kafka_consumer (KafkaConsumer): To read data from the publisher
        kafka_producer (KafkaProducer): To send data to the publisher

    """
    contents = [{b'sha1': c['sha1']} for c in CONTENTS]
    # revisions = [{b'id': c['id']} for c in REVISIONS]
    # releases = [{b'id': c['id']} for c in RELEASES]

    # read the output of the publisher
    consumer_from_publisher.subscribe(
        topics=['%s.%s' % (TEST_CONFIG['final_prefix'], object_type)
                for object_type in TEST_CONFIG['object_types']])

    print('#### producer_to_publisher: Sending: %s' % contents[0])
    # send message to the publisher
    producer_to_publisher.send(
        '%s.content' % TEST_CONFIG['temporary_prefix'],
        contents[0]
    )

    nb_messages = len(contents)

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

        expected_key = contents[num][b'sha1']
        assert expected_key == msg.key

        expected_value = CONTENTS[num]
        # Transformation is needed due to msgpack which encodes keys and values
        value = {}
        for k, v in msg.value.items():
            k = k.decode()
            if k == 'status':
                v = v.decode()
            value[k] = v

        assert expected_value == value
