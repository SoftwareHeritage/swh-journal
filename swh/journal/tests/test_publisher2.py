# Copyright (C) 2018-2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from kafka import KafkaConsumer, KafkaProducer


def write_and_read(kafka_producer: KafkaProducer,
                   kafka_consumer: KafkaConsumer) -> None:
    """Produces writes to a topic, consumer consumes from the same topic.

    """
    message = b'msg'
    topic = 'abc'
    # write to kafka
    kafka_producer.send(topic, message)
    kafka_producer.flush()
    # read from it
    consumed = list(kafka_consumer)
    assert len(consumed) == 1
    assert consumed[0].topic == topic
    assert consumed[0].value == message


def test_read_write(kafka_producer: KafkaProducer,
                    kafka_consumer: KafkaConsumer):
    """Independent test from the publisher so far"""
    write_and_read(kafka_producer, kafka_consumer)


def test_poll_publisher():
    pass

# def setUp(self):
#     self.publisher = JournalPublisherTest()
#     self.contents = [{b'sha1': c['sha1']} for c in CONTENTS]
#     # self.revisions = [{b'id': c['id']} for c in REVISIONS]
#     # self.releases = [{b'id': c['id']} for c in RELEASES]
#     # producer and consumer to send and read data from publisher
#     self.producer_to_publisher = KafkaProducer(
#         bootstrap_servers=TEST_CONFIG['brokers'],
#         key_serializer=key_to_kafka,
#         value_serializer=key_to_kafka,
#         acks='all')
#     self.consumer_from_publisher = KafkaConsumer(
#         bootstrap_servers=TEST_CONFIG['brokers'],
#         value_deserializer=kafka_to_key)
#     self.consumer_from_publisher.subscribe(
#         topics=['%s.%s' % (TEST_CONFIG['temporary_prefix'], object_type)
#                 for object_type in TEST_CONFIG['object_types']])


# def test_poll(kafka_consumer):
#     # given (send message to the publisher)
#     self.producer_to_publisher.send(
#         '%s.content' % TEST_CONFIG['temporary_prefix'],
#         self.contents[0]
#     )

#     nb_messages = 1

#     # when (the publisher poll 1 message and send 1 reified object)
#     self.publisher.poll(max_messages=nb_messages)

#     # then (client reads from the messages from output topic)
#     msgs = []
#     for num, msg in enumerate(self.consumer_from_publisher):
#         print('#### consumed msg %s: %s ' % (num, msg))
#         msgs.append(msg)

#     self.assertEqual(len(msgs), nb_messages)
#     print('##### msgs: %s' % msgs)
#     # check the results
#     expected_topic = 'swh.journal.objects.content'
#     expected_object = (self.contents[0][b'sha1'], CONTENTS[0])

#     self.assertEqual(msgs, (expected_topic, expected_object))
