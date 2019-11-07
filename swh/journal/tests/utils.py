from swh.journal.client import JournalClient, ACCEPTED_OBJECT_TYPES
from swh.journal.writer.kafka import KafkaJournalWriter
from swh.journal.serializers import (kafka_to_value, key_to_kafka,
                                     value_to_kafka)


class FakeKafkaMessage:
    def __init__(self, topic, key, value):
        self._topic = topic
        self._key = key_to_kafka(key)
        self._value = value_to_kafka(value)

    def topic(self):
        return self._topic

    def value(self):
        return self._value

    def key(self):
        return self._key

    def error(self):
        return None


class MockedKafkaWriter(KafkaJournalWriter):
    def __init__(self, queue):
        self._prefix = 'prefix'
        self.queue = queue

    def send(self, topic, key, value):
        msg = FakeKafkaMessage(topic=topic, key=key, value=value)
        self.queue.append(msg)

    def flush(self):
        pass


class MockedKafkaConsumer:
    """Mimic the confluent_kafka.Consumer API, producing the messages stored
       in `queue`.

       You're only allowed to subscribe to topics in which the queue has
       messages.
    """
    def __init__(self, queue):
        self.queue = queue
        self.committed = False

    def consume(self, num_messages, timeout=None):
        L = self.queue[0:num_messages]
        self.queue[0:num_messages] = []
        return L

    def commit(self):
        if self.queue == []:
            self.committed = True

    def list_topics(self, timeout=None):
        return set(message.topic() for message in self.queue)

    def subscribe(self, topics):
        unknown_topics = set(topics) - self.list_topics()
        if unknown_topics:
            raise ValueError('Unknown topics %s' % ', '.join(unknown_topics))


class MockedJournalClient(JournalClient):
    def __init__(self, queue, object_types=ACCEPTED_OBJECT_TYPES):
        self._object_types = object_types
        self.consumer = MockedKafkaConsumer(queue)
        self.process_timeout = 0
        self.max_messages = 0
        self.value_deserializer = kafka_to_value
