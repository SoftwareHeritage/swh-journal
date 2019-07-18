from collections import namedtuple

from swh.journal.client import JournalClient, ACCEPTED_OBJECT_TYPES
from swh.journal.direct_writer import DirectKafkaWriter
from swh.journal.serializers import (
    key_to_kafka, kafka_to_key, value_to_kafka, kafka_to_value)

FakeKafkaMessage = namedtuple('FakeKafkaMessage', 'key value')
FakeKafkaPartition = namedtuple('FakeKafkaPartition', 'topic')


class MockedKafkaWriter(DirectKafkaWriter):
    def __init__(self, queue):
        self._prefix = 'prefix'
        self.queue = queue

    def send(self, topic, key, value):
        key = kafka_to_key(key_to_kafka(key))
        value = kafka_to_value(value_to_kafka(value))
        partition = FakeKafkaPartition(topic)
        msg = FakeKafkaMessage(key=key, value=value)
        if self.queue and {partition} == set(self.queue[-1]):
            # The last message is of the same object type, groupping them
            self.queue[-1][partition].append(msg)
        else:
            self.queue.append({partition: [msg]})


class MockedKafkaConsumer:
    def __init__(self, queue):
        self.queue = queue
        self.committed = False

    def poll(self):
        return self.queue.pop(0)

    def commit(self):
        if self.queue == []:
            self.committed = True


class MockedJournalClient(JournalClient):
    def __init__(self, queue, object_types=ACCEPTED_OBJECT_TYPES):
        self._object_types = object_types
        self.consumer = MockedKafkaConsumer(queue)
