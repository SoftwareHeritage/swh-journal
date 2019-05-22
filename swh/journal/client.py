# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from abc import ABCMeta, abstractmethod
from kafka import KafkaConsumer
import logging

from .serializers import kafka_to_key, kafka_to_value


logger = logging.getLogger(__name__)


# Only accepted offset reset policy accepted
ACCEPTED_OFFSET_RESET = ['earliest', 'latest']

# Only accepted object types
ACCEPTED_OBJECT_TYPES = [
    'content',
    'directory',
    'revision',
    'release',
    'snapshot',
    'origin',
    'origin_visit'
]


class JournalClient(metaclass=ABCMeta):
    """A base client for the Software Heritage journal.

    The current implementation of the journal uses Apache Kafka
    brokers to publish messages under a given topic prefix, with each
    object type using a specific topic under that prefix.

    Clients subscribe to events specific to each object type by using
    the `object_types` configuration variable.

    Clients can be sharded by setting the `client_id` to a common
    value across instances. The journal will share the message
    throughput across the nodes sharing the same client_id.

    Messages are processed by the `process_objects` method in batches
    of maximum `max_messages`.

    """
    def __init__(
            self, brokers, topic_prefix, consumer_id,
            object_types=ACCEPTED_OBJECT_TYPES,
            max_messages=0, auto_offset_reset='earliest'):

        if auto_offset_reset not in ACCEPTED_OFFSET_RESET:
            raise ValueError(
                'Option \'auto_offset_reset\' only accept %s.' %
                ACCEPTED_OFFSET_RESET)

        for object_type in object_types:
            if object_type not in ACCEPTED_OBJECT_TYPES:
                raise ValueError(
                    'Option \'object_types\' only accepts %s.' %
                    ACCEPTED_OFFSET_RESET)

        self.consumer = KafkaConsumer(
            bootstrap_servers=brokers,
            key_deserializer=kafka_to_key,
            value_deserializer=kafka_to_value,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=False,
            group_id=consumer_id,
        )

        self.consumer.subscribe(
            topics=['%s.%s' % (topic_prefix, object_type)
                    for object_type in object_types],
        )

        self.max_messages = max_messages
        self._object_types = object_types

    def poll(self):
        return self.consumer.poll()

    def commit(self):
        self.consumer.commit()

    def process(self, max_messages=None):
        nb_messages = 0

        while not self.max_messages or nb_messages < self.max_messages:
            polled = self.poll()
            for (partition, messages) in polled.items():
                object_type = partition.topic.split('.')[-1]
                # Got a message from a topic we did not subscribe to.
                assert object_type in self._object_types, object_type

                self.process_objects(
                    {object_type: [msg.value for msg in messages]})

                nb_messages += len(messages)

            self.commit()
            logger.info('Processed %d messages.' % nb_messages)
        return nb_messages

    # Override the following method in the sub-classes

    @abstractmethod
    def process_objects(self, messages):
        """Process the objects (store, compute, etc...)

        Args:
            messages (dict): Dict of key object_type (as per
            configuration) and their associated values.

        """
        pass
