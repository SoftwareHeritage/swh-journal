# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

from abc import ABCMeta, abstractmethod
from collections import defaultdict
from kafka import KafkaConsumer

from swh.core.config import SWHConfig
from .serializers import kafka_to_key, kafka_to_value


# Only accepted offset reset policy accepted
ACCEPTED_OFFSET_RESET = ['earliest', 'latest']

# Only accepted object types
ACCEPTED_OBJECT_TYPES = [
    'content',
    'revision',
    'release',
    'occurrence',
    'origin',
    'origin_visit'
]


class JournalClient(SWHConfig, metaclass=ABCMeta):
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
    DEFAULT_CONFIG = {
        # Broker to connect to
        'brokers': ('list[str]', ['localhost']),
        # Prefix topic to receive notification from
        'topic_prefix': ('str', 'swh.journal.objects'),
        # Consumer identifier
        'consumer_id': ('str', 'swh.journal.client'),
        # Object types to deal with (in a subscription manner)
        'object_types': ('list[str]', [
            'content', 'revision',
            'release', 'occurrence',
            'origin', 'origin_visit'
        ]),
        # Number of messages to batch process
        'max_messages': ('int', 100),
        'auto_offset_reset': ('str', 'earliest')
    }

    CONFIG_BASE_FILENAME = 'journal/client'

    ADDITIONAL_CONFIG = {}

    def __init__(self, extra_configuration={}):
        self.config = self.parse_config_file(
            additional_configs=[self.ADDITIONAL_CONFIG])
        if extra_configuration:
            self.config.update(extra_configuration)

        self.log = logging.getLogger('swh.journal.client.JournalClient')

        auto_offset_reset = self.config['auto_offset_reset']
        if auto_offset_reset not in ACCEPTED_OFFSET_RESET:
            raise ValueError(
                'Option \'auto_offset_reset\' only accept %s.' %
                ACCEPTED_OFFSET_RESET)

        object_types = self.config['object_types']
        for object_type in object_types:
            if object_type not in ACCEPTED_OBJECT_TYPES:
                raise ValueError(
                    'Option \'object_types\' only accepts %s.' %
                    ACCEPTED_OFFSET_RESET)

        self.consumer = KafkaConsumer(
            bootstrap_servers=self.config['brokers'],
            key_deserializer=kafka_to_key,
            value_deserializer=kafka_to_value,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=False,
            group_id=self.config['consumer_id'],
        )

        self.consumer.subscribe(
            topics=['%s.%s' % (self.config['topic_prefix'], object_type)
                    for object_type in object_types],
        )

        self.max_messages = self.config['max_messages']

    def process(self):
        """Main entry point to process event message reception.

        """
        while True:
            messages = defaultdict(list)

            for num, message in enumerate(self.consumer):
                object_type = message.topic.split('.')[-1]
                messages[object_type].append(message.value)
                if num >= self.max_messages:
                    break

            self.process_objects(messages)
            self.consumer.commit()

    # Override the following method in the sub-classes

    @abstractmethod
    def process_objects(self, messages):
        """Process the objects (store, compute, etc...)

        Args:
            messages (dict): Dict of key object_type (as per
            configuration) and their associated values.

        """
        pass
