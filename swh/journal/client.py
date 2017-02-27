# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

from abc import ABCMeta, abstractmethod
from collections import defaultdict
from kafka import KafkaConsumer

from swh.core.config import SWHConfig
from .serializers import kafka_to_value


class SWHJournalClient(SWHConfig, metaclass=ABCMeta):
    """SWH Journal client able to subscribe and react to events from
    specific topics.

    The configuration defines:
    - brokers ([str]): the brokers to receive messages from

    - topic_prefix (str): the topic prefix string name used to
      read messages from

    - consumer_id (str): consumer identifier

    - object_types ([str]): object types to subscribe events for. This
      is used in conjunction of the topic_prefix key to compute the
      queue name.

    - max_messages (int): Group messages in block of max_messages
      before sending them.

    """
    DEFAULT_CONFIG = {
        # Broker to connect to
        'brokers': ('list[str]', ['localhost']),
        # Prefix topic to receive notification from
        'topic_prefix': ('str', 'swh.journal.test_publisher'),
        # Consumer identifier
        'consumer_id': ('str', 'swh.journal.client.test'),
        # Object types to deal with (in a subscription manner)
        'object_types': ('list[str]', [
            'content', 'revision', 'release', 'occurrence',
            'origin', 'origin_visit']),
        # Number of messages to batch process
        'max_messages': ('int', 100),
    }

    CONFIG_BASE_FILENAME = 'journal/client'

    ADDITIONAL_CONFIG = None

    def __init__(self, extra_configuration={}):
        self.config = self.parse_config_file(
            additional_configs=[self.ADDITIONAL_CONFIG])
        if extra_configuration:
            self.config.update(extra_configuration)

        self.log = logging.getLogger('swh.journal.client.SWHJournalClient')

        self.consumer = KafkaConsumer(
            bootstrap_servers=self.config['brokers'],
            value_deserializer=kafka_to_value,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            group_id=self.config['consumer_id'],
        )

        self.consumer.subscribe(
            topics=['%s.%s' % (self.config['topic_prefix'], object_type)
                    for object_type in self.config['object_types']],
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
