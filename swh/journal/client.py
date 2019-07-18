# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

from kafka import KafkaConsumer

from .serializers import kafka_to_key, kafka_to_value
from swh.journal import DEFAULT_PREFIX

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


class JournalClient:
    """A base client for the Software Heritage journal.

    The current implementation of the journal uses Apache Kafka
    brokers to publish messages under a given topic prefix, with each
    object type using a specific topic under that prefix. If the 'prefix'
    argument is None (default value), it will take the default value
    'swh.journal.objects'.

    Clients subscribe to events specific to each object type as listed in the
    `object_types` argument (if unset, defaults to all accepted objet types).

    Clients can be sharded by setting the `group_id` to a common
    value across instances. The journal will share the message
    throughput across the nodes sharing the same group_id.

    Messages are processed by the `process_objects` method in batches
    of maximum `max_messages`.

    Any other named argument is passed directly to KafkaConsumer().

    """
    def __init__(
            self, brokers, group_id, prefix=None, object_types=None,
            max_messages=0, auto_offset_reset='earliest', **kwargs):
        if prefix is None:
            prefix = DEFAULT_PREFIX
        if object_types is None:
            object_types = ACCEPTED_OBJECT_TYPES
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
            group_id=group_id,
            **kwargs)

        self.consumer.subscribe(
            topics=['%s.%s' % (prefix, object_type)
                    for object_type in object_types],
        )

        self.max_messages = max_messages
        self._object_types = object_types

    def process(self, worker_fn):
        """Polls Kafka for a batch of messages, and calls the worker_fn
        with these messages.

        Args:
            worker_fn Callable[Dict[str, List[dict]]]: Function called with
                                                       the messages as
                                                       argument.
        """
        nb_messages = 0
        polled = self.consumer.poll()
        for (partition, messages) in polled.items():
            object_type = partition.topic.split('.')[-1]
            # Got a message from a topic we did not subscribe to.
            assert object_type in self._object_types, object_type

            worker_fn({object_type: [msg.value for msg in messages]})

            nb_messages += len(messages)

        self.consumer.commit()
        return nb_messages
