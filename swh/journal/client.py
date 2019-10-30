# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import defaultdict
import logging
import time

from confluent_kafka import Consumer, KafkaException

from .serializers import kafka_to_value
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


def _error_cb(error):
    if error.fatal():
        raise KafkaException(error)
    logger.info('Received non-fatal kafka error: %s', error)


def _on_commit(error, partitions):
    if error is not None:
        _error_cb(error)


class JournalClient:
    """A base client for the Software Heritage journal.

    The current implementation of the journal uses Apache Kafka
    brokers to publish messages under a given topic prefix, with each
    object type using a specific topic under that prefix. If the 'prefix'
    argument is None (default value), it will take the default value
    'swh.journal.objects'.

    Clients subscribe to events specific to each object type as listed in the
    `object_types` argument (if unset, defaults to all accepted object types).

    Clients can be sharded by setting the `group_id` to a common
    value across instances. The journal will share the message
    throughput across the nodes sharing the same group_id.

    Messages are processed by the `worker_fn` callback passed to the
    `process` method, in batches of maximum `max_messages`.

    Any other named argument is passed directly to KafkaConsumer().

    """
    def __init__(
            self, brokers, group_id, prefix=None, object_types=None,
            max_messages=0, process_timeout=0, auto_offset_reset='earliest',
            **kwargs):
        if prefix is None:
            prefix = DEFAULT_PREFIX
        if object_types is None:
            object_types = ACCEPTED_OBJECT_TYPES
        if auto_offset_reset not in ACCEPTED_OFFSET_RESET:
            raise ValueError(
                'Option \'auto_offset_reset\' only accept %s, not %s' %
                (ACCEPTED_OFFSET_RESET, auto_offset_reset))

        for object_type in object_types:
            if object_type not in ACCEPTED_OBJECT_TYPES:
                raise ValueError(
                    'Option \'object_types\' only accepts %s, not %s.' %
                    (ACCEPTED_OBJECT_TYPES, object_type))

        self.value_deserializer = kafka_to_value

        if isinstance(brokers, str):
            brokers = [brokers]

        consumer_settings = {
            **kwargs,
            'bootstrap.servers': ','.join(brokers),
            'auto.offset.reset': auto_offset_reset,
            'group.id': group_id,
            'on_commit': _on_commit,
            'error_cb': _error_cb,
            'enable.auto.commit': False,
            'logger': logger,
        }

        logger.debug('Consumer settings: %s', consumer_settings)

        self.consumer = Consumer(consumer_settings, logger=logger)

        topics = ['%s.%s' % (prefix, object_type)
                  for object_type in object_types]

        logger.debug('Upstream topics: %s',
                     self.consumer.list_topics(timeout=10))
        logger.debug('Subscribing to: %s', topics)

        self.consumer.subscribe(topics=topics)

        self.max_messages = max_messages
        self.process_timeout = process_timeout

        self._object_types = object_types

    def process(self, worker_fn):
        """Polls Kafka for a batch of messages, and calls the worker_fn
        with these messages.

        Args:
            worker_fn Callable[Dict[str, List[dict]]]: Function called with
                                                       the messages as
                                                       argument.
        """
        start_time = time.monotonic()
        nb_messages = 0

        objects = defaultdict(list)

        while True:
            # timeout for message poll
            timeout = 1.0

            elapsed = time.monotonic() - start_time
            if self.process_timeout:
                if elapsed + 0.01 >= self.process_timeout:
                    break

                timeout = self.process_timeout - elapsed

            num_messages = 20

            if self.max_messages:
                if nb_messages >= self.max_messages:
                    break
                num_messages = min(num_messages, self.max_messages-nb_messages)

            messages = self.consumer.consume(
                timeout=timeout, num_messages=num_messages)
            if not messages:
                continue

            for message in messages:
                error = message.error()
                if error is not None:
                    if error.fatal():
                        raise KafkaException(error)
                    logger.info('Received non-fatal kafka error: %s', error)
                    continue

                nb_messages += 1

                object_type = message.topic().split('.')[-1]
                # Got a message from a topic we did not subscribe to.
                assert object_type in self._object_types, object_type

                objects[object_type].append(
                    self.value_deserializer(message.value())
                )

            if objects:
                worker_fn(dict(objects))
                objects.clear()

                self.consumer.commit()
        return nb_messages

    def close(self):
        self.consumer.close()
