# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import defaultdict
import logging
import os
import time
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from confluent_kafka import Consumer, KafkaException, KafkaError

from .serializers import kafka_to_value
from swh.journal import DEFAULT_PREFIX

logger = logging.getLogger(__name__)
rdkafka_logger = logging.getLogger(__name__ + '.rdkafka')


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

# Errors that Kafka raises too often and are not useful; therefore they
# we lower their log level to DEBUG instead of INFO.
_SPAMMY_ERRORS = [
    KafkaError._NO_OFFSET,
]


def _error_cb(error):
    if error.fatal():
        raise KafkaException(error)
    if error.code() in _SPAMMY_ERRORS:
        logger.debug('Received non-fatal kafka error: %s', error)
    else:
        logger.info('Received non-fatal kafka error: %s', error)


def _on_commit(error, partitions):
    if error is not None:
        _error_cb(error)


class JournalClient:
    """A base client for the Software Heritage journal.

    The current implementation of the journal uses Apache Kafka
    brokers to publish messages under a given topic prefix, with each
    object type using a specific topic under that prefix. If the `prefix`
    argument is None (default value), it will take the default value
    `'swh.journal.objects'`.

    Clients subscribe to events specific to each object type as listed in the
    `object_types` argument (if unset, defaults to all accepted object types).

    Clients can be sharded by setting the `group_id` to a common
    value across instances. The journal will share the message
    throughput across the nodes sharing the same group_id.

    Messages are processed by the `worker_fn` callback passed to the `process`
    method, in batches of maximum `batch_size` messages (defaults to 200).

    If set, the processing stops after processing `stop_after_objects` messages
    in total.

    `stop_on_eof` stops the processing when the client has reached the end of
    each partition in turn.

    `auto_offset_reset` sets the behavior of the client when the consumer group
    initializes: `'earliest'` (the default) processes all objects since the
    inception of the topics; `''`

    Any other named argument is passed directly to KafkaConsumer().

    """
    def __init__(
            self,
            brokers: Union[str, List[str]],
            group_id: str,
            prefix: Optional[str] = None,
            object_types: Optional[List[str]] = None,
            stop_after_objects: Optional[int] = None,
            batch_size: int = 200,
            process_timeout: Optional[float] = None,
            auto_offset_reset: str = 'earliest',
            stop_on_eof: bool = False,
            **kwargs
    ):
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

        if batch_size <= 0:
            raise ValueError("Option 'batch_size' needs to be positive")

        self.value_deserializer = kafka_to_value

        if isinstance(brokers, str):
            brokers = [brokers]

        debug_logging = rdkafka_logger.isEnabledFor(logging.DEBUG)
        if debug_logging and 'debug' not in kwargs:
            kwargs['debug'] = 'consumer'

        # Static group instance id management
        group_instance_id = os.environ.get('KAFKA_GROUP_INSTANCE_ID')
        if group_instance_id:
            kwargs['group.instance.id'] = group_instance_id

        if 'group.instance.id' in kwargs:
            # When doing static consumer group membership, set a higher default
            # session timeout. The session timeout is the duration after which
            # the broker considers that a consumer has left the consumer group
            # for good, and triggers a rebalance. Considering our current
            # processing pattern, 10 minutes gives the consumer ample time to
            # restart before that happens.
            if 'session.timeout.ms' not in kwargs:
                kwargs['session.timeout.ms'] = 10 * 60 * 1000  # 10 minutes

        if 'session.timeout.ms' in kwargs:
            # When the session timeout is set, rdkafka requires the max poll
            # interval to be set to a higher value; the max poll interval is
            # rdkafka's way of figuring out whether the client's message
            # processing thread has stalled: when the max poll interval lapses
            # between two calls to consumer.poll(), rdkafka leaves the consumer
            # group and terminates the connection to the brokers.
            #
            # We default to 1.5 times the session timeout
            if 'max.poll.interval.ms' not in kwargs:
                kwargs['max.poll.interval.ms'] = (
                    kwargs['session.timeout.ms'] // 2 * 3
                )

        consumer_settings = {
            **kwargs,
            'bootstrap.servers': ','.join(brokers),
            'auto.offset.reset': auto_offset_reset,
            'group.id': group_id,
            'on_commit': _on_commit,
            'error_cb': _error_cb,
            'enable.auto.commit': False,
            'logger': rdkafka_logger,
        }

        self.stop_on_eof = stop_on_eof
        if self.stop_on_eof:
            consumer_settings['enable.partition.eof'] = True

        logger.debug('Consumer settings: %s', consumer_settings)

        self.consumer = Consumer(consumer_settings)

        topics = ['%s.%s' % (prefix, object_type)
                  for object_type in object_types]

        logger.debug('Upstream topics: %s',
                     self.consumer.list_topics(timeout=10))
        logger.debug('Subscribing to: %s', topics)

        self.consumer.subscribe(topics=topics)

        self.stop_after_objects = stop_after_objects
        self.process_timeout = process_timeout
        self.eof_reached: Set[Tuple[str, str]] = set()
        self.batch_size = batch_size

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
        total_objects_processed = 0

        while True:
            # timeout for message poll
            timeout = 1.0

            elapsed = time.monotonic() - start_time
            if self.process_timeout:
                # +0.01 to prevent busy-waiting on / spamming consumer.poll.
                # consumer.consume() returns shortly before X expired
                # (a matter of milliseconds), so after it returns a first
                # time, it would then be called with a timeout in the order
                # of milliseconds, therefore returning immediately, then be
                # called again, etc.
                if elapsed + 0.01 >= self.process_timeout:
                    break

                timeout = self.process_timeout - elapsed

            batch_size = self.batch_size
            if self.stop_after_objects:
                if total_objects_processed >= self.stop_after_objects:
                    break

                # clamp batch size to avoid overrunning stop_after_objects
                batch_size = min(
                    self.stop_after_objects-total_objects_processed,
                    batch_size,
                )

            messages = self.consumer.consume(
                timeout=timeout, num_messages=batch_size)
            if not messages:
                continue

            batch_processed, at_eof = self.handle_messages(messages, worker_fn)
            total_objects_processed += batch_processed
            if at_eof:
                break

        return total_objects_processed

    def handle_messages(self, messages, worker_fn):
        objects: Dict[str, List[Any]] = defaultdict(list)
        nb_processed = 0

        for message in messages:
            error = message.error()
            if error is not None:
                if error.code() == KafkaError._PARTITION_EOF:
                    self.eof_reached.add(
                        (message.topic(), message.partition())
                    )
                else:
                    _error_cb(error)
                continue

            nb_processed += 1

            object_type = message.topic().split('.')[-1]
            # Got a message from a topic we did not subscribe to.
            assert object_type in self._object_types, object_type

            objects[object_type].append(self.deserialize_message(message))

        if objects:
            worker_fn(dict(objects))
            self.consumer.commit()

        at_eof = (self.stop_on_eof and all(
            (tp.topic, tp.partition) in self.eof_reached
            for tp in self.consumer.assignment()
        ))

        return nb_processed, at_eof

    def deserialize_message(self, message):
        return self.value_deserializer(message.value())

    def close(self):
        self.consumer.close()
