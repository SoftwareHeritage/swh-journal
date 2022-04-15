# Copyright (C) 2017-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import defaultdict
from importlib import import_module
from itertools import cycle
import logging
import os
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

from confluent_kafka import Consumer, KafkaError, KafkaException

from swh.core.statsd import statsd
from swh.journal import DEFAULT_PREFIX

from .serializers import kafka_to_value

logger = logging.getLogger(__name__)
rdkafka_logger = logging.getLogger(__name__ + ".rdkafka")


# Only accepted offset reset policy accepted
ACCEPTED_OFFSET_RESET = ["earliest", "latest"]

# Errors that Kafka raises too often and are not useful; therefore they
# we lower their log level to DEBUG instead of INFO.
_SPAMMY_ERRORS = [
    KafkaError._NO_OFFSET,
]

JOURNAL_MESSAGE_NUMBER_METRIC = "swh_journal_client_handle_message_total"
JOURNAL_STATUS_METRIC = "swh_journal_client_status"


def get_journal_client(cls: str, **kwargs: Any):
    """Factory function to instantiate a journal client object.

    Currently, only the "kafka" journal client is supported.
    """
    if cls == "kafka":
        if "stats_cb" in kwargs:
            stats_cb = kwargs["stats_cb"]
            if isinstance(stats_cb, str):
                try:
                    module_path, func_name = stats_cb.split(":")
                except ValueError:
                    raise ValueError(
                        "Invalid stats_cb configuration option: "
                        "it should be a string like 'path.to.module:function'"
                    )
                try:
                    module = import_module(module_path, package=__package__)
                except ModuleNotFoundError:
                    raise ValueError(
                        "Invalid stats_cb configuration option: "
                        f"module {module_path} not found"
                    )
                try:
                    kwargs["stats_cb"] = getattr(module, func_name)
                except AttributeError:
                    raise ValueError(
                        "Invalid stats_cb configuration option: "
                        f"function {func_name} not found in module {module_path}"
                    )
        return JournalClient(**kwargs)
    raise ValueError("Unknown journal client class `%s`" % cls)


def _error_cb(error):
    if error.fatal():
        raise KafkaException(error)
    if error.code() in _SPAMMY_ERRORS:
        logger.debug("Received non-fatal kafka error: %s", error)
    else:
        logger.info("Received non-fatal kafka error: %s", error)


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
    `object_types` argument (if unset, defaults to all existing kafka topic under
    the prefix).

    Clients can be sharded by setting the `group_id` to a common
    value across instances. The journal will share the message
    throughput across the nodes sharing the same group_id.

    Messages are processed by the `worker_fn` callback passed to the `process`
    method, in batches of maximum `batch_size` messages (defaults to 200).

    The objects passed to the `worker_fn` callback are the result of the kafka
    message converted by the `value_deserializer` function. By default (if this
    argument is not given), it will produce dicts (using the `kafka_to_value`
    function). This signature of the function is:

        `value_deserializer(object_type: str, kafka_msg: bytes) -> Any`

    If the value returned by `value_deserializer` is None, it is ignored and
    not passed the `worker_fn` function.

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
        privileged: bool = False,
        stop_after_objects: Optional[int] = None,
        batch_size: int = 200,
        process_timeout: Optional[float] = None,
        auto_offset_reset: str = "earliest",
        stop_on_eof: bool = False,
        value_deserializer: Optional[Callable[[str, bytes], Any]] = None,
        **kwargs,
    ):
        if prefix is None:
            prefix = DEFAULT_PREFIX
        if auto_offset_reset not in ACCEPTED_OFFSET_RESET:
            raise ValueError(
                "Option 'auto_offset_reset' only accept %s, not %s"
                % (ACCEPTED_OFFSET_RESET, auto_offset_reset)
            )

        if batch_size <= 0:
            raise ValueError("Option 'batch_size' needs to be positive")
        if value_deserializer:
            self.value_deserializer = value_deserializer
        else:
            self.value_deserializer = lambda _, value: kafka_to_value(value)

        if isinstance(brokers, str):
            brokers = [brokers]

        debug_logging = rdkafka_logger.isEnabledFor(logging.DEBUG)
        if debug_logging and "debug" not in kwargs:
            kwargs["debug"] = "consumer"

        # Static group instance id management
        group_instance_id = os.environ.get("KAFKA_GROUP_INSTANCE_ID")
        if group_instance_id:
            kwargs["group.instance.id"] = group_instance_id

        if "group.instance.id" in kwargs:
            # When doing static consumer group membership, set a higher default
            # session timeout. The session timeout is the duration after which
            # the broker considers that a consumer has left the consumer group
            # for good, and triggers a rebalance. Considering our current
            # processing pattern, 10 minutes gives the consumer ample time to
            # restart before that happens.
            if "session.timeout.ms" not in kwargs:
                kwargs["session.timeout.ms"] = 10 * 60 * 1000  # 10 minutes

        if "session.timeout.ms" in kwargs:
            # When the session timeout is set, rdkafka requires the max poll
            # interval to be set to a higher value; the max poll interval is
            # rdkafka's way of figuring out whether the client's message
            # processing thread has stalled: when the max poll interval lapses
            # between two calls to consumer.poll(), rdkafka leaves the consumer
            # group and terminates the connection to the brokers.
            #
            # We default to 1.5 times the session timeout
            if "max.poll.interval.ms" not in kwargs:
                kwargs["max.poll.interval.ms"] = kwargs["session.timeout.ms"] // 2 * 3

        consumer_settings = {
            **kwargs,
            "bootstrap.servers": ",".join(brokers),
            "auto.offset.reset": auto_offset_reset,
            "group.id": group_id,
            "on_commit": _on_commit,
            "error_cb": _error_cb,
            "enable.auto.commit": False,
            "logger": rdkafka_logger,
        }

        self.stop_on_eof = stop_on_eof
        if self.stop_on_eof:
            consumer_settings["enable.partition.eof"] = True

        logger.debug("Consumer settings: %s", consumer_settings)

        self.consumer = Consumer(consumer_settings)
        if privileged:
            privileged_prefix = f"{prefix}_privileged"
        else:  # do not attempt to subscribe to privileged topics
            privileged_prefix = f"{prefix}"
        existing_topics = [
            topic
            for topic in self.consumer.list_topics(timeout=10).topics.keys()
            if (
                topic.startswith(f"{prefix}.")
                or topic.startswith(f"{privileged_prefix}.")
            )
        ]
        if not existing_topics:
            raise ValueError(
                f"The prefix {prefix} does not match any existing topic "
                "on the kafka broker"
            )

        if not object_types:
            object_types = list({topic.split(".")[-1] for topic in existing_topics})

        self.subscription = []
        unknown_types = []
        for object_type in object_types:
            topics = (f"{privileged_prefix}.{object_type}", f"{prefix}.{object_type}")
            for topic in topics:
                if topic in existing_topics:
                    self.subscription.append(topic)
                    break
            else:
                unknown_types.append(object_type)
        if unknown_types:
            raise ValueError(
                f"Topic(s) for object types {','.join(unknown_types)} "
                "are unknown on the kafka broker"
            )

        logger.debug(f"Upstream topics: {existing_topics}")
        self.subscribe()

        self.stop_after_objects = stop_after_objects

        self.eof_reached: Set[Tuple[str, str]] = set()
        self.batch_size = batch_size

        if process_timeout is not None:
            raise DeprecationWarning(
                "'process_timeout' argument is not supported anymore by "
                "JournalClient; please remove it from your configuration.",
            )

    def subscribe(self):
        """Subscribe to topics listed in self.subscription

        This can be overridden if you need, for instance, to manually assign partitions.
        """
        logger.debug(f"Subscribing to: {self.subscription}")
        self.consumer.subscribe(topics=self.subscription)

    def process(self, worker_fn):
        """Polls Kafka for a batch of messages, and calls the worker_fn
        with these messages.

        Args:
            worker_fn Callable[Dict[str, List[dict]]]: Function called with
                                                       the messages as
                                                       argument.
        """
        total_objects_processed = 0
        # timeout for message poll
        timeout = 1.0

        with statsd.status_gauge(
            JOURNAL_STATUS_METRIC, statuses=["idle", "processing", "waiting"]
        ) as set_status:
            set_status("idle")
            while True:
                batch_size = self.batch_size
                if self.stop_after_objects:
                    if total_objects_processed >= self.stop_after_objects:
                        break

                    # clamp batch size to avoid overrunning stop_after_objects
                    batch_size = min(
                        self.stop_after_objects - total_objects_processed,
                        batch_size,
                    )
                set_status("waiting")
                for i in cycle(reversed(range(10))):
                    messages = self.consumer.consume(
                        timeout=timeout, num_messages=batch_size
                    )
                    if messages:
                        break

                    # do check for an EOF condition iff we already consumed
                    # messages, otherwise we could detect an EOF condition
                    # before messages had a chance to reach us (e.g. in tests)
                    if total_objects_processed > 0 and self.stop_on_eof and i == 0:
                        at_eof = all(
                            (tp.topic, tp.partition) in self.eof_reached
                            for tp in self.consumer.assignment()
                        )
                        if at_eof:
                            break
                if messages:
                    set_status("processing")
                    batch_processed, at_eof = self.handle_messages(messages, worker_fn)

                    set_status("idle")
                    # report the number of handled messages
                    statsd.increment(
                        JOURNAL_MESSAGE_NUMBER_METRIC, value=batch_processed
                    )
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
                    self.eof_reached.add((message.topic(), message.partition()))
                else:
                    _error_cb(error)
                continue
            if message.value() is None:
                # ignore message with no payload, these can be generated in tests
                continue
            nb_processed += 1
            object_type = message.topic().split(".")[-1]
            deserialized_object = self.deserialize_message(
                message, object_type=object_type
            )
            if deserialized_object is not None:
                objects[object_type].append(deserialized_object)

        if objects:
            worker_fn(dict(objects))
        self.consumer.commit()

        at_eof = self.stop_on_eof and all(
            (tp.topic, tp.partition) in self.eof_reached
            for tp in self.consumer.assignment()
        )

        return nb_processed, at_eof

    def deserialize_message(self, message, object_type=None):
        return self.value_deserializer(object_type, message.value())

    def close(self):
        self.consumer.close()
