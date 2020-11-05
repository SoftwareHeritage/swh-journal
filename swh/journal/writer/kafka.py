# Copyright (C) 2019-2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import time
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterable,
    List,
    NamedTuple,
    Optional,
    Type,
    TypeVar,
)

from confluent_kafka import KafkaException, Producer

from swh.journal.serializers import KeyType, key_to_kafka, pprint_key, value_to_kafka

from . import ValueProtocol

logger = logging.getLogger(__name__)


class DeliveryTag(NamedTuple):
    """Unique tag allowing us to check for a message delivery"""

    topic: str
    kafka_key: bytes


class DeliveryFailureInfo(NamedTuple):
    """Verbose information for failed deliveries"""

    object_type: str
    key: KeyType
    message: str
    code: str


def get_object_type(topic: str) -> str:
    """Get the object type from a topic string"""
    return topic.rsplit(".", 1)[-1]


class KafkaDeliveryError(Exception):
    """Delivery failed on some kafka messages."""

    def __init__(self, message: str, delivery_failures: Iterable[DeliveryFailureInfo]):
        self.message = message
        self.delivery_failures = list(delivery_failures)

    def pretty_failures(self) -> str:
        return ", ".join(
            f"{f.object_type} {pprint_key(f.key)} ({f.message})"
            for f in self.delivery_failures
        )

    def __str__(self):
        return f"KafkaDeliveryError({self.message}, [{self.pretty_failures()}])"


TValue = TypeVar("TValue", bound=ValueProtocol)


class KafkaJournalWriter(Generic[TValue]):
    """This class is used to write serialized versions of value objects to a
    series of Kafka topics. The type parameter `TValue`, which must implement the
    `ValueProtocol`, is the type of values this writer will write.
    Typically, `TValue` will be `swh.model.model.BaseModel`.

    Topics used to send objects representations are built from a ``prefix`` plus the
    type of the object:

      ``{prefix}.{object_type}``

    Objects can be sent as is, or can be anonymized. The anonymization feature, when
    activated, will write anonymized versions of value objects in the main topic, and
    stock (non-anonymized) objects will be sent to a dedicated (privileged) set of
    topics:

      ``{prefix}_privileged.{object_type}``

    The anonymization of a value object is the result of calling its
    ``anonymize()`` method. An object is considered anonymizable if this
    method returns a (non-None) value.

    Args:
      brokers: list of broker addresses and ports.
      prefix: the prefix used to build the topic names for objects.
      client_id: the id of the writer sent to kafka.
      value_sanitizer: a function that takes the object type and the dict
        representation of an object as argument, and returns an other dict
        that should be actually stored in the journal (eg. removing keys
        that do no belong there)
      producer_config: extra configuration keys passed to the `Producer`.
      flush_timeout: timeout, in seconds, after which the `flush` operation
        will fail if some message deliveries are still pending.
      producer_class: override for the kafka producer class.
      anonymize: if True, activate the anonymization feature.

    """

    def __init__(
        self,
        brokers: Iterable[str],
        prefix: str,
        client_id: str,
        value_sanitizer: Callable[[str, Dict[str, Any]], Dict[str, Any]],
        producer_config: Optional[Dict] = None,
        flush_timeout: float = 120,
        producer_class: Type[Producer] = Producer,
        anonymize: bool = False,
    ):
        self._prefix = prefix
        self._prefix_privileged = f"{self._prefix}_privileged"
        self.anonymize = anonymize

        if not producer_config:
            producer_config = {}

        if "message.max.bytes" not in producer_config:
            producer_config = {
                "message.max.bytes": 100 * 1024 * 1024,
                **producer_config,
            }

        self.producer = producer_class(
            {
                "bootstrap.servers": ",".join(brokers),
                "client.id": client_id,
                "on_delivery": self._on_delivery,
                "error_cb": self._error_cb,
                "logger": logger,
                "acks": "all",
                **producer_config,
            }
        )

        # Delivery management
        self.flush_timeout = flush_timeout

        # delivery tag -> original object "key" mapping
        self.deliveries_pending: Dict[DeliveryTag, KeyType] = {}

        # List of (object_type, key, error_msg, error_name) for failed deliveries
        self.delivery_failures: List[DeliveryFailureInfo] = []

        self.value_sanitizer = value_sanitizer

    def _error_cb(self, error):
        if error.fatal():
            raise KafkaException(error)
        logger.info("Received non-fatal kafka error: %s", error)

    def _on_delivery(self, error, message):
        (topic, key) = delivery_tag = DeliveryTag(message.topic(), message.key())
        sent_key = self.deliveries_pending.pop(delivery_tag, None)

        if error is not None:
            self.delivery_failures.append(
                DeliveryFailureInfo(
                    get_object_type(topic), sent_key, error.str(), error.name()
                )
            )

    def send(self, topic: str, key: KeyType, value):
        kafka_key = key_to_kafka(key)
        self.producer.produce(
            topic=topic, key=kafka_key, value=value_to_kafka(value),
        )

        self.deliveries_pending[DeliveryTag(topic, kafka_key)] = key

    def delivery_error(self, message) -> KafkaDeliveryError:
        """Get all failed deliveries, and clear them"""
        ret = self.delivery_failures
        self.delivery_failures = []

        while self.deliveries_pending:
            delivery_tag, orig_key = self.deliveries_pending.popitem()
            (topic, kafka_key) = delivery_tag
            ret.append(
                DeliveryFailureInfo(
                    get_object_type(topic),
                    orig_key,
                    "No delivery before flush() timeout",
                    "SWH_FLUSH_TIMEOUT",
                )
            )

        return KafkaDeliveryError(message, ret)

    def flush(self):
        start = time.monotonic()

        self.producer.flush(self.flush_timeout)

        while self.deliveries_pending:
            if time.monotonic() - start > self.flush_timeout:
                break
            self.producer.poll(0.1)

        if self.deliveries_pending:
            # Delivery timeout
            raise self.delivery_error(
                "flush() exceeded timeout (%ss)" % self.flush_timeout,
            )
        elif self.delivery_failures:
            raise self.delivery_error("Failed deliveries after flush()")

    def _write_addition(self, object_type: str, object_: TValue) -> None:
        """Write a single object to the journal"""
        key = object_.unique_key()

        if self.anonymize:
            anon_object_ = object_.anonymize()
            if anon_object_:  # can be either None, or an anonymized object
                # if the object is anonymizable, send the non-anonymized version in the
                # privileged channel
                topic = f"{self._prefix_privileged}.{object_type}"
                dict_ = self.value_sanitizer(object_type, object_.to_dict())
                logger.debug("topic: %s, key: %s, value: %s", topic, key, dict_)
                self.send(topic, key=key, value=dict_)
                object_ = anon_object_

        topic = f"{self._prefix}.{object_type}"
        dict_ = self.value_sanitizer(object_type, object_.to_dict())
        logger.debug("topic: %s, key: %s, value: %s", topic, key, dict_)
        self.send(topic, key=key, value=dict_)

    def write_addition(self, object_type: str, object_: TValue) -> None:
        """Write a single object to the journal"""
        self._write_addition(object_type, object_)
        self.flush()

    write_update = write_addition

    def write_additions(self, object_type: str, objects: Iterable[TValue]) -> None:
        """Write a set of objects to the journal"""
        for object_ in objects:
            self._write_addition(object_type, object_)

        self.flush()
