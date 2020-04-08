# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
from typing import Dict, Iterable, Optional, Type, Union, overload

from confluent_kafka import Producer, KafkaException

from swh.model.hashutil import DEFAULT_ALGORITHMS
from swh.model.model import (
    BaseModel,
    Content,
    Directory,
    Origin,
    OriginVisit,
    Release,
    Revision,
    SkippedContent,
    Snapshot,
)

from swh.journal.serializers import KeyType, key_to_kafka, value_to_kafka

logger = logging.getLogger(__name__)

OBJECT_TYPES: Dict[Type[BaseModel], str] = {
    Content: "content",
    Directory: "directory",
    Origin: "origin",
    OriginVisit: "origin_visit",
    Release: "release",
    Revision: "revision",
    SkippedContent: "skipped_content",
    Snapshot: "snapshot",
}

ModelObject = Union[
    Content, Directory, Origin, OriginVisit, Release, Revision, SkippedContent, Snapshot
]


class KafkaJournalWriter:
    """This class is instantiated and used by swh-storage to write incoming
    new objects to Kafka before adding them to the storage backend
    (eg. postgresql) itself."""

    def __init__(
        self,
        brokers: Iterable[str],
        prefix: str,
        client_id: str,
        producer_config: Optional[Dict] = None,
    ):
        self._prefix = prefix

        if not producer_config:
            producer_config = {}

        if "message.max.bytes" not in producer_config:
            producer_config = {
                "message.max.bytes": 100 * 1024 * 1024,
                **producer_config,
            }

        self.producer = Producer(
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

    def _error_cb(self, error):
        if error.fatal():
            raise KafkaException(error)
        logger.info("Received non-fatal kafka error: %s", error)

    def _on_delivery(self, error, message):
        if error is not None:
            self._error_cb(error)

    def send(self, topic: str, key: KeyType, value):
        kafka_key = key_to_kafka(key)
        self.producer.produce(
            topic=topic, key=kafka_key, value=value_to_kafka(value),
        )

        # Need to service the callbacks regularly by calling poll
        self.producer.poll(0)

    def flush(self):
        self.producer.flush()

    # these @overload'ed versions of the _get_key method aim at helping mypy figuring
    # the correct type-ing.
    @overload
    def _get_key(
        self, object_type: str, object_: Union[Revision, Release, Directory, Snapshot]
    ) -> bytes:
        ...

    @overload
    def _get_key(self, object_type: str, object_: Content) -> bytes:
        ...

    @overload
    def _get_key(self, object_type: str, object_: SkippedContent) -> Dict[str, bytes]:
        ...

    @overload
    def _get_key(self, object_type: str, object_: Origin) -> Dict[str, bytes]:
        ...

    @overload
    def _get_key(self, object_type: str, object_: OriginVisit) -> Dict[str, str]:
        ...

    def _get_key(self, object_type: str, object_) -> KeyType:
        if object_type in ("revision", "release", "directory", "snapshot"):
            return object_.id
        elif object_type == "content":
            return object_.sha1  # TODO: use a dict of hashes
        elif object_type == "skipped_content":
            return {hash: getattr(object_, hash) for hash in DEFAULT_ALGORITHMS}
        elif object_type == "origin":
            return {"url": object_.url}
        elif object_type == "origin_visit":
            return {
                "origin": object_.origin,
                "date": str(object_.date),
            }
        else:
            raise ValueError("Unknown object type: %s." % object_type)

    def _sanitize_object(
        self, object_type: str, object_: ModelObject
    ) -> Dict[str, str]:
        dict_ = object_.to_dict()
        if object_type == "origin_visit":
            # :(
            dict_["date"] = str(dict_["date"])
        return dict_

    def _write_addition(self, object_type: str, object_: ModelObject) -> None:
        """Write a single object to the journal"""
        topic = f"{self._prefix}.{object_type}"
        key = self._get_key(object_type, object_)
        dict_ = self._sanitize_object(object_type, object_)
        logger.debug("topic: %s, key: %s, value: %s", topic, key, dict_)
        self.send(topic, key=key, value=dict_)

    def write_addition(self, object_type: str, object_: ModelObject) -> None:
        """Write a single object to the journal"""
        self._write_addition(object_type, object_)
        self.flush()

    write_update = write_addition

    def write_additions(self, object_type: str, objects: Iterable[ModelObject]) -> None:
        """Write a set of objects to the journal"""
        for object_ in objects:
            self._write_addition(object_type, object_)

        self.flush()
