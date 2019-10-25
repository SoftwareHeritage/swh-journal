# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

from confluent_kafka import Producer, KafkaException

from swh.model.hashutil import DEFAULT_ALGORITHMS
from swh.model.model import BaseModel

from swh.journal.serializers import key_to_kafka, value_to_kafka

logger = logging.getLogger(__name__)


class KafkaJournalWriter:
    """This class is instantiated and used by swh-storage to write incoming
    new objects to Kafka before adding them to the storage backend
    (eg. postgresql) itself."""
    def __init__(self, brokers, prefix, client_id):
        self._prefix = prefix

        if isinstance(brokers, str):
            brokers = [brokers]

        self.producer = Producer({
            'bootstrap.servers': ','.join(brokers),
            'client.id': client_id,
            'on_delivery': self._on_delivery,
            'error_cb': self._error_cb,
            'logger': logger,
            'enable.idempotence': 'true',
        })

    def _error_cb(self, error):
        if error.fatal():
            raise KafkaException(error)
        logger.info('Received non-fatal kafka error: %s', error)

    def _on_delivery(self, error, message):
        if error is not None:
            self._error_cb(error)

    def send(self, topic, key, value):
        self.producer.produce(
            topic=topic,
            key=key_to_kafka(key),
            value=value_to_kafka(value),
        )

        # Need to service the callbacks regularly by calling poll
        self.producer.poll(0)

    def flush(self):
        self.producer.flush()

    def _get_key(self, object_type, object_):
        if object_type in ('revision', 'release', 'directory', 'snapshot'):
            return object_['id']
        elif object_type == 'content':
            return object_['sha1']  # TODO: use a dict of hashes
        elif object_type == 'skipped_content':
            return {
                hash: object_[hash]
                for hash in DEFAULT_ALGORITHMS
            }
        elif object_type == 'origin':
            return {'url': object_['url']}
        elif object_type == 'origin_visit':
            return {
                'origin': object_['origin'],
                'date': str(object_['date']),
            }
        else:
            raise ValueError('Unknown object type: %s.' % object_type)

    def _sanitize_object(self, object_type, object_):
        if object_type == 'origin_visit':
            return {
                **object_,
                'date': str(object_['date']),
            }
        elif object_type == 'origin':
            assert 'id' not in object_
        return object_

    def write_addition(self, object_type, object_, flush=True):
        """Write a single object to the journal"""
        if isinstance(object_, BaseModel):
            object_ = object_.to_dict()
        topic = '%s.%s' % (self._prefix, object_type)
        key = self._get_key(object_type, object_)
        object_ = self._sanitize_object(object_type, object_)
        logger.debug('topic: %s, key: %s, value: %s', topic, key, object_)
        self.send(topic, key=key, value=object_)

        if flush:
            self.flush()

    write_update = write_addition

    def write_additions(self, object_type, objects, flush=True):
        """Write a set of objects to the journal"""
        for object_ in objects:
            self.write_addition(object_type, object_, flush=False)

        if flush:
            self.flush()
