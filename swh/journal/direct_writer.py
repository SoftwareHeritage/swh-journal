# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

from confluent_kafka import Producer

from swh.model.hashutil import DEFAULT_ALGORITHMS
from swh.model.model import BaseModel

from .serializers import key_to_kafka, value_to_kafka

logger = logging.getLogger(__name__)


class DirectKafkaWriter:
    """This class is instantiated and used by swh-storage to write incoming
    new objects to Kafka before adding them to the storage backend
    (eg. postgresql) itself."""
    def __init__(self, brokers, prefix, client_id):
        self._prefix = prefix

        self.producer = Producer({
            'bootstrap.servers': brokers,
            'client.id': client_id,
            'enable.idempotence': 'true',
        })

    def send(self, topic, key, value):
        self.producer.produce(
            topic=topic,
            key=key_to_kafka(key),
            value=value_to_kafka(value),
        )

    def flush(self):
        self.producer.flush()
        # Need to service the callbacks regularly by calling poll
        self.producer.poll(0)

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
            return {'url': object_['url'], 'type': object_['type']}
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
