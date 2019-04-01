# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

from kafka import KafkaConsumer

from .serializers import kafka_to_value

logger = logging.getLogger(__name__)


OBJECT_TYPES = frozenset([
    'origin', 'origin_visit', 'snapshot', 'release', 'revision',
    'directory', 'content',
])


class StorageReplayer:
    def __init__(self, brokers, prefix, consumer_id,
                 object_types=OBJECT_TYPES):
        if not set(object_types).issubset(OBJECT_TYPES):
            raise ValueError('Unknown object types: %s' % ', '.join(
                set(object_types) - OBJECT_TYPES))

        self._object_types = object_types
        self.consumer = KafkaConsumer(
            bootstrap_servers=brokers,
            value_deserializer=kafka_to_value,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            group_id=consumer_id,
        )
        self.consumer.subscribe(
            topics=['%s.%s' % (prefix, object_type)
                    for object_type in object_types],
        )

    def fill(self, storage, max_messages):
        num = 0
        for message in self.consumer:
            object_type = message.topic.split('.')[-1]

            # Got a message from a topic we did not subscribe to.
            assert object_type in self._object_types, object_type

            self.insert_object(storage, object_type, message.value)

            num += 1
            if num >= max_messages:
                break
        return num

    def insert_object(self, storage, object_type, object_):
        if object_type in ('content', 'directory', 'revision', 'release',
                           'origin'):
            if object_type == 'content':
                # TODO: we don't write contents in Kafka, so we need to
                # find a way to insert them somehow.
                object_['status'] = 'absent'
            method = getattr(storage, object_type + '_add')
            method([object_])
        elif object_type == 'origin_visit':
            origin_id = storage.origin_add_one(object_.pop('origin'))
            visit = storage.origin_visit_add(
                origin=origin_id, date=object_.pop('date'))
            storage.origin_visit_update(
                origin_id, visit['visit'], **object_)
        else:
            assert False
