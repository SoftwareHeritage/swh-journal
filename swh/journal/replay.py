# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

from kafka import KafkaConsumer

from swh.storage import HashCollision

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

    def poll(self):
        return self.consumer.poll()

    def commit(self):
        self.consumer.commit()

    def fill(self, storage, max_messages=None):
        nb_messages = 0

        def done():
            nonlocal nb_messages
            return max_messages and nb_messages >= max_messages

        while not done():
            polled = self.poll()
            for (partition, messages) in polled.items():
                object_type = partition.topic.split('.')[-1]
                # Got a message from a topic we did not subscribe to.
                assert object_type in self._object_types, object_type

                self.insert_objects(storage, object_type,
                                    [msg.value for msg in messages])

                nb_messages += len(messages)
                if done():
                    break
            self.commit()
            logger.info('Processed %d messages.' % nb_messages)
        return nb_messages

    def insert_objects(self, storage, object_type, objects):
        if object_type in ('content', 'directory', 'revision', 'release',
                           'snapshot', 'origin'):
            if object_type == 'content':
                # TODO: insert 'content' in batches
                for object_ in objects:
                    try:
                        storage.content_add_metadata([object_])
                    except HashCollision as e:
                        logger.error('Hash collision: %s', e.args)
            else:
                # TODO: split batches that are too large for the storage
                # to handle?
                method = getattr(storage, object_type + '_add')
                method(objects)
        elif object_type == 'origin_visit':
            storage.origin_visit_upsert([
                {
                    **obj,
                    'origin': storage.origin_add_one(obj['origin'])
                }
                for obj in objects])
        else:
            assert False
