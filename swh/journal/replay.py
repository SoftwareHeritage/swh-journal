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
                assert messages
                for message in messages:
                    object_type = partition.topic.split('.')[-1]

                    # Got a message from a topic we did not subscribe to.
                    assert object_type in self._object_types, object_type

                    self.insert_object(storage, object_type, message.value)

                    nb_messages += 1
                    if done():
                        break
                if done():
                    break
            self.commit()
            logger.info('Processed %d messages.' % nb_messages)
        return nb_messages

    def insert_object(self, storage, object_type, object_):
        if object_type in ('content', 'directory', 'revision', 'release',
                           'snapshot', 'origin'):
            if object_type == 'content':
                try:
                    storage.content_add_metadata([object_])
                except HashCollision as e:
                    logger.error('Hash collision: %s', e.args)
            else:
                method = getattr(storage, object_type + '_add')
                method([object_])
        elif object_type == 'origin_visit':
            storage.origin_visit_upsert([{
                **object_,
                'origin': storage.origin_add_one(object_['origin'])}])
        else:
            assert False
