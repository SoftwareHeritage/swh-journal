# Copyright (C) 2016-2017 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import defaultdict
import logging

from kafka import KafkaProducer, KafkaConsumer

from swh.core.config import SWHConfig
from swh.storage import get_storage

from .serializers import kafka_to_key, key_to_kafka


class SWHJournalPublisher(SWHConfig):
    DEFAULT_CONFIG = {
        'brokers': ('list[str]', ['getty.internal.softwareheritage.org']),

        'temporary_prefix': ('str', 'swh.tmp_journal.new'),
        'final_prefix': ('str', 'swh.journal.objects'),

        'consumer_id': ('str', 'swh.journal.publisher'),
        'publisher_id': ('str', 'swh.journal.publisher'),

        'object_types': ('list[str]', ['content', 'revision', 'release']),

        'storage': ('dict', {
            'cls': 'remote',
            'args': {
                'url': 'http://localhost:5002/',
            }
        }),

        'max_messages': ('int', 10000),
    }

    CONFIG_BASE_FILENAME = 'journal/publisher'

    def __init__(self, extra_configuration=None):
        self.config = config = self.parse_config_file()
        if extra_configuration:
            config.update(extra_configuration)

        self.storage = get_storage(**config['storage'])

        # yes, the temporary topics contain values that are actually _keys_
        self.consumer = KafkaConsumer(
            bootstrap_servers=config['brokers'],
            value_deserializer=kafka_to_key,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            group_id=config['consumer_id'],
        )
        self.producer = KafkaProducer(
            bootstrap_servers=config['brokers'],
            key_serializer=key_to_kafka,
            value_serializer=key_to_kafka,
            client_id=config['publisher_id'],
        )

        self.consumer.subscribe(
            topics=['%s.%s' % (config['temporary_prefix'], object_type)
                    for object_type in config['object_types']],
        )

        self.max_messages = self.config['max_messages']

    def poll(self):
        """Process a batch of messages"""
        num = 0
        messages = defaultdict(list)

        for num, message in enumerate(self.consumer):
            object_type = message.topic.split('.')[-1]
            messages[object_type].append(message.value)
            if num >= self.max_messages:
                break

        new_objects = self.process_objects(messages)
        self.produce_messages(new_objects)
        self.consumer.commit()

    def process_objects(self, messages):
        processors = {
            'content': self.process_contents,
            'revision': self.process_revisions,
            'release': self.process_releases,
        }

        return {
            key: processors[key](value)
            for key, value in messages.items()
        }

    def produce_messages(self, messages):
        for object_type, objects in messages.items():
            topic = '%s.%s' % (self.config['final_prefix'], object_type)
            for key, object in objects:
                self.producer.send(topic, key=key, value=object)

        self.producer.flush()

    def process_contents(self, content_objs):
        metadata = self.storage.content_get_metadata(
            (c[b'sha1'] for c in content_objs))
        return [(content['sha1'], content) for content in metadata]

    def process_revisions(self, revision_objs):
        metadata = self.storage.revision_get((r[b'id'] for r in revision_objs))
        return [(revision['id'], revision) for revision in metadata]

    def process_releases(self, release_objs):
        metadata = self.storage.release_get((r[b'id'] for r in release_objs))
        return [(release['id'], release) for release in metadata]


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(process)d %(levelname)s %(message)s'
    )
    publisher = SWHJournalPublisher()
    while True:
        publisher.poll()
