# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import defaultdict
import logging

from kafka import KafkaProducer, KafkaConsumer

from swh.core.config import SWHConfig
from swh.storage import get_storage

from .serializers import kafka_to_value, value_to_kafka


class SWHJournalPublisher(SWHConfig):
    DEFAULT_CONFIG = {
        'brokers': ('list[str]', ['getty.internal.softwareheritage.org']),

        'temporary_prefix': ('str', 'swh.tmp_journal.new'),
        'final_prefix': ('str', 'swh.journal.test_publisher'),

        'consumer_id': ('str', 'swh.journal.publisher.test'),
        'publisher_id': ('str', 'swh.journal.publisher.test'),

        'object_types': ('list[str]', ['content', 'revision', 'release']),

        'storage_class': ('str', 'local_storage'),
        'storage_args': ('list[str]', ['service=softwareheritage',
                                       '/srv/softwareheritage/objects']),
    }

    CONFIG_BASE_FILENAME = 'journal/publisher'

    def __init__(self, extra_configuration=None):
        self.config = config = self.parse_config_file()
        if extra_configuration:
            config.update(extra_configuration)

        self.storage = get_storage(config['storage_class'],
                                   config['storage_args'])

        self.consumer = KafkaConsumer(
            bootstrap_servers=config['brokers'],
            value_deserializer=kafka_to_value,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            group_id=config['consumer_id'],
        )
        self.producer = KafkaProducer(
            bootstrap_servers=config['brokers'],
            value_serializer=value_to_kafka,
            client_id=config['publisher_id'],
        )

        self.consumer.subscribe(
            topics=['%s.%s' % (config['temporary_prefix'], object_type)
                    for object_type in config['object_types']],
        )

    def poll(self, max_messages):
        """Process a batch of messages"""
        num = 0
        messages = defaultdict(list)

        for num, message in enumerate(self.consumer):
            object_type = message.topic.split('.')[-1]
            messages[object_type].append(message.value)
            if num >= max_messages:
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
        metadata = self.storage.content_get_metadata(content_objs)
        return [(content['sha1'], content) for content in metadata]

    def process_revisions(self, revision_objs):
        metadata = self.storage.revision_get(revision_objs)
        return [(revision['id'], revision) for revision in metadata]

    def process_releases(self, release_objs):
        metadata = self.storage.release_get(release_objs)
        return [(release['id'], release) for release in metadata]

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(process)d %(levelname)s %(message)s'
    )
    publisher = SWHJournalPublisher()
    while True:
        publisher.poll(10000)
