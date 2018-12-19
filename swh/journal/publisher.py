# Copyright (C) 2016-2018 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import defaultdict
import logging

from kafka import KafkaProducer, KafkaConsumer

from swh.core.config import SWHConfig
from swh.storage import get_storage
from swh.storage.algos import snapshot

from .serializers import kafka_to_key, key_to_kafka


class JournalPublisher(SWHConfig):
    """The journal publisher is a layer in charge of:

    - consuming messages from topics (1 topic per object_type)
    - reify the object ids read from those topics (using the storage)
    - producing those reified objects to output topics (1 topic per
      object type)

    The main entry point for this class is the 'poll' method.

    """
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

        self._prepare_storage(config)
        self._prepare_journal(config)

        self.max_messages = self.config['max_messages']

    def _prepare_journal(self, config):
        """Prepare the consumer and subscriber instances for the publisher to
           actually be able to discuss with the journal.

        """
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

    def _prepare_storage(self, config):
        """Prepare the storage instance needed for the publisher to be able to
           discuss with the storage to retrieve the objects.

        """
        self.storage = get_storage(**config['storage'])

    def poll(self, max_messages=None):
        """Process a batch of messages from the consumer's topics. Use the
           storage to reify those ids. Produces back those reified
           objects to the production topics.

           This method polls a given amount of message then stops.
           The number of messages to consume is either provided or
           configured as fallback.

           The following method is expected to be called from within a
           loop.

        """
        messages = defaultdict(list)
        if max_messages is None:
            max_messages = self.max_messages

        for num, message in enumerate(self.consumer):
            object_type = message.topic.split('.')[-1]
            messages[object_type].append(message.value)
            if num >= max_messages:
                break

        new_objects = self.process_objects(messages)
        self.produce_messages(new_objects)
        self.consumer.commit()

    def process_objects(self, messages):
        """Given a dict of messages {object type: [object id]}, reify those
           ids to swh object from the storage and returns a
           corresponding dict.

        Args:
            messages (dict): Dict of {object_type: [id-as-bytes]}

        Returns:
            Dict of {object_type: [tuple]}.

                object_type (str): content, revision, release
                tuple (bytes, dict): object id as bytes, object as swh dict.

        """
        processors = {
            'content': self.process_contents,
            'revision': self.process_revisions,
            'release': self.process_releases,
            'snapshot': self.process_snapshots,
            'origin': self.process_origins,
            'origin_visit': self.process_origin_visits,
        }

        return {
            key: processors[key](value)
            for key, value in messages.items()
        }

    def produce_messages(self, messages):
        """Produce new swh object to the producer topic.

        Args:
            messages ([dict]): Dict of {object_type: [tuple]}.

                object_type (str): content, revision, release
                tuple (bytes, dict): object id as bytes, object as swh dict.

        """
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

    def process_origins(self, origin_objs):
        return origin_objs

    def process_origin_visits(self, origin_visits):
        metadata = []
        for ov in origin_visits:
            origin_visit = self.storage.origin_visit_get_by(
                ov['origin'], ov['visit'])
            if origin_visit:
                pk = ov['origin'], ov['visit']
                metadata.append((pk, origin_visit))
        return metadata

    def process_snapshots(self, snapshot_objs):
        metadata = []
        for snap in snapshot_objs:
            full_obj = snapshot.snapshot_get_all_branches(
                self.storage, snap[b'id'])
            metadata.append((full_obj['id'], full_obj))

        return metadata


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(process)d %(levelname)s %(message)s'
    )
    publisher = JournalPublisher()
    while True:
        publisher.poll()
