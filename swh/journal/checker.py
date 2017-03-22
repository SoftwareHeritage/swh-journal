# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Module defining a class in charge of computing the missing objects
from the journal queues and sending those back to the queues.

"""

from collections import defaultdict
from kafka import KafkaProducer, KafkaConsumer

from swh.core.config import SWHConfig
from .backend import Backend
from .serializers import kafka_to_value, value_to_kafka


# Dict from object to its identifier
OBJECT_TO_ID_FN = {
    'content': lambda c: c[b'sha1'],
    'origin': lambda o: o[b'id'],
    'revision': lambda r: r[b'id'],
    'release': lambda r: r[b'id'],
}


class SWHJournalChecker(SWHConfig):
    """Class in charge of computing a diff against list of objects and the
       actual swh-storage's objects.  The missing objects resulting
       from that diff are queued back in the publisher's queues.

       This is designed to be run periodically.

    """
    DEFAULT_CONFIG = {
        'brokers': ('list[str]', ['getty.internal.softwareheritage.org']),

        'reading_prefix': ('str', 'swh.journal.objects'),
        'writing_prefix': ('str', 'swh.journal.objects'),

        'consumer_id': ('str', 'swh.journal.publisher.test'),
        'publisher_id': ('str', 'swh.journal.publisher.test'),

        'object_types': ('list[str]', ['content', 'revision', 'release']),
        'diff_journal': ('bool', False),

        'storage_dbconn': ('str', 'service=swh-dev'),
    }

    CONFIG_BASE_FILENAME = 'journal/checker'

    def __init__(self, extra_configuration=None):
        self.config = config = self.parse_config_file()
        if extra_configuration:
            config.update(extra_configuration)

        self.storage_backend = Backend(self.config['storage_dbconn'])

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

        self.diff_journal = self.config['diff_journal']

        self.object_read_fn = {
            'content': self.storage_backend.content_get_ids,
            'origin': self.storage_backend.origin_get_ids,
            'revision': self.storage_backend.revision_get_ids,
            'release': self.storage_backend.release_get_ids,
        }

        if self.diff_journal:
            self.consumer.subscribe(
                topics=['%s.%s' % (config['reading_prefix'], obj_type)
                        for obj_type in config['object_types']],
            )

    def _read_journal(self):
        """Read all the journal objects and returns as a dict of obj_type,
           set of identifiers.

        """
        journal_objs = defaultdict(set)
        for message in self.consumer:
            obj_type = message.topic.split('.')[-1]
            obj_id = OBJECT_TO_ID_FN[obj_type](message.value)
            journal_objs[obj_type].add(obj_id)

        return journal_objs

    def _read_storage(self):
        """Read all the storage's objects and returns as dict of object_types,
           set of identifiers.

        """
        storage_objs = {}
        for obj_type in self.config['object_types']:
            storage_objs[obj_type] = set(self.object_read_fn[obj_type]())

        return storage_objs

    def _compute_diff(self, storage_objs, journal_objs):
        """Compute the difference between storage_objects and journal_objects.

        Args:
            storage_objects (dict): objects from storage, key is the
                                    type, value is the set of ids for
                                    that type.
            journal_objects (dict): objects from journal, key is the
                                    type, value is the set of ids for
                                    that type.

        Returns:
            dict of difference for each object_type

        """
        objects = {}
        for obj_type in self.config['object_types']:
            objects[obj_type] = storage_objs[obj_type] - journal_objs[obj_type]

        return objects

    def run(self):
        """Reads storage's subscribed object types and send them all back to
           the publisher queue.

           Optionally, reads journal's objects and compute the
           difference. The missing objects present in the storage and
           missing from the journal are sent back to the journal.

        """
        storage_objs = self._read_storage()

        if self.diff_journal:
            journal_objs = self._read_journal()
            objects = self._compute_diffs(storage_objs, journal_objs)
        else:
            objects = storage_objs

        for obj_type, objs in objects.items():
            topic = '%s.%s' % (self.config['writing_prefix'], obj_type)
            for obj_id in objs:
                self.producer.send(topic, value=obj_id)


if __name__ == '__main__':
    SWHJournalChecker().run()
