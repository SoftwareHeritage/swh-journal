# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Module defining journal checker classes.

Those checker goal is to send back all, or missing objects from the
journal queues.

At the moment, a first naive implementation is the
SWHSimpleCheckerProducer.  It simply reads the objects from the
storage and sends every object identifier back to the journal.

"""

from kafka import KafkaProducer

from swh.core.config import SWHConfig
from .backend import Backend
from .serializers import value_to_kafka


class SWHJournalSimpleCheckerProducer(SWHConfig):
    """Class in charge of reading the storage's objects and sends those
       back to the publisher queue.

       This is designed to be run periodically.

    """
    DEFAULT_CONFIG = {
        'brokers': ('list[str]', ['getty.internal.softwareheritage.org']),
        'writing_prefix': ('str', 'swh.journal.objects'),
        'publisher_id': ('str', 'swh.journal.publisher.test'),
        'object_types': ('list[str]', ['content', 'revision', 'release']),
        'storage_dbconn': ('str', 'service=swh-dev'),
    }

    CONFIG_BASE_FILENAME = 'journal/checker'

    def __init__(self, extra_configuration=None):
        self.config = config = self.parse_config_file()
        if extra_configuration:
            config.update(extra_configuration)

        self.storage_backend = Backend(self.config['storage_dbconn'])

        self.producer = KafkaProducer(
            bootstrap_servers=config['brokers'],
            value_serializer=value_to_kafka,
            client_id=config['publisher_id'],
        )

        self.object_read_fn = {
            'content': self.storage_backend.content_get_ids,
            'origin': self.storage_backend.origin_get_ids,
            'revision': self.storage_backend.revision_get_ids,
            'release': self.storage_backend.release_get_ids,
        }

    def _read_storage(self):
        """Read all the storage's objects and returns as dict of object_types,
           set of identifiers.

        """
        for obj_type in self.config['object_types']:
            for obj_id in self.object_read_fn[obj_type]():
                yield obj_type, obj_id

    def run(self):
        """Reads storage's subscribed object types and send them all back to
           the publisher queue.

        """

        for obj_type, obj_id in self._read_storage():
            topic = '%s.%s' % (self.config['writing_prefix'], obj_type)
            self.producer.send(topic, value=obj_id)


if __name__ == '__main__':
    SWHJournalSimpleCheckerProducer().run()
