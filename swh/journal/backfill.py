# Copyright (C) 2017-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Module defining journal backfiller classes.

Those backfiller goal is to produce back part or all of the objects
from the storage to the journal topics

At the moment, a first naive implementation is the
JournalBackfiller.  It simply reads the objects from the
storage and sends every object identifier back to the journal.

"""

import psycopg2

from kafka import KafkaProducer

from .serializers import key_to_kafka


TYPE_TO_PRIMARY_KEY = {
    'origin': ['id'],
    'content': ['sha1', 'sha1_git', 'sha256', 'blake2s256'],
    'directory': ['id'],
    'revision': ['id'],
    'release': ['id'],
    'origin_visit': ['origin', 'visit'],
    'skipped_content': ['sha1', 'sha1_git', 'sha256'],
}


def entry_to_bytes(entry):
    """Convert an entry coming from the database to bytes"""
    if isinstance(entry, memoryview):
        return entry.tobytes()
    if isinstance(entry, tuple):
        return [entry_to_bytes(value) for value in entry]
    return entry


def fetch(db_conn, obj_type):
    """Fetch all obj_type's identifiers from db.

    This opens one connection, stream objects and when done, close
    the connection.

    Raises:
        ValueError if obj_type is not supported

    Yields:
        Identifiers for the specific object_type

    """
    primary_key = TYPE_TO_PRIMARY_KEY.get(obj_type)
    if not primary_key:
        raise ValueError('The object type %s is not supported. '
                         'Only possible values are %s' % (
                             obj_type, TYPE_TO_PRIMARY_KEY.keys()))

    primary_key_str = ','.join(primary_key)
    query = 'select %s from %s order by %s' % (
        primary_key_str, obj_type, primary_key_str)
    server_side_cursor_name = 'swh.journal.%s' % obj_type

    with psycopg2.connect(db_conn) as db:
        cursor = db.cursor(name=server_side_cursor_name)
        cursor.execute(query)
        for o in cursor:
            yield dict(zip(primary_key, entry_to_bytes(o)))


DEFAULT_CONFIG = {
    'brokers': ('list[str]', ['getty.internal.softwareheritage.org']),
    'temporary_prefix': ('str', 'swh.tmp_journal.new'),
    'publisher_id': ('str', 'swh.journal.publisher.test'),
    'object_types': ('list[str]', ['content', 'revision', 'release']),
    'storage_dbconn': ('str', 'service=swh-dev'),
}


MANDATORY_KEYS = [
    'brokers', 'temporary_prefix', 'publisher_id', 'object_types',
    'storage_dbconn',
]


class JournalBackfiller:
    """Class in charge of reading the storage's objects and sends those
       back to the publisher queue.

       This is designed to be run periodically.

    """
    def __init__(self, config=None):
        self.config = config
        self.check_config(config)
        self.object_types = self.config['object_types']
        self.storage_dbconn = self.config['storage_dbconn']

        self.producer = KafkaProducer(
            bootstrap_servers=config['brokers'],
            value_serializer=key_to_kafka,
            client_id=config['publisher_id'],
        )

    def check_config(self, config):
        missing_keys = []
        for key in MANDATORY_KEYS:
            if not config.get(key):
                missing_keys.append(key)

        if missing_keys:
            raise ValueError(
                'Configuration error: The following keys must be'
                ' provided: %s' % (','.join(missing_keys), ))

        object_types = config['object_types']
        for obj_type in object_types:
            if obj_type not in TYPE_TO_PRIMARY_KEY:
                raise ValueError('The object type %s is not supported. '
                                 'Possible values are %s' % (
                                     obj_type,
                                     ', '.join(TYPE_TO_PRIMARY_KEY)))

    def _read_storage(self):
        """Read storage's objects and generates tuple as object_type, dict of
           object.

           Yields:
               tuple of object_type, object as dict

        """
        for obj_type in self.object_types:
            for obj in fetch(self.storage_dbconn, obj_type):
                yield obj_type, obj

    def run(self):
        """Reads storage's subscribed object types and send them to the
           publisher's reading queue.

        """
        for obj_type, obj in self._read_storage():
            topic = '%s.%s' % (self.config['temporary_prefix'], obj_type)
            self.producer.send(topic, value=obj)


if __name__ == '__main__':
    print('Please use the "swh-journal backfiller run" command')
