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

import logging
import psycopg2

from .direct_writer import DirectKafkaWriter

from swh.core.db import typecast_bytea

logger = logging.getLogger(__name__)


# Defining the key components per object type
TYPE_TO_PARTITION_KEY = {
    'content': ['sha1'],
    # 'skipped_content': ['sha1'],
    # 'directory': ['id'],
    # 'revision': ['id'],
    # 'release': ['id'],
    # 'origin': ['type', 'url'],
    # 'origin_visit': ['type', 'url', 'fetch_date', 'visit_date'],
}

# The columns to read per object type
TYPE_TO_COLUMNS = {
    'content': [
        'sha1', 'sha1_git', 'sha256', 'blake2s256', 'length', 'status',
        'ctime'  # fix the conversion
    ],
    # 'skipped_content': [
    #     'sha1', 'sha1_git', 'sha256', 'blake2s256', 'length', 'ctime',
    #     'status', 'reason',
    # ],
    # 'origin': ['type', 'url'],
    # 'origin_visit': ['type', 'url', 'fetch_date', 'visit_date'],
    # 'directory': ['id'],
    # 'revision': ['id'],
    # 'release': ['id'],
}


def fetch(db_conn, obj_type):
    """Fetch all obj_type's identifiers from db.

    This opens one connection, stream objects and when done, close
    the connection.

    Raises:
        ValueError if obj_type is not supported

    Yields:
        Identifiers for the specific object_type

    """
    columns = TYPE_TO_COLUMNS.get(obj_type)
    if not columns:
        raise ValueError('The object type %s is not supported. '
                         'Only possible values are %s' % (
                             obj_type, TYPE_TO_PARTITION_KEY.keys()))

    columns_str = ','.join(columns)
    query = 'select %s from %s order by %s limit 1000' % (
        columns_str, obj_type, ','.join(TYPE_TO_PARTITION_KEY[obj_type]))
    logging.debug('query: %s', query)
    server_side_cursor_name = 'swh.journal.%s' % obj_type

    def cursor_setup(conn, server_side_cursor_name):
        """Setup cursor to return dict of data"""
        # cur = conn.cursor(name=server_side_cursor_name)
        cur = conn.cursor()
        cur.execute("SELECT null::bytea, null::bytea[]")
        bytea_oid = cur.description[0][1]
        bytea_array_oid = cur.description[1][1]

        t_bytes = psycopg2.extensions.new_type(
            (bytea_oid,), "bytea", typecast_bytea)
        psycopg2.extensions.register_type(t_bytes, conn)

        t_bytes_array = psycopg2.extensions.new_array_type(
            (bytea_array_oid,), "bytea[]", t_bytes)
        psycopg2.extensions.register_type(t_bytes_array, conn)

        return cur

    with psycopg2.connect(db_conn) as conn:
        cursor = cursor_setup(conn, server_side_cursor_name)
        logger.debug('Fetching data for table %s', obj_type)
        cursor.execute(query)
        for row in cursor:
            record = dict(zip(columns, row))
            logger.debug('record: %s' % record)
            logger.debug('keys: %s' % record.keys())
            yield record


MANDATORY_KEYS = [
    'brokers', 'object_types', 'storage_dbconn',
    'final_prefix', 'client_id',
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

        self.writer = DirectKafkaWriter(
            brokers=config['brokers'],
            prefix=config['final_prefix'],
            client_id=config['client_id']
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
            if obj_type not in TYPE_TO_PARTITION_KEY:
                raise ValueError('The object type %s is not supported. '
                                 'Possible values are %s' % (
                                     obj_type,
                                     ', '.join(TYPE_TO_PARTITION_KEY)))

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
            self.writer.write_addition(object_type=obj_type, object_=obj)


if __name__ == '__main__':
    print('Please use the "swh-journal backfiller run" command')
