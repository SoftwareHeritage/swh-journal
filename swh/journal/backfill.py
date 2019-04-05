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
from swh.storage.converters import db_to_release, db_to_revision


logger = logging.getLogger(__name__)

RANGE_GENERATORS = {
    'content': lambda start, end: byte_ranges(24, start, end),
    'skipped_content': lambda start, end: [(None, None)],
    'directory': lambda start, end: byte_ranges(24, start, end),
    'revision': lambda start, end: byte_ranges(24, start, end),
    'release': lambda start, end: byte_ranges(16, start, end),
    'snapshot': lambda start, end: byte_ranges(16, start, end),
}

PARTITION_KEY = {
    'content': ['sha1'],
    'skipped_content': None,  # unused
    # 'directory': ['id'],
    'revision': ['revision.id'],
    'release': ['release.id'],
    # 'snapshot': ['id'],
    # 'origin': ['type', 'url'],
    # 'origin_visit': ['type', 'url', 'fetch_date', 'visit_date'],
}

COLUMNS = {
    'content': [
        'sha1', 'sha1_git', 'sha256', 'blake2s256', 'length', 'status',
        'ctime'
    ],
    'skipped_content': [
        'sha1', 'sha1_git', 'sha256', 'blake2s256', 'length', 'ctime',
        'status', 'reason',
    ],
    # 'directory': ['id'],
    'revision': [
        ("revision.id", "id"),
        "date",
        "date_offset",
        "committer_date",
        "committer_date_offset",
        "type",
        "directory",
        "message",
        "synthetic",
        "metadata",
        "date_neg_utc_offset",
        "committer_date_neg_utc_offset",
        ("array(select parent_id::bytea from revision_history rh "
         "where rh.id = revision.id order by rh.parent_rank asc)",
         "parents"),
        ("a.id", "author_id"),
        ("a.name", "author_name"),
        ("a.email", "author_email"),
        ("a.fullname", "author_fullname"),
        ("c.id", "committer_id"),
        ("c.name", "committer_name"),
        ("c.email", "committer_email"),
        ("c.fullname", "committer_fullname"),
    ],
    'release': [
        ("release.id", "id"),
        "date",
        "date_offset",
        "comment",
        ("release.name", "name"),
        "synthetic",
        "date_neg_utc_offset",
        "target",
        "target_type",
        ("a.id", "author_id"),
        ("a.name", "author_name"),
        ("a.email", "author_email"),
        ("a.fullname", "author_fullname"),
    ],
    # 'snapshot': ['id'],
    # 'origin': ['type', 'url'],
    # 'origin_visit': ['type', 'url', 'fetch_date', 'visit_date'],
}


JOINS = {
    'release': ['person a on release.author=a.id'],
    'revision': ['person a on revision.author=a.id',
                 'person c on revision.committer=c.id'],
}


def release_converter(release):
    """Convert release from the flat representation to swh model
       compatible objects.

    """
    release = db_to_release(release)
    if 'author' in release and release['author']:
        del release['author']['id']
    return release


def revision_converter(revision):
    """Convert revision from the flat representation to swh model
       compatible objects.

    """
    revision = db_to_revision(revision)
    if 'author' in revision and revision['author']:
        del revision['author']['id']
    if 'committer' in revision and revision['committer']:
        del revision['committer']['id']
    return revision


CONVERTERS = {
    'release': release_converter,
    'revision': revision_converter,
}


def object_to_offset(object_id, numbits):
    """Compute the index of the range containing object id, when dividing
       space into 2^numbits.

    Args:
        object_id (str): The hex representation of object_id
        numbits (int): Number of bits in which we divide input space

    Returns:
        The index of the range containing object id

    """
    q, r = divmod(numbits, 8)
    length = q + (r != 0)
    shift_bits = 8 - r if r else 0

    truncated_id = object_id[:length * 2]
    if len(truncated_id) < length * 2:
        truncated_id += '0' * (length * 2 - len(truncated_id))

    truncated_id_bytes = bytes.fromhex(truncated_id)
    return int.from_bytes(truncated_id_bytes, byteorder='big') >> shift_bits


def byte_ranges(numbits, start_object=None, end_object=None):
    """Generate start/end pairs of bytes spanning numbits bits and
       constrained by optional start_object and end_object.

    Args:
        numbits (int): Number of bits in which we divide input space
        start_object (str): Hex object id contained in the first range
                            returned
        end_object (str): Hex object id contained in the last range
                          returned

    Yields:
        2^numbits pairs of bytes

    """
    q, r = divmod(numbits, 8)
    length = q + (r != 0)
    shift_bits = 8 - r if r else 0

    def to_bytes(i):
        return int.to_bytes(i << shift_bits, length=length, byteorder='big')

    start_offset = 0
    end_offset = 1 << numbits

    if start_object is not None:
        start_offset = object_to_offset(start_object, numbits)
    if end_object is not None:
        end_offset = object_to_offset(end_object, numbits) + 1

    for start in range(start_offset, end_offset):
        end = start + 1

        if start == 0:
            yield None, to_bytes(end)
        elif end == 1 << numbits:
            yield to_bytes(start), None
        else:
            yield to_bytes(start), to_bytes(end)


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


def fetch(db_conn, obj_type, start, end):
    """Fetch all obj_type's identifiers from db.

    This opens one connection, stream objects and when done, close
    the connection.

    Args:
        conn: Db connection object
        obj_type (str): Object type
        start (Union[bytes|Tuple]): Range start identifier
        end (Union[bytes|Tuple]): Range end identifier

    Raises:
        ValueError if obj_type is not supported

    Yields:
        Objects in the given range

    """
    columns = COLUMNS.get(obj_type)
    if not columns:
        raise ValueError('The object type %s is not supported. '
                         'Only possible values are %s' % (
                             obj_type, PARTITION_KEY.keys()))

    join_specs = JOINS.get(obj_type)
    join_clause = '\n'.join('left join %s' % clause for clause in join_specs)

    where = []
    where_args = []
    if start:
        where.append('%(keys)s >= %%s')
        where_args.append(start)
    if end:
        where.append('%(keys)s < %%s')
        where_args.append(end)

    where_clause = ''
    if where:
        where_clause = ('where ' + ' and '.join(where)) % {
            'keys': '(%s)' % ','.join(PARTITION_KEY[obj_type])
        }

    column_specs = []
    column_aliases = []
    for column in columns:
        if isinstance(column, str):
            column_specs.append(column)
            column_aliases.append(column)
        else:
            column_specs.append('%s as %s' % column)
            column_aliases.append(column[1])

    query = '''
    select %(columns)s
    from %(table)s
    %(join)s
    %(where)s
    ''' % {
        'columns': ','.join(column_specs),
        'table': obj_type,
        'join': join_clause,
        'where': where_clause,
    }

    converter = CONVERTERS.get(obj_type)

    server_side_cursor_name = 'swh.journal.%s' % obj_type
    with psycopg2.connect(db_conn) as conn:
        cursor = cursor_setup(conn, server_side_cursor_name)
        logger.debug('Fetching data for table %s', obj_type)
        logger.debug('query: %s %s', query, where_args)
        cursor.execute(query, where_args)
        for row in cursor:
            record = dict(zip(column_aliases, row))
            if converter:
                record = converter(record)

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

    def run(self, object_type, start_object, end_object):
        """Reads storage's subscribed object types and send them to the
           publisher's reading queue.

        """
        for start, end in RANGE_GENERATORS[object_type](
                start_object, end_object):
            logger.info('Processing %s range %s to %s', object_type,
                        start, end)

            for obj in fetch(
                    self.storage_dbconn, object_type, start=start, end=end):
                self.writer.write_addition(object_type=object_type,
                                           object_=obj)


if __name__ == '__main__':
    print('Please use the "swh-journal backfiller run" command')
