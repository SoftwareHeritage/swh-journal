# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import psycopg2
import psycopg2.extras


def entry_to_bytes(entry):
    """Convert an entry coming from the database to bytes"""
    if isinstance(entry, memoryview):
        return entry.tobytes()
    if isinstance(entry, list):
        return [entry_to_bytes(value) for value in entry]
    return entry


class Backend:
    """Backend for Software Heritage object identifiers batch retrieval.

    The need is to retrieve all the identifiers per object type fast (stream).
    For this, the implementation is using:
    - server side cursor
    - one db connection per object type

    """
    _map_type_primary_key = {
        'origin': 'id',
        'content': 'sha1',
        'directory': 'id',
        'revision': 'id',
        'release': 'id',
    }

    def __init__(self, db_conn):
        self.db_conn = db_conn

    def fetch(self, obj_type):
        """"""
        primary_key = self._map_type_primary_key.get(obj_type)
        if not primary_key:
            raise ValueError('The object type %s is not supported. '
                             'Only possible values are %s' % (
                                 obj_type, self._map_type_primary_key.keys()))

        query = 'select %s from %s order by %s' % (
            primary_key, obj_type, primary_key)
        server_side_cursor_name = 'swh.journal.%s' % obj_type

        with psycopg2.connect(dsn=self.db_conn) as db:
            cursor = db.cursor(name=server_side_cursor_name)
            cursor.execute(query)
            for o in cursor:
                yield entry_to_bytes(o[0])
