# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from functools import wraps

import psycopg2
import psycopg2.extras


def entry_to_bytes(entry):
    """Convert an entry coming from the database to bytes"""
    if isinstance(entry, memoryview):
        return entry.tobytes()
    if isinstance(entry, list):
        return [entry_to_bytes(value) for value in entry]
    return entry


def line_to_bytes(line):
    """Convert a line coming from the database to bytes"""
    if not line:
        return line
    if isinstance(line, dict):
        return {k: entry_to_bytes(v) for k, v in line.items()}
    return line.__class__(entry_to_bytes(entry) for entry in line)


def cursor_to_bytes(cursor):
    """Yield all the data from a cursor as bytes"""
    yield from (line_to_bytes(line) for line in cursor)


def autocommit(fn):
    @wraps(fn)
    def wrapped(self, *args, **kwargs):
        autocommit = False
        if 'cursor' not in kwargs or not kwargs['cursor']:
            autocommit = True
            kwargs['cursor'] = self.cursor()

        try:
            ret = fn(self, *args, **kwargs)
        except:
            if autocommit:
                self.rollback()
            raise

        if autocommit:
            self.commit()

        return ret

    return wrapped


class Backend:
    """Backend for querying Software Heritage object identifiers.

    """

    def __init__(self, db_conn):
        self.db_conn = db_conn
        self.db = None
        self.reconnect()

    def reconnect(self):
        if not self.db or self.db.closed:
            self.db = psycopg2.connect(dsn=self.db_conn)

    def cursor(self):
        """Return a fresh cursor on the database, with auto-reconnection in
        case of failure

        """
        cur = None

        # Get a fresh cursor and reconnect at most three times
        tries = 0
        while True:
            tries += 1
            try:
                cur = self.db.cursor()
                cur.execute('select 1')
                break
            except psycopg2.OperationalError:
                if tries < 3:
                    self.reconnect()
                else:
                    raise

        return cur

    def commit(self):
        """Commit a transaction

        """
        self.db.commit()

    def rollback(self):
        """Rollback a transaction

        """
        self.db.rollback()

    @autocommit
    def content_get_ids(self, cursor=None):
        cursor.execute('select sha1 from content')
        for c in cursor_to_bytes(cursor):
            yield c[0]

    @autocommit
    def origin_get_ids(self, cursor=None):
        cursor.execute('select id from origin')
        for o in cursor_to_bytes(cursor):
            yield o[0]

    @autocommit
    def revision_get_ids(self, cursor=None):
        cursor.execute('select id from revision')
        for r in cursor_to_bytes(cursor):
            yield r[0]

    @autocommit
    def release_get_ids(self, cursor=None):
        cursor.execute('select id from release')
        for r in cursor_to_bytes(cursor):
            yield r[0]
