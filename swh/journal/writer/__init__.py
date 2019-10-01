# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


def get_journal_writer(cls, args={}):
    if cls == 'inmemory':  # FIXME: Remove inmemory in due time
        import warnings
        warnings.warn("cls = 'inmemory' is deprecated, use 'memory' instead",
                      DeprecationWarning)
        cls = 'memory'
    if cls == 'memory':
        from .inmemory import InMemoryJournalWriter as JournalWriter
    elif cls == 'kafka':
        from .kafka import KafkaJournalWriter as JournalWriter
    else:
        raise ValueError('Unknown journal writer class `%s`' % cls)

    return JournalWriter(**args)
