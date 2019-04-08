# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.journal.backfill import (
    JournalBackfiller, compute_query
)


TEST_CONFIG = {
    'brokers': ['localhost'],
    'final_prefix': 'swh.tmp_journal.new',
    'client_id': 'swh.journal.publisher.test',
    'storage_dbconn': 'service=swh-dev',
}


def test_config_ko_missing_mandatory_key():
    """Missing configuration key will make the initialization fail

    """
    for key in TEST_CONFIG.keys():
        config = TEST_CONFIG.copy()
        config.pop(key)

        with pytest.raises(ValueError) as e:
            JournalBackfiller(config)

        error = ('Configuration error: The following keys must be'
                 ' provided: %s' % (','.join([key]), ))
        assert e.value.args[0] == error


# def test_config_ko_unknown_object_type():
#     """Parse arguments will fail if the object type is unknown

#     """
#     backfiller = JournalBackfiller(TEST_CONFIG)
#     with pytest.raises(ValueError) as e:
#         backfiller.parse_arguments('unknown-object-type', 1, 2)

#     error = ('Object type unknown-object-type is not supported. '
#              'The only possible values are %s' % (
#                  ', '.join(PARTITION_KEY)))
#     assert e.value.args[0] == error


def test_compute_query_content():
    query, where_args, column_aliases = compute_query(
        'content', '\x000000', '\x000001')

    assert where_args == ['\x000000', '\x000001']

    assert column_aliases == [
        'sha1', 'sha1_git', 'sha256', 'blake2s256', 'length', 'status',
        'ctime'
    ]

    assert query == '''
select sha1,sha1_git,sha256,blake2s256,length,status,ctime
from content

where (sha1) >= %s and (sha1) < %s
    '''
