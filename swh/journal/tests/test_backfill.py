# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.journal.backfill import JournalBackfiller, TYPE_TO_PRIMARY_KEY


TEST_CONFIG = {
    'brokers': ['localhost'],
    'final_prefix': 'swh.tmp_journal.new',
    'client_id': 'swh.journal.publisher.test',
    'object_types': ['content', 'revision', 'release'],
    'storage_dbconn': 'service=swh-dev',
}


def test_config_ko_missing_mandatory_key():
    for key in TEST_CONFIG.keys():
        config = TEST_CONFIG.copy()
        config.pop(key)

        with pytest.raises(ValueError) as e:
            JournalBackfiller(config)

        error = ('Configuration error: The following keys must be'
                 ' provided: %s' % (','.join([key]), ))
        assert e.value.args[0] == error


def test_config_ko_unknown_object_type():
    wrong_config = TEST_CONFIG.copy()
    wrong_config['object_types'] = ['something-wrong']
    with pytest.raises(ValueError) as e:
        JournalBackfiller(wrong_config)

    error = ('The object type something-wrong is not supported. '
             'Possible values are %s' % (
                 ', '.join(TYPE_TO_PRIMARY_KEY)))
    assert e.value.args[0] == error
