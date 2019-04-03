# Copyright (C) 2018-2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest
import unittest

from .conftest import (
    JournalPublisherTest, TEST_CONFIG,
    CONTENTS, REVISIONS, RELEASES, ORIGINS
)
from swh.journal.publisher import MANDATORY_KEYS


class JournalPublisherNoKafkaInMemoryStorage(JournalPublisherTest):
    """A journal publisher with:
    - no kafka dependency
    - in-memory storage

    """
    def check_config(self, config):
        """No need to check the configuration here as we do not use kafka

        """
        pass

    def _prepare_journal(self, config):
        """No journal for now

        """
        pass


class TestPublisherNoKafka(unittest.TestCase):
    """This tests only the part not using any kafka instance

    """
    def setUp(self):
        self.publisher = JournalPublisherNoKafkaInMemoryStorage(TEST_CONFIG)
        self.contents = [{b'sha1': c['sha1']} for c in CONTENTS]
        self.revisions = [{b'id': c['id']} for c in REVISIONS]
        self.releases = [{b'id': c['id']} for c in RELEASES]
        # those needs id generation from the storage
        # so initialization is different than other entities
        self.origins = [{b'url': o['url'],
                         b'type': o['type']}
                        for o in self.publisher.origins]
        self.origin_visits = [{b'origin': ov['origin'],
                               b'visit': ov['visit']}
                              for ov in self.publisher.origin_visits]
        # full objects
        storage = self.publisher.storage
        ovs = []
        for ov in self.origin_visits:
            _ov = storage.origin_visit_get_by(
                ov[b'origin'], ov[b'visit'])
            _ov['date'] = str(_ov['date'])
            ovs.append(_ov)
        self.expected_origin_visits = ovs

    def test_process_contents(self):
        actual_contents = self.publisher.process_contents(self.contents)
        expected_contents = [(c['sha1'], c) for c in CONTENTS]
        self.assertEqual(actual_contents, expected_contents)

    def test_process_revisions(self):
        actual_revisions = self.publisher.process_revisions(self.revisions)
        expected_revisions = [(c['id'], c) for c in REVISIONS]
        self.assertEqual(actual_revisions, expected_revisions)

    def test_process_releases(self):
        actual_releases = self.publisher.process_releases(self.releases)
        expected_releases = [(c['id'], c) for c in RELEASES]
        self.assertEqual(actual_releases, expected_releases)

    def test_process_origins(self):
        actual_origins = self.publisher.process_origins(self.origins)
        expected_origins = [({'url': o[b'url'], 'type': o[b'type']},
                             {'url': o[b'url'], 'type': o[b'type']})
                            for o in self.origins]
        self.assertEqual(actual_origins, expected_origins)

    def test_process_origin_visits(self):
        actual_ovs = self.publisher.process_origin_visits(self.origin_visits)
        expected_ovs = [((ov['origin'], ov['visit']), ov)
                        for ov in self.expected_origin_visits]
        self.assertEqual(actual_ovs, expected_ovs)

    def test_process_objects(self):
        messages = {
            'content': self.contents,
            'revision': self.revisions,
            'release': self.releases,
            'origin': self.origins,
            'origin_visit': self.origin_visits,
        }

        actual_objects = self.publisher.process_objects(messages)

        expected_contents = [(c['sha1'], c) for c in CONTENTS]
        expected_revisions = [(c['id'], c) for c in REVISIONS]
        expected_releases = [(c['id'], c) for c in RELEASES]
        expected_origins = [(o, o) for o in ORIGINS]
        expected_ovs = [((ov['origin'], ov['visit']), ov)
                        for ov in self.expected_origin_visits]
        expected_objects = {
            'content': expected_contents,
            'revision': expected_revisions,
            'release': expected_releases,
            'origin': expected_origins,
            'origin_visit': expected_ovs,
        }

        self.assertEqual(actual_objects, expected_objects)


class JournalPublisherCheckTest(JournalPublisherTest):
    """A journal publisher with:
    - no kafka dependency
    - in-memory storage

    """
    def _prepare_journal(self, config):
        """No journal for now

        """
        pass


def test_check_config_ok(test_config):
    """Instantiate a publisher with the right config is fine

    """
    publisher = JournalPublisherCheckTest(test_config)
    assert publisher is not None


def test_check_config_ko(test_config):
    """Instantiate a publisher with the wrong config should raise

    """
    for k in MANDATORY_KEYS:
        conf = test_config.copy()
        conf.pop(k)
        with pytest.raises(ValueError) as e:
            JournalPublisherCheckTest(conf)

        error = ('Configuration error: The following keys must be'
                 ' provided: %s' % (','.join([k]), ))
        assert e.value.args[0] == error
