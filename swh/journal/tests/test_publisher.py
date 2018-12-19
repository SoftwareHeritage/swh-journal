# Copyright (C) 2018 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from swh.model.hashutil import hash_to_bytes
from swh.journal.publisher import JournalPublisher
from swh.storage.in_memory import Storage

CONTENTS = [
    {
        'length': 3,
        'sha1': hash_to_bytes(
            '34973274ccef6ab4dfaaf86599792fa9c3fe4689'),
        'sha1_git': b'foo',
        'blake2s256': b'bar',
        'sha256': b'baz',
        'status': 'visible',
    },
]

REVISIONS = [
    {
        'id': hash_to_bytes('7026b7c1a2af56521e951c01ed20f255fa054238'),
        'message': b'hello',
        'date': {
            'timestamp': {
                'seconds': 1234567891,
                'microseconds': 0,
            },
            'offset': 120,
            'negative_utc': None,
        },
        'committer': None,
        'committer_date': None,
    },
    {
        'id': hash_to_bytes('368a48fe15b7db2383775f97c6b247011b3f14f4'),
        'message': b'hello again',
        'date': {
            'timestamp': {
                'seconds': 1234567892,
                'microseconds': 0,
            },
            'offset': 120,
            'negative_utc': None,
        },
        'committer': None,
        'committer_date': None,
    },
]

RELEASES = [
    {
        'id': hash_to_bytes('d81cc0710eb6cf9efd5b920a8453e1e07157b6cd'),
        'name': b'v0.0.1',
        'date': {
            'timestamp': {
                'seconds': 1234567890,
                'microseconds': 0,
            },
            'offset': 120,
            'negative_utc': None,
        },
    },
]

ORIGINS = [
    {
        'url': 'https://somewhere.org/den/fox',
        'type': 'git',
    },
    {
        'url': 'https://overtherainbow.org/fox/den',
        'type': 'svn',
    }
]

ORIGIN_VISITS = [
    {
        'date': '2013-05-07T04:20:39.369271+00:00',
    },
    {
        'date': '2018-11-27T17:20:39.000000+00:00',
    }
]


class JournalPublisherTest(JournalPublisher):
    def parse_config_file(self):
        return {
            'brokers': ['localhost'],
            'temporary_prefix': 'swh.tmp_journal.new',
            'final_prefix': 'swh.journal.objects',
            'consumer_id': 'swh.journal.test.publisher',
            'publisher_id': 'swh.journal.test.publisher',
            'object_types': ['content'],
            'max_messages': 3,
        }

    def _prepare_storage(self, config):
        self.storage = Storage()
        self.storage.content_add({'data': b'42', **c} for c in CONTENTS)
        self.storage.revision_add(REVISIONS)
        self.storage.release_add(RELEASES)
        origins = self.storage.origin_add(ORIGINS)
        origin_visits = []
        for i, ov in enumerate(ORIGIN_VISITS):
            origin_id = origins[i]['id']
            ov = self.storage.origin_visit_add(origin_id, ov['date'])
            origin_visits.append(ov)
        self.origins = origins
        self.origin_visits = origin_visits

    def _prepare_journal(self, config):
        """No journal for now

        """
        pass


class TestPublisher(unittest.TestCase):
    def setUp(self):
        self.publisher = JournalPublisherTest()
        self.contents = [{b'sha1': c['sha1']} for c in CONTENTS]
        self.revisions = [{b'id': c['id']} for c in REVISIONS]
        self.releases = [{b'id': c['id']} for c in RELEASES]
        # those needs id generation from the storage
        # so initialization is different than other entities
        self.origins = [{'url': o['url'],
                         'type': o['type']}
                        for o in self.publisher.origins]
        self.origin_visits = [{'origin': ov['origin'],
                               'visit': ov['visit']}
                              for ov in self.publisher.origin_visits]
        # full objects
        storage = self.publisher.storage
        ovs = []
        for ov in self.origin_visits:
            ovs.append(storage.origin_visit_get_by(**ov))
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
        expected_origins = self.origins
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
        expected_origins = ORIGINS
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
