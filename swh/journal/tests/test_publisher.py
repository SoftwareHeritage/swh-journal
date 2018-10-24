# Copyright (C) 2018 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from swh.model.hashutil import hash_to_bytes
from swh.journal.publisher import SWHJournalPublisher


class MockStorage:
    # Type from object to their corresponding expected key id
    type_to_key = {
        'content': 'sha1',
        'revision': 'id',
        'release': 'id',
    }

    def __init__(self, state):
        """Initialize mock storage's state.

        Args:
            state (dict): keys are the object type (content, revision,
                          release) and values are a list of dict
                          representing the associated typed objects

        """
        self.state = {}
        for type, key in self.type_to_key.items():
            self.state[type] = {
                obj[key]: obj for obj in state[type]
            }

    def get(self, type, objects):
        """Given an object type and a list of objects with type type, returns
           the state's matching objects.

        Args:
            type (str): expected object type (release, revision, content)
            objects ([bytes]): list of object id (bytes)

        Returns:
            list of dict corresponding to the id provided

        """
        data = []
        if type not in self.type_to_key:
            raise ValueError('Programmatic error: expected %s not %s' % (
                ', '.join(self.type_to_key), type
            ))
        object_ids = self.state[type]
        for _id in objects:
            c = object_ids.get(_id)
            if c:
                data.append(c)

        return data

    def content_get_metadata(self, contents):
        return self.get('content', contents)

    def revision_get(self, revisions):
        return self.get('revision', revisions)

    def release_get(self, releases):
        return self.get('release', releases)


CONTENTS = [
    {
        'data': b'42\n',
        'length': 3,
        'sha1': hash_to_bytes(
            '34973274ccef6ab4dfaaf86599792fa9c3fe4689'),
        'status': 'visible',
    },
]

REVISIONS = [
    {
        'id': hash_to_bytes('7026b7c1a2af56521e951c01ed20f255fa054238'),
        'message': b'hello',
    },
    {
        'id': hash_to_bytes('368a48fe15b7db2383775f97c6b247011b3f14f4'),
        'message': b'hello again',
    },
]

RELEASES = [
    {
        'id': hash_to_bytes('d81cc0710eb6cf9efd5b920a8453e1e07157b6cd'),
        'name': b'v0.0.1',
    },
]


class JournalPublisherTest(SWHJournalPublisher):
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
        self.storage = MockStorage(state={
            'content': CONTENTS,
            'revision': REVISIONS,
            'release': RELEASES,
        })

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

    def test_process_objects(self):
        messages = {
            'content': self.contents,
            'revision': self.revisions,
            'release': self.releases,
        }

        actual_objects = self.publisher.process_objects(messages)

        expected_contents = [(c['sha1'], c) for c in CONTENTS]
        expected_revisions = [(c['id'], c) for c in REVISIONS]
        expected_releases = [(c['id'], c) for c in RELEASES]

        expected_objects = {
            'content': expected_contents,
            'revision': expected_revisions,
            'release': expected_releases,
        }

        self.assertEqual(actual_objects, expected_objects)
