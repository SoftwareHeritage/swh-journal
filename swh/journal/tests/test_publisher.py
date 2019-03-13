# Copyright (C) 2018-2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from pathlib import Path
from typing import Tuple

from swh.model.hashutil import hash_to_bytes
from swh.journal.publisher import JournalPublisher
from swh.storage.in_memory import Storage
from kafka import KafkaConsumer, KafkaProducer

from subprocess import Popen
from pytest_kafka import (
    make_zookeeper_process, make_kafka_server, make_kafka_consumer
)
# from swh.journal.serializers import kafka_to_key, key_to_kafka


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
        'length': 3,
        'sha1': hash_to_bytes(
            '34973274ccef6ab4dfaaf86599792fa9c3fe4689'),
        'sha1_git': b'foo',
        'blake2s256': b'bar',
        'sha256': b'baz',
        'status': 'visible',
    },
]

COMMITTER = [
    {
        'id': 1,
        'fullname': 'foo',
    },
    {
        'id': 2,
        'fullname': 'bar',
    }
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
        'committer': COMMITTER[0],
        'author':  COMMITTER[0],
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
        'committer': COMMITTER[1],
        'author':  COMMITTER[1],
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
        'author': COMMITTER[0],
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

TEST_CONFIG = {
    'brokers': ['localhost'],
    'temporary_prefix': 'swh.tmp_journal.new',
    'final_prefix': 'swh.journal.objects',
    'consumer_id': 'swh.journal.publisher',
    'publisher_id': 'swh.journal.publisher',
    'object_types': ['content'],
    'max_messages': 1,  # will read 1 message and stops
}


class JournalPublisherTest(JournalPublisher):
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

        print("publisher.origin-visits", self.origin_visits)


class JournalPublisherNoKafkaInMemoryStorage(JournalPublisherTest):
    """A journal publisher with:
    - no kafka dependency
    - mock storage as storage

    """
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


# Prepare kafka

ROOT = Path(__file__).parent
KAFKA_SCRIPTS = ROOT / 'kafka/bin/'

KAFKA_BIN = str(KAFKA_SCRIPTS / 'kafka-server-start.sh')
ZOOKEEPER_BIN = str(KAFKA_SCRIPTS / 'zookeeper-server-start.sh')


zookeeper_proc = make_zookeeper_process(ZOOKEEPER_BIN)
kafka_server = make_kafka_server(KAFKA_BIN, 'zookeeper_proc')

TOPIC = 'abc'
kafka_consumer = make_kafka_consumer(
    'kafka_server', seek_to_beginning=True, kafka_topics=[TOPIC])


def write_to_kafka(kafka_server: Tuple[Popen, int], message: bytes) -> None:
    """Write a message to kafka_server."""
    _, kafka_port = kafka_server
    producer = KafkaProducer(
        bootstrap_servers='localhost:{}'.format(kafka_port))
    producer.send(TOPIC, message)
    producer.flush()


def write_and_read(kafka_server: Tuple[Popen, int],
                   kafka_consumer: KafkaConsumer) -> None:
    """Write to kafka_server, consume with kafka_consumer."""
    message = b'msg'
    write_to_kafka(kafka_server, message)
    consumed = list(kafka_consumer)
    assert len(consumed) == 1
    assert consumed[0].topic == TOPIC
    assert consumed[0].value == message


def test_message(kafka_server: Tuple[Popen, int],
                 kafka_consumer: KafkaConsumer):
    write_and_read(kafka_server, kafka_consumer)

# def setUp(self):
#     self.publisher = JournalPublisherTest()
#     self.contents = [{b'sha1': c['sha1']} for c in CONTENTS]
#     # self.revisions = [{b'id': c['id']} for c in REVISIONS]
#     # self.releases = [{b'id': c['id']} for c in RELEASES]
#     # producer and consumer to send and read data from publisher
#     self.producer_to_publisher = KafkaProducer(
#         bootstrap_servers=TEST_CONFIG['brokers'],
#         key_serializer=key_to_kafka,
#         value_serializer=key_to_kafka,
#         acks='all')
#     self.consumer_from_publisher = KafkaConsumer(
#         bootstrap_servers=TEST_CONFIG['brokers'],
#         value_deserializer=kafka_to_key)
#     self.consumer_from_publisher.subscribe(
#         topics=['%s.%s' % (TEST_CONFIG['temporary_prefix'], object_type)
#                 for object_type in TEST_CONFIG['object_types']])


# def test_poll(kafka_consumer):
#     # given (send message to the publisher)
#     self.producer_to_publisher.send(
#         '%s.content' % TEST_CONFIG['temporary_prefix'],
#         self.contents[0]
#     )

#     nb_messages = 1

#     # when (the publisher poll 1 message and send 1 reified object)
#     self.publisher.poll(max_messages=nb_messages)

#     # then (client reads from the messages from output topic)
#     msgs = []
#     for num, msg in enumerate(self.consumer_from_publisher):
#         print('#### consumed msg %s: %s ' % (num, msg))
#         msgs.append(msg)

#     self.assertEqual(len(msgs), nb_messages)
#     print('##### msgs: %s' % msgs)
#     # check the results
#     expected_topic = 'swh.journal.objects.content'
#     expected_object = (self.contents[0][b'sha1'], CONTENTS[0])

#     self.assertEqual(msgs, (expected_topic, expected_object))
