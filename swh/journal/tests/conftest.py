# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import pytest

from kafka import KafkaConsumer, KafkaProducer
from subprocess import Popen
from typing import Tuple

from pathlib import Path
from pytest_kafka import (
    make_zookeeper_process, make_kafka_server, make_kafka_consumer
)

from swh.journal.publisher import JournalPublisher
from swh.model.hashutil import hash_to_bytes

from swh.journal.serializers import kafka_to_key, key_to_kafka


TEST_CONFIG = {
    'brokers': ['localhost'],
    'temporary_prefix': 'swh.tmp_journal.new',
    'final_prefix': 'swh.journal.objects',
    'consumer_id': 'swh.journal.publisher',
    'publisher_id': 'swh.journal.publisher',
    'object_types': ['content', 'revision', 'release', 'origin'],
    'max_messages': 1,  # will read 1 message and stops
    'storage': {'cls': 'memory', 'args': {}}
}

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


class JournalPublisherTest(JournalPublisher):
    """A journal publisher which override the default configuration
       parsing setup.

    """
    def _prepare_storage(self, config):
        super()._prepare_storage(config)
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


KAFKA_ROOT = os.environ.get('SWH_KAFKA_ROOT', Path(__file__).parent)
KAFKA_SCRIPTS = KAFKA_ROOT / 'kafka/bin/'

KAFKA_BIN = str(KAFKA_SCRIPTS / 'kafka-server-start.sh')
ZOOKEEPER_BIN = str(KAFKA_SCRIPTS / 'zookeeper-server-start.sh')


# Those defines fixtures
zookeeper_proc = make_zookeeper_process(ZOOKEEPER_BIN)
kafka_server = make_kafka_server(KAFKA_BIN, 'zookeeper_proc')


@pytest.fixture
def kafka_producer(request: 'SubRequest', kafka_server: Tuple[Popen, int]) -> KafkaProducer:  # noqa
    _, port = kafka_server
    producer = KafkaProducer(bootstrap_servers='localhost:{}'.format(port))
    return producer


@pytest.fixture
def kafka_consumer(
        request: 'SubRequest', kafka_server: Tuple[Popen, int]) -> KafkaConsumer:  # noqa

    TOPIC = 'abc'
    kafka_consumer = make_kafka_consumer(
        'kafka_server', seek_to_beginning=True, kafka_topics=[TOPIC])
    return kafka_consumer(request)


class JournalPublisherKafkaInMemoryStorage(JournalPublisherTest):
    """A journal publisher with:
    - kafka dependency
    - in-memory storage

    """
    def _prepare_journal(self, config):
        """No journal for now

        """
        self.consumer = KafkaConsumer(
            bootstrap_servers=config['brokers'],
            value_deserializer=kafka_to_key,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            group_id=config['consumer_id'],
        )
        self.producer = KafkaProducer(
            bootstrap_servers=config['brokers'],
            key_serializer=key_to_kafka,
            value_serializer=key_to_kafka,
            client_id=config['publisher_id'],
        )


@pytest.fixture
def journal_publisher(request: 'SubRequest', kafka_consumer, kafka_producer):
    return JournalPublisherKafkaInMemoryStorage(TEST_CONFIG)