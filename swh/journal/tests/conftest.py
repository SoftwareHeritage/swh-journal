# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import pytest
import logging

from kafka import KafkaProducer, KafkaConsumer
from subprocess import Popen
from typing import Tuple, Dict

from pathlib import Path
from pytest_kafka import (
    make_zookeeper_process, make_kafka_server, constants
)

from swh.journal.publisher import JournalPublisher
from swh.model.hashutil import hash_to_bytes

from swh.journal.serializers import kafka_to_key, key_to_kafka, kafka_to_value


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

# From type to tuple (id, <objects instances to test>)
OBJECT_TYPE_KEYS = {
    'content': (b'sha1', CONTENTS),
    'revision': (b'id', REVISIONS),
    'release': (b'id', RELEASES),
}


TEST_CONFIG = {
    'temporary_prefix': 'swh.tmp_journal.new',
    'final_prefix': 'swh.journal.objects',
    'consumer_id': 'swh.journal.publisher',
    'publisher_id': 'swh.journal.publisher',
    'object_types': OBJECT_TYPE_KEYS.keys(),
    'max_messages': 1,  # will read 1 message and stops
    'storage': {'cls': 'memory', 'args': {}}
}


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


KAFKA_ROOT = os.environ.get('SWH_KAFKA_ROOT')
KAFKA_ROOT = KAFKA_ROOT if KAFKA_ROOT else os.path.dirname(__file__) + '/kafka'
if not os.path.exists(KAFKA_ROOT):
    msg = ('Development error: %s must exist and target an '
           'existing kafka installation' % KAFKA_ROOT)
    raise ValueError(msg)

KAFKA_SCRIPTS = Path(KAFKA_ROOT) / 'bin'

KAFKA_BIN = str(KAFKA_SCRIPTS / 'kafka-server-start.sh')
ZOOKEEPER_BIN = str(KAFKA_SCRIPTS / 'zookeeper-server-start.sh')


# Those defines fixtures
zookeeper_proc = make_zookeeper_process(ZOOKEEPER_BIN)
kafka_server = make_kafka_server(KAFKA_BIN, 'zookeeper_proc')

logger = logging.getLogger('kafka')
logger.setLevel(logging.WARN)


@pytest.fixture
def test_config():
    """Test configuration needed for publisher/producer/consumer

    """
    return TEST_CONFIG


@pytest.fixture
def producer_to_publisher(
        kafka_server: Tuple[Popen, int],
        test_config: Dict,
) -> KafkaProducer:  # noqa
    """Producer to send message to the publisher's consumer.

    """
    _, port = kafka_server
    producer = KafkaProducer(
        bootstrap_servers='localhost:{}'.format(port),
        key_serializer=key_to_kafka,
        value_serializer=key_to_kafka,
        client_id=test_config['consumer_id'],
    )
    return producer


@pytest.fixture
def consumer_from_publisher(kafka_server: Tuple[Popen, int],
                            test_config: Dict) -> KafkaConsumer:
    """Get a connected Kafka consumer.

    """
    kafka_topics = ['%s.%s' % (test_config['final_prefix'], object_type)
                    for object_type in test_config['object_types']]
    _, kafka_port = kafka_server
    consumer = KafkaConsumer(
        *kafka_topics,
        bootstrap_servers='localhost:{}'.format(kafka_port),
        consumer_timeout_ms=constants.DEFAULT_CONSUMER_TIMEOUT_MS,
        key_deserializer=kafka_to_key,
        value_deserializer=kafka_to_value,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id="test-consumer"
    )

    # Enforce auto_offset_reset=earliest even if the consumer was created
    # too soon wrt the server.
    while len(consumer.assignment()) == 0:
        consumer.poll(timeout_ms=20)
    consumer.seek_to_beginning()

    return consumer


@pytest.fixture
def publisher(kafka_server: Tuple[Popen, int],
              test_config: Dict) -> JournalPublisher:
    """Test Publisher factory. We cannot use a fixture here as we need to
       modify the sample.

    """
    # consumer and producer of the publisher needs to discuss with the
    # right instance
    _, port = kafka_server
    test_config['brokers'] = ['localhost:{}'.format(port)]
    publisher = JournalPublisherTest(test_config)
    return publisher
