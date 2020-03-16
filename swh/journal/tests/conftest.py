# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import pytest
import logging
import random
import string

from confluent_kafka import Consumer
from subprocess import Popen
from typing import Any, Dict, List, Optional, Tuple

from pathlib import Path
from pytest_kafka import (
    make_zookeeper_process, make_kafka_server, ZOOKEEPER_CONFIG_TEMPLATE,
)

from swh.model.hashutil import hash_to_bytes


logger = logging.getLogger(__name__)

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

duplicate_content1 = {
    'length': 4,
    'sha1': hash_to_bytes(
        '44973274ccef6ab4dfaaf86599792fa9c3fe4689'),
    'sha1_git': b'another-foo',
    'blake2s256': b'another-bar',
    'sha256': b'another-baz',
    'status': 'visible',
}

# Craft a sha1 collision
duplicate_content2 = duplicate_content1.copy()
sha1_array = bytearray(duplicate_content1['sha1_git'])
sha1_array[0] += 1
duplicate_content2['sha1_git'] = bytes(sha1_array)


DUPLICATE_CONTENTS = [duplicate_content1, duplicate_content2]


COMMITTERS = [
    {
        'fullname': b'foo',
        'name': b'foo',
        'email': b'',
    },
    {
        'fullname': b'bar',
        'name': b'bar',
        'email': b'',
    }
]

DATES = [
    {
        'timestamp': {
            'seconds': 1234567891,
            'microseconds': 0,
        },
        'offset': 120,
        'negative_utc': None,
    },
    {
        'timestamp': {
            'seconds': 1234567892,
            'microseconds': 0,
        },
        'offset': 120,
        'negative_utc': None,
    }
]

REVISIONS = [
    {
        'id': hash_to_bytes('7026b7c1a2af56521e951c01ed20f255fa054238'),
        'message': b'hello',
        'date': DATES[0],
        'committer': COMMITTERS[0],
        'author':  COMMITTERS[0],
        'committer_date': DATES[0],
        'type': 'git',
        'directory': '\x01'*20,
        'synthetic': False,
        'metadata': None,
        'parents': [],
    },
    {
        'id': hash_to_bytes('368a48fe15b7db2383775f97c6b247011b3f14f4'),
        'message': b'hello again',
        'date': DATES[1],
        'committer': COMMITTERS[1],
        'author':  COMMITTERS[1],
        'committer_date': DATES[1],
        'type': 'hg',
        'directory': '\x02'*20,
        'synthetic': False,
        'metadata': None,
        'parents': [],
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
        'author': COMMITTERS[0],
        'target_type': 'revision',
        'target': b'\x04'*20,
        'message': b'foo',
        'synthetic': False,
    },
]

ORIGINS = [
    {
        'url': 'https://somewhere.org/den/fox',
    },
    {
        'url': 'https://overtherainbow.org/fox/den',
    }
]

ORIGIN_VISITS = [
    {
        'origin': ORIGINS[0]['url'],
        'date': '2013-05-07 04:20:39.369271+00:00',
        'snapshot': None,  # TODO
        'status': 'ongoing',  # TODO
        'metadata': {'foo': 'bar'},
        'type': 'git',
    },
    {
        'origin': ORIGINS[0]['url'],
        'date': '2018-11-27 17:20:39+00:00',
        'snapshot': None,  # TODO
        'status': 'ongoing',  # TODO
        'metadata': {'baz': 'qux'},
        'type': 'git',
    }
]

# From type to tuple (id, <objects instances to test>)
OBJECT_TYPE_KEYS = {
    'content': ('sha1', CONTENTS),
    'revision': ('id', REVISIONS),
    'release': ('id', RELEASES),
    'origin': (None, ORIGINS),
    'origin_visit': (None, ORIGIN_VISITS),
}  # type: Dict[str, Tuple[Optional[str], List[Dict[str, Any]]]]


KAFKA_ROOT = os.environ.get('SWH_KAFKA_ROOT')
KAFKA_ROOT = KAFKA_ROOT if KAFKA_ROOT else os.path.dirname(__file__) + '/kafka'
if not os.path.exists(KAFKA_ROOT):
    msg = ('Development error: %s must exist and target an '
           'existing kafka installation' % KAFKA_ROOT)
    raise ValueError(msg)

KAFKA_SCRIPTS = Path(KAFKA_ROOT) / 'bin'

KAFKA_BIN = str(KAFKA_SCRIPTS / 'kafka-server-start.sh')
ZOOKEEPER_BIN = str(KAFKA_SCRIPTS / 'zookeeper-server-start.sh')

ZK_CONFIG_TEMPLATE = ZOOKEEPER_CONFIG_TEMPLATE + '\nadmin.enableServer=false\n'

# Those defines fixtures
zookeeper_proc = make_zookeeper_process(ZOOKEEPER_BIN,
                                        zk_config_template=ZK_CONFIG_TEMPLATE,
                                        scope='session')
os.environ['KAFKA_LOG4J_OPTS'] = \
    '-Dlog4j.configuration=file:%s/log4j.properties' % \
    os.path.dirname(__file__)
kafka_server = make_kafka_server(KAFKA_BIN, 'zookeeper_proc', scope='session')

kafka_logger = logging.getLogger('kafka')
kafka_logger.setLevel(logging.WARN)


@pytest.fixture(scope='function')
def kafka_prefix():
    """Pick a random prefix for kafka topics on each call"""
    return ''.join(random.choice(string.ascii_lowercase) for _ in range(10))


@pytest.fixture(scope='function')
def kafka_consumer_group(kafka_prefix: str):
    """Pick a random consumer group for kafka consumers on each call"""
    return "test-consumer-%s" % kafka_prefix


TEST_CONFIG = {
    'consumer_id': 'swh.journal.consumer',
    'object_types': OBJECT_TYPE_KEYS.keys(),
    'stop_after_objects': 1,  # will read 1 object and stop
    'storage': {'cls': 'memory', 'args': {}},
}


@pytest.fixture
def test_config(kafka_server: Tuple[Popen, int],
                kafka_prefix: str):
    """Test configuration needed for producer/consumer

    """
    _, port = kafka_server
    return {
        **TEST_CONFIG,
        'brokers': ['127.0.0.1:{}'.format(port)],
        'prefix': kafka_prefix + '.swh.journal.objects',
    }


@pytest.fixture
def consumer(
    kafka_server: Tuple[Popen, int],
    test_config: Dict,
    kafka_consumer_group: str,
) -> Consumer:
    """Get a connected Kafka consumer.

    """
    _, kafka_port = kafka_server
    consumer = Consumer({
        'bootstrap.servers': '127.0.0.1:{}'.format(kafka_port),
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
        'group.id': kafka_consumer_group,
    })

    kafka_topics = [
        '%s.%s' % (test_config['prefix'], object_type)
        for object_type in test_config['object_types']
    ]

    consumer.subscribe(kafka_topics)

    yield consumer

    consumer.close()
