# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import functools
import re
import tempfile
from subprocess import Popen
from typing import Tuple
from unittest.mock import patch

from click.testing import CliRunner
from kafka import KafkaProducer
import pytest

from swh.objstorage.backends.in_memory import InMemoryObjStorage
from swh.storage.in_memory import Storage

from swh.journal.cli import cli
from swh.journal.serializers import key_to_kafka, value_to_kafka


CLI_CONFIG = '''
storage:
    cls: memory
    args: {}
objstorage_src:
    cls: mocked
    args:
        name: src
objstorage_dst:
    cls: mocked
    args:
        name: dst
'''


@pytest.fixture
def storage():
    """An instance of swh.storage.in_memory.Storage that gets injected
    into the CLI functions."""
    storage = Storage()
    with patch('swh.journal.cli.get_storage') as get_storage_mock:
        get_storage_mock.return_value = storage
        yield storage


def invoke(catch_exceptions, args):
    runner = CliRunner()
    with tempfile.NamedTemporaryFile('a', suffix='.yml') as config_fd:
        config_fd.write(CLI_CONFIG)
        config_fd.seek(0)
        args = ['-C' + config_fd.name] + args
        result = runner.invoke(cli, args)
    if not catch_exceptions and result.exception:
        print(result.output)
        raise result.exception
    return result


def test_replay(
        storage: Storage,
        kafka_prefix: str,
        kafka_server: Tuple[Popen, int]):
    (_, port) = kafka_server
    kafka_prefix += '.swh.journal.objects'

    producer = KafkaProducer(
        bootstrap_servers='localhost:{}'.format(port),
        key_serializer=key_to_kafka,
        value_serializer=value_to_kafka,
        client_id='test-producer',
    )

    snapshot = {'id': b'foo', 'branches': {
        b'HEAD': {
            'target_type': 'revision',
            'target': b'bar',
        }
    }}
    producer.send(
        topic=kafka_prefix+'.snapshot', key=snapshot['id'], value=snapshot)

    result = invoke(False, [
        'replay',
        '--broker', 'localhost:%d' % port,
        '--group-id', 'test-cli-consumer',
        '--prefix', kafka_prefix,
        '--max-messages', '1',
    ])
    expected = r'Done.\n'
    assert result.exit_code == 0, result.output
    assert re.fullmatch(expected, result.output, re.MULTILINE), result.output

    assert storage.snapshot_get(snapshot['id']) == {
        **snapshot, 'next_branch': None}


def _patch_objstorages(names):
    objstorages = {name: InMemoryObjStorage() for name in names}

    def get_mock_objstorage(cls, args):
        assert cls == 'mocked', cls
        return objstorages[args['name']]

    def decorator(f):
        @functools.wraps(f)
        @patch('swh.journal.cli.get_objstorage')
        def newf(get_objstorage_mock, *args, **kwargs):
            get_objstorage_mock.side_effect = get_mock_objstorage
            f(*args, objstorages=objstorages, **kwargs)

        return newf

    return decorator


def _fill_objstorage_and_kafka(kafka_port, kafka_prefix, objstorages):
    producer = KafkaProducer(
        bootstrap_servers='localhost:{}'.format(kafka_port),
        key_serializer=key_to_kafka,
        value_serializer=value_to_kafka,
        client_id='test-producer',
    )

    contents = {}
    for i in range(10):
        content = b'\x00'*19 + bytes([i])
        sha1 = objstorages['src'].add(content)
        contents[sha1] = content
        producer.send(topic=kafka_prefix+'.content', key=sha1, value={
            'sha1': sha1,
            'status': 'visible',
        })

    producer.flush()

    return contents


@_patch_objstorages(['src', 'dst'])
def test_replay_content(
        objstorages,
        storage: Storage,
        kafka_prefix: str,
        kafka_server: Tuple[Popen, int]):
    (_, kafka_port) = kafka_server
    kafka_prefix += '.swh.journal.objects'

    contents = _fill_objstorage_and_kafka(
        kafka_port, kafka_prefix, objstorages)

    result = invoke(False, [
        'content-replay',
        '--broker', 'localhost:%d' % kafka_port,
        '--group-id', 'test-cli-consumer',
        '--prefix', kafka_prefix,
        '--max-messages', '10',
    ])
    expected = r'Done.\n'
    assert result.exit_code == 0, result.output
    assert re.fullmatch(expected, result.output, re.MULTILINE), result.output

    for (sha1, content) in contents.items():
        assert sha1 in objstorages['dst'], sha1
        assert objstorages['dst'].get(sha1) == content


@_patch_objstorages(['src', 'dst'])
def test_replay_content_exclude(
        objstorages,
        storage: Storage,
        kafka_prefix: str,
        kafka_server: Tuple[Popen, int]):
    (_, kafka_port) = kafka_server
    kafka_prefix += '.swh.journal.objects'

    contents = _fill_objstorage_and_kafka(
        kafka_port, kafka_prefix, objstorages)

    excluded_contents = list(contents)[0::2]  # picking half of them
    with tempfile.NamedTemporaryFile(mode='w+b') as fd:
        fd.write(b''.join(sorted(excluded_contents)))

        fd.seek(0)

        result = invoke(False, [
            'content-replay',
            '--broker', 'localhost:%d' % kafka_port,
            '--group-id', 'test-cli-consumer',
            '--prefix', kafka_prefix,
            '--max-messages', '10',
            '--exclude-sha1-file', fd.name,
        ])
    expected = r'Done.\n'
    assert result.exit_code == 0, result.output
    assert re.fullmatch(expected, result.output, re.MULTILINE), result.output

    for (sha1, content) in contents.items():
        if sha1 in excluded_contents:
            assert sha1 not in objstorages['dst'], sha1
        else:
            assert sha1 in objstorages['dst'], sha1
            assert objstorages['dst'].get(sha1) == content
