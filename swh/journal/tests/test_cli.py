# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import functools
import logging
import re
import tempfile
from subprocess import Popen
from typing import Any, Dict, Tuple
from unittest.mock import patch

from click.testing import CliRunner
from confluent_kafka import Producer
import pytest

from swh.objstorage.backends.in_memory import InMemoryObjStorage
from swh.storage import get_storage

from swh.journal.cli import cli
from swh.journal.serializers import key_to_kafka, value_to_kafka


logger = logging.getLogger(__name__)


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
    """An swh-storage object that gets injected into the CLI functions."""
    storage_config = {
        'cls': 'pipeline',
        'steps': [
            {'cls': 'validate'},
            {'cls': 'memory'},
        ]
    }
    storage = get_storage(**storage_config)
    with patch('swh.journal.cli.get_storage') as get_storage_mock:
        get_storage_mock.return_value = storage
        yield storage


def invoke(catch_exceptions, args, env=None):
    runner = CliRunner()
    with tempfile.NamedTemporaryFile('a', suffix='.yml') as config_fd:
        config_fd.write(CLI_CONFIG)
        config_fd.seek(0)
        args = ['-C' + config_fd.name] + args
        result = runner.invoke(
            cli, args, obj={'log_level': logging.DEBUG}, env=env,
        )
    if not catch_exceptions and result.exception:
        print(result.output)
        raise result.exception
    return result


def test_replay(
        storage,
        kafka_prefix: str,
        kafka_server: Tuple[Popen, int]):
    (_, port) = kafka_server
    kafka_prefix += '.swh.journal.objects'

    producer = Producer({
        'bootstrap.servers': 'localhost:{}'.format(port),
        'client.id': 'test-producer',
        'enable.idempotence': 'true',
    })

    snapshot = {'id': b'foo', 'branches': {
        b'HEAD': {
            'target_type': 'revision',
            'target': b'\x01'*20,
        }
    }}  # type: Dict[str, Any]
    producer.produce(
        topic=kafka_prefix+'.snapshot',
        key=key_to_kafka(snapshot['id']),
        value=value_to_kafka(snapshot),
    )
    producer.flush()

    logger.debug('Flushed producer')

    result = invoke(False, [
        'replay',
        '--broker', '127.0.0.1:%d' % port,
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


NUM_CONTENTS = 10


def _fill_objstorage_and_kafka(kafka_port, kafka_prefix, objstorages):
    producer = Producer({
        'bootstrap.servers': '127.0.0.1:{}'.format(kafka_port),
        'client.id': 'test-producer',
        'enable.idempotence': 'true',
    })

    contents = {}
    for i in range(NUM_CONTENTS):
        content = b'\x00'*19 + bytes([i])
        sha1 = objstorages['src'].add(content)
        contents[sha1] = content
        producer.produce(
            topic=kafka_prefix+'.content',
            key=key_to_kafka(sha1),
            value=key_to_kafka({
                'sha1': sha1,
                'status': 'visible',
            }),
        )

    producer.flush()

    return contents


@_patch_objstorages(['src', 'dst'])
def test_replay_content(
        objstorages,
        storage,
        kafka_prefix: str,
        kafka_server: Tuple[Popen, int]):
    (_, kafka_port) = kafka_server
    kafka_prefix += '.swh.journal.objects'

    contents = _fill_objstorage_and_kafka(
        kafka_port, kafka_prefix, objstorages)

    result = invoke(False, [
        'content-replay',
        '--broker', '127.0.0.1:%d' % kafka_port,
        '--group-id', 'test-cli-consumer',
        '--prefix', kafka_prefix,
        '--max-messages', str(NUM_CONTENTS),
    ])
    expected = r'Done.\n'
    assert result.exit_code == 0, result.output
    assert re.fullmatch(expected, result.output, re.MULTILINE), result.output

    for (sha1, content) in contents.items():
        assert sha1 in objstorages['dst'], sha1
        assert objstorages['dst'].get(sha1) == content


@_patch_objstorages(['src', 'dst'])
def test_replay_content_static_group_id(
        objstorages,
        storage,
        kafka_prefix: str,
        kafka_server: Tuple[Popen, int],
        caplog):
    (_, kafka_port) = kafka_server
    kafka_prefix += '.swh.journal.objects'

    contents = _fill_objstorage_and_kafka(
        kafka_port, kafka_prefix, objstorages)

    # Setup log capture to fish the consumer settings out of the log messages
    caplog.set_level(logging.DEBUG, 'swh.journal.client')

    result = invoke(False, [
        'content-replay',
        '--broker', '127.0.0.1:%d' % kafka_port,
        '--group-id', 'test-cli-consumer',
        '--prefix', kafka_prefix,
        '--max-messages', str(NUM_CONTENTS),
    ], {'KAFKA_GROUP_INSTANCE_ID': 'static-group-instance-id'})
    expected = r'Done.\n'
    assert result.exit_code == 0, result.output
    assert re.fullmatch(expected, result.output, re.MULTILINE), result.output

    consumer_settings = None
    for record in caplog.records:
        if 'Consumer settings' in record.message:
            consumer_settings = record.args
            break

    assert consumer_settings is not None, (
        'Failed to get consumer settings out of the consumer log. '
        'See log capture for details.'
    )
    assert consumer_settings['group.instance.id'] == 'static-group-instance-id'
    assert consumer_settings['session.timeout.ms'] == 60 * 10 * 1000
    assert consumer_settings['max.poll.interval.ms'] == 90 * 10 * 1000

    for (sha1, content) in contents.items():
        assert sha1 in objstorages['dst'], sha1
        assert objstorages['dst'].get(sha1) == content


@_patch_objstorages(['src', 'dst'])
def test_replay_content_exclude(
        objstorages,
        storage,
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
            '--broker', '127.0.0.1:%d' % kafka_port,
            '--group-id', 'test-cli-consumer',
            '--prefix', kafka_prefix,
            '--max-messages', str(NUM_CONTENTS),
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


NUM_CONTENTS_DST = 5


@_patch_objstorages(['src', 'dst'])
@pytest.mark.parametrize("check_dst,expected_copied,expected_in_dst", [
    (True, NUM_CONTENTS - NUM_CONTENTS_DST, NUM_CONTENTS_DST),
    (False, NUM_CONTENTS, 0),
])
def test_replay_content_check_dst(
        objstorages,
        storage,
        kafka_prefix: str,
        kafka_server: Tuple[Popen, int],
        check_dst: bool,
        expected_copied: int,
        expected_in_dst: int,
        caplog):
    (_, kafka_port) = kafka_server
    kafka_prefix += '.swh.journal.objects'

    contents = _fill_objstorage_and_kafka(
        kafka_port, kafka_prefix, objstorages)

    for i, (sha1, content) in enumerate(contents.items()):
        if i >= NUM_CONTENTS_DST:
            break

        objstorages['dst'].add(content, obj_id=sha1)

    caplog.set_level(logging.DEBUG, 'swh.journal.replay')

    result = invoke(False, [
        'content-replay',
        '--broker', '127.0.0.1:%d' % kafka_port,
        '--group-id', 'test-cli-consumer',
        '--prefix', kafka_prefix,
        '--max-messages', str(NUM_CONTENTS),
        '--check-dst' if check_dst else '--no-check-dst',
    ])
    expected = r'Done.\n'
    assert result.exit_code == 0, result.output
    assert re.fullmatch(expected, result.output, re.MULTILINE), result.output

    copied = 0
    in_dst = 0
    for record in caplog.records:
        logtext = record.getMessage()
        if 'copied' in logtext:
            copied += 1
        elif 'in dst' in logtext:
            in_dst += 1

    assert (copied == expected_copied and in_dst == expected_in_dst), (
        "Unexpected amount of objects copied, see the captured log for details"
    )

    for (sha1, content) in contents.items():
        assert sha1 in objstorages['dst'], sha1
        assert objstorages['dst'].get(sha1) == content
