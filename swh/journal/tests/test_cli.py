# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import re
import tempfile
from subprocess import Popen
from typing import Tuple
from unittest.mock import patch

from click.testing import CliRunner
from kafka import KafkaProducer
import pytest

from swh.storage.in_memory import Storage

from swh.journal.cli import cli
from swh.journal.serializers import key_to_kafka, value_to_kafka


CLI_CONFIG = '''
storage:
    cls: memory
    args: {}
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


# TODO: write a test for the content-replay command
