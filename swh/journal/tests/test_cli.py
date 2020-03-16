# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import Counter
import copy
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
import yaml

from swh.model.hashutil import hash_to_hex
from swh.objstorage.backends.in_memory import InMemoryObjStorage
from swh.storage import get_storage

from swh.journal.cli import cli
from swh.journal.replay import CONTENT_REPLAY_RETRIES
from swh.journal.serializers import key_to_kafka, value_to_kafka


logger = logging.getLogger(__name__)


CLI_CONFIG = {
    'storage': {
        'cls': 'memory',
    },
    'objstorage_src': {
        'cls': 'mocked',
        'name': 'src',
    },
    'objstorage_dst': {
        'cls': 'mocked',
        'name': 'dst',
    },
}


@pytest.fixture
def storage():
    """An swh-storage object that gets injected into the CLI functions."""
    storage_config = {
        'cls': 'pipeline',
        'steps': [
            {'cls': 'memory'},
        ]
    }
    storage = get_storage(**storage_config)
    with patch('swh.journal.cli.get_storage') as get_storage_mock:
        get_storage_mock.return_value = storage
        yield storage


@pytest.fixture
def monkeypatch_retry_sleep(monkeypatch):
    from swh.journal.replay import copy_object, obj_in_objstorage
    monkeypatch.setattr(copy_object.retry, 'sleep', lambda x: None)
    monkeypatch.setattr(obj_in_objstorage.retry, 'sleep', lambda x: None)


def invoke(*args, env=None, journal_config=None):
    config = copy.deepcopy(CLI_CONFIG)
    if journal_config:
        config['journal'] = journal_config

    runner = CliRunner()
    with tempfile.NamedTemporaryFile('a', suffix='.yml') as config_fd:
        yaml.dump(config, config_fd)
        config_fd.seek(0)
        args = ['-C' + config_fd.name] + list(args)
        return runner.invoke(
            cli, args, obj={'log_level': logging.DEBUG}, env=env,
        )


def test_replay(
        storage,
        kafka_prefix: str,
        kafka_consumer_group: str,
        kafka_server: Tuple[Popen, int]):
    (_, kafka_port) = kafka_server
    kafka_prefix += '.swh.journal.objects'

    producer = Producer({
        'bootstrap.servers': 'localhost:{}'.format(kafka_port),
        'client.id': 'test-producer',
        'acks': 'all',
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

    result = invoke(
        'replay',
        '--stop-after-objects', '1',
        journal_config={
            'brokers': ['127.0.0.1:%d' % kafka_port],
            'group_id': kafka_consumer_group,
            'prefix': kafka_prefix,
        },
    )

    expected = r'Done.\n'
    assert result.exit_code == 0, result.output
    assert re.fullmatch(expected, result.output, re.MULTILINE), result.output

    assert storage.snapshot_get(snapshot['id']) == {
        **snapshot, 'next_branch': None}


def _patch_objstorages(names):
    objstorages = {name: InMemoryObjStorage() for name in names}

    def get_mock_objstorage(cls, **args):
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
        'acks': 'all',
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
        kafka_consumer_group: str,
        kafka_server: Tuple[Popen, int]):
    (_, kafka_port) = kafka_server
    kafka_prefix += '.swh.journal.objects'

    contents = _fill_objstorage_and_kafka(
        kafka_port, kafka_prefix, objstorages)

    result = invoke(
        'content-replay',
        '--stop-after-objects', str(NUM_CONTENTS),
        journal_config={
            'brokers': ['127.0.0.1:%d' % kafka_port],
            'group_id': kafka_consumer_group,
            'prefix': kafka_prefix,
        },
    )

    expected = r'Done.\n'
    assert result.exit_code == 0, result.output
    assert re.fullmatch(expected, result.output, re.MULTILINE), result.output

    for (sha1, content) in contents.items():
        assert sha1 in objstorages['dst'], sha1
        assert objstorages['dst'].get(sha1) == content


@_patch_objstorages(['src', 'dst'])
def test_replay_content_structured_log(
        objstorages,
        storage,
        kafka_prefix: str,
        kafka_consumer_group: str,
        kafka_server: Tuple[Popen, int],
        caplog):
    (_, kafka_port) = kafka_server
    kafka_prefix += '.swh.journal.objects'

    contents = _fill_objstorage_and_kafka(
        kafka_port, kafka_prefix, objstorages)

    caplog.set_level(logging.DEBUG, 'swh.journal.replay')

    expected_obj_ids = set(hash_to_hex(sha1) for sha1 in contents)

    result = invoke(
        'content-replay',
        '--stop-after-objects', str(NUM_CONTENTS),
        journal_config={
            'brokers': ['127.0.0.1:%d' % kafka_port],
            'group_id': kafka_consumer_group,
            'prefix': kafka_prefix,
        },
    )
    expected = r'Done.\n'
    assert result.exit_code == 0, result.output
    assert re.fullmatch(expected, result.output, re.MULTILINE), result.output

    copied = set()
    for record in caplog.records:
        logtext = record.getMessage()
        if 'copied' in logtext:
            copied.add(record.args['obj_id'])

    assert copied == expected_obj_ids, (
        "Mismatched logging; see captured log output for details."
    )


@_patch_objstorages(['src', 'dst'])
def test_replay_content_static_group_id(
        objstorages,
        storage,
        kafka_prefix: str,
        kafka_consumer_group: str,
        kafka_server: Tuple[Popen, int],
        caplog):
    (_, kafka_port) = kafka_server
    kafka_prefix += '.swh.journal.objects'

    contents = _fill_objstorage_and_kafka(
        kafka_port, kafka_prefix, objstorages)

    # Setup log capture to fish the consumer settings out of the log messages
    caplog.set_level(logging.DEBUG, 'swh.journal.client')

    result = invoke(
        'content-replay',
        '--stop-after-objects', str(NUM_CONTENTS),
        env={'KAFKA_GROUP_INSTANCE_ID': 'static-group-instance-id'},
        journal_config={
            'brokers': ['127.0.0.1:%d' % kafka_port],
            'group_id': kafka_consumer_group,
            'prefix': kafka_prefix,
        },
    )
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
        kafka_consumer_group: str,
        kafka_server: Tuple[Popen, int]):
    (_, kafka_port) = kafka_server
    kafka_prefix += '.swh.journal.objects'

    contents = _fill_objstorage_and_kafka(
        kafka_port, kafka_prefix, objstorages)

    excluded_contents = list(contents)[0::2]  # picking half of them
    with tempfile.NamedTemporaryFile(mode='w+b') as fd:
        fd.write(b''.join(sorted(excluded_contents)))

        fd.seek(0)

        result = invoke(
            'content-replay',
            '--stop-after-objects', str(NUM_CONTENTS),
            '--exclude-sha1-file', fd.name,
            journal_config={
                'brokers': ['127.0.0.1:%d' % kafka_port],
                'group_id': kafka_consumer_group,
                'prefix': kafka_prefix,
            },
        )
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
        kafka_consumer_group: str,
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

    result = invoke(
        'content-replay',
        '--stop-after-objects', str(NUM_CONTENTS),
        '--check-dst' if check_dst else '--no-check-dst',
        journal_config={
            'brokers': ['127.0.0.1:%d' % kafka_port],
            'group_id': kafka_consumer_group,
            'prefix': kafka_prefix,
        },
    )
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


class FlakyObjStorage(InMemoryObjStorage):
    def __init__(self, *args, **kwargs):
        state = kwargs.pop('state')
        self.failures_left = Counter(kwargs.pop('failures'))
        super().__init__(*args, **kwargs)
        if state:
            self.state = state

    def flaky_operation(self, op, obj_id):
        if self.failures_left[op, obj_id] > 0:
            self.failures_left[op, obj_id] -= 1
            raise RuntimeError(
                'Failed %s on %s' % (op, hash_to_hex(obj_id))
            )

    def get(self, obj_id):
        self.flaky_operation('get', obj_id)
        return super().get(obj_id)

    def add(self, data, obj_id=None, check_presence=True):
        self.flaky_operation('add', obj_id)
        return super().add(data, obj_id=obj_id,
                           check_presence=check_presence)

    def __contains__(self, obj_id):
        self.flaky_operation('in', obj_id)
        return super().__contains__(obj_id)


@_patch_objstorages(['src', 'dst'])
def test_replay_content_check_dst_retry(
        objstorages,
        storage,
        kafka_prefix: str,
        kafka_consumer_group: str,
        kafka_server: Tuple[Popen, int],
        monkeypatch_retry_sleep):
    (_, kafka_port) = kafka_server
    kafka_prefix += '.swh.journal.objects'

    contents = _fill_objstorage_and_kafka(
        kafka_port, kafka_prefix, objstorages)

    failures = {}
    for i, (sha1, content) in enumerate(contents.items()):
        if i >= NUM_CONTENTS_DST:
            break

        objstorages['dst'].add(content, obj_id=sha1)
        failures['in', sha1] = 1

    orig_dst = objstorages['dst']
    objstorages['dst'] = FlakyObjStorage(state=orig_dst.state,
                                         failures=failures)

    result = invoke(
        'content-replay',
        '--stop-after-objects', str(NUM_CONTENTS),
        '--check-dst',
        journal_config={
            'brokers': ['127.0.0.1:%d' % kafka_port],
            'group_id': kafka_consumer_group,
            'prefix': kafka_prefix,
        },
    )
    expected = r'Done.\n'
    assert result.exit_code == 0, result.output
    assert re.fullmatch(expected, result.output, re.MULTILINE), result.output

    for (sha1, content) in contents.items():
        assert sha1 in objstorages['dst'], sha1
        assert objstorages['dst'].get(sha1) == content


@_patch_objstorages(['src', 'dst'])
def test_replay_content_failed_copy_retry(
        objstorages,
        storage,
        kafka_prefix: str,
        kafka_consumer_group: str,
        kafka_server: Tuple[Popen, int],
        caplog,
        monkeypatch_retry_sleep):
    (_, kafka_port) = kafka_server
    kafka_prefix += '.swh.journal.objects'

    contents = _fill_objstorage_and_kafka(
        kafka_port, kafka_prefix, objstorages)

    add_failures = {}
    get_failures = {}
    definitely_failed = set()

    # We want to generate operations failing 1 to CONTENT_REPLAY_RETRIES times.
    # We generate failures for 2 different operations, get and add.
    num_retry_contents = 2 * CONTENT_REPLAY_RETRIES

    assert num_retry_contents < NUM_CONTENTS, (
        "Need to generate more test contents to properly test retry behavior"
    )

    for i, sha1 in enumerate(contents):
        if i >= num_retry_contents:
            break

        # This generates a number of failures, up to CONTENT_REPLAY_RETRIES
        num_failures = (i % CONTENT_REPLAY_RETRIES) + 1

        # This generates failures of add for the first CONTENT_REPLAY_RETRIES
        # objects, then failures of get.
        if i < CONTENT_REPLAY_RETRIES:
            add_failures['add', sha1] = num_failures
        else:
            get_failures['get', sha1] = num_failures

        # Only contents that have CONTENT_REPLAY_RETRIES or more are
        # definitely failing
        if num_failures >= CONTENT_REPLAY_RETRIES:
            definitely_failed.add(hash_to_hex(sha1))

    objstorages['dst'] = FlakyObjStorage(
        state=objstorages['dst'].state,
        failures=add_failures,
    )
    objstorages['src'] = FlakyObjStorage(
        state=objstorages['src'].state,
        failures=get_failures,
    )

    caplog.set_level(logging.DEBUG, 'swh.journal.replay')

    result = invoke(
        'content-replay',
        '--stop-after-objects', str(NUM_CONTENTS),
        journal_config={
            'brokers': ['127.0.0.1:%d' % kafka_port],
            'group_id': kafka_consumer_group,
            'prefix': kafka_prefix,
        },
    )
    expected = r'Done.\n'
    assert result.exit_code == 0, result.output
    assert re.fullmatch(expected, result.output, re.MULTILINE), result.output

    copied = 0
    actually_failed = set()
    for record in caplog.records:
        logtext = record.getMessage()
        if 'copied' in logtext:
            copied += 1
        elif 'Failed operation' in logtext:
            assert record.levelno == logging.ERROR
            assert record.args['retries'] == CONTENT_REPLAY_RETRIES
            actually_failed.add(record.args['obj_id'])

    assert actually_failed == definitely_failed, (
        'Unexpected object copy failures; see captured log for details'
    )

    for (sha1, content) in contents.items():
        if hash_to_hex(sha1) in definitely_failed:
            assert sha1 not in objstorages['dst']
            continue

        assert sha1 in objstorages['dst'], sha1
        assert objstorages['dst'].get(sha1) == content


@_patch_objstorages(['src', 'dst'])
def test_replay_content_objnotfound(
        objstorages,
        storage,
        kafka_prefix: str,
        kafka_consumer_group: str,
        kafka_server: Tuple[Popen, int],
        caplog):
    (_, kafka_port) = kafka_server
    kafka_prefix += '.swh.journal.objects'

    contents = _fill_objstorage_and_kafka(
        kafka_port, kafka_prefix, objstorages)

    num_contents_deleted = 5
    contents_deleted = set()

    for i, sha1 in enumerate(contents):
        if i >= num_contents_deleted:
            break

        del objstorages['src'].state[sha1]
        contents_deleted.add(hash_to_hex(sha1))

    caplog.set_level(logging.DEBUG, 'swh.journal.replay')

    result = invoke(
        'content-replay',
        '--stop-after-objects', str(NUM_CONTENTS),
        journal_config={
            'brokers': ['127.0.0.1:%d' % kafka_port],
            'group_id': kafka_consumer_group,
            'prefix': kafka_prefix,
        },
    )
    expected = r'Done.\n'
    assert result.exit_code == 0, result.output
    assert re.fullmatch(expected, result.output, re.MULTILINE), result.output

    copied = 0
    not_in_src = set()
    for record in caplog.records:
        logtext = record.getMessage()
        if 'copied' in logtext:
            copied += 1
        elif 'object not found' in logtext:
            # Check that the object id can be recovered from logs
            assert record.levelno == logging.ERROR
            not_in_src.add(record.args['obj_id'])

    assert copied == NUM_CONTENTS - num_contents_deleted, (
        "Unexpected number of contents copied"
    )

    assert not_in_src == contents_deleted, (
        "Mismatch between deleted contents and not_in_src logs"
    )

    for (sha1, content) in contents.items():
        if sha1 not in objstorages['src']:
            continue
        assert sha1 in objstorages['dst'], sha1
        assert objstorages['dst'].get(sha1) == content
