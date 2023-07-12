# Copyright (C) 2019-2023 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import math
from typing import Dict, List, cast
from unittest.mock import MagicMock

from confluent_kafka import Producer
import pytest

from swh.core.pytest_plugin import FakeSocket
from swh.journal.client import EofBehavior, JournalClient
from swh.journal.serializers import kafka_to_value, key_to_kafka, value_to_kafka
from swh.model.model import Content, Revision
from swh.model.tests.swh_model_data import TEST_OBJECTS

REV = {
    "message": b"something cool",
    "author": {"fullname": b"Peter", "name": None, "email": b"peter@ouiche.lo"},
    "committer": {"fullname": b"Stephen", "name": b"From Outer Space", "email": None},
    "date": {
        "timestamp": {"seconds": 123456789, "microseconds": 123},
        "offset": 120,
        "negative_utc": False,
    },
    "committer_date": {
        "timestamp": {"seconds": 123123456, "microseconds": 0},
        "offset": 0,
        "negative_utc": False,
    },
    "type": "git",
    "directory": (
        b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
        b"\x01\x02\x03\x04\x05"
    ),
    "synthetic": False,
    "metadata": None,
    "parents": [],
    "id": b"\x8b\xeb\xd1\x9d\x07\xe2\x1e0\xe2 \x91X\x8d\xbd\x1c\xa8\x86\xdeB\x0c",
}


@pytest.mark.parametrize("legacy_eof", [True, False])
def test_client(
    kafka_prefix: str, kafka_consumer_group: str, kafka_server: str, legacy_eof: bool
):
    producer = Producer(
        {
            "bootstrap.servers": kafka_server,
            "client.id": "test producer",
            "acks": "all",
        }
    )

    # Fill Kafka
    producer.produce(
        topic=kafka_prefix + ".revision",
        key=REV["id"],
        value=value_to_kafka(REV),
    )
    producer.flush()

    if legacy_eof:
        with pytest.deprecated_call():
            client = JournalClient(
                brokers=[kafka_server],
                group_id=kafka_consumer_group,
                prefix=kafka_prefix,
                stop_on_eof=True,
            )
    else:
        client = JournalClient(
            brokers=[kafka_server],
            group_id=kafka_consumer_group,
            prefix=kafka_prefix,
            on_eof=EofBehavior.STOP,
        )
    worker_fn = MagicMock()
    client.process(worker_fn)

    worker_fn.assert_called_once_with({"revision": [REV]})


def test_client_statsd(
    kafka_prefix: str,
    kafka_consumer_group: str,
    kafka_server: str,
):
    producer = Producer(
        {
            "bootstrap.servers": kafka_server,
            "client.id": "test producer",
            "acks": "all",
        }
    )

    # Fill Kafka
    producer.produce(
        topic=kafka_prefix + ".revision",
        key=REV["id"],
        value=value_to_kafka(REV),
    )
    producer.flush()

    client = JournalClient(
        brokers=[kafka_server],
        group_id=kafka_consumer_group,
        prefix=kafka_prefix,
        on_eof=EofBehavior.STOP,
    )
    client.statsd._socket = FakeSocket()
    worker_fn = MagicMock()
    client.process(worker_fn)

    group_tag = f"#group:{kafka_consumer_group}"

    worker_fn.assert_called_once_with({"revision": [REV]})
    assert client.statsd.namespace == "swh_journal_client"
    # check at least initialization of the status gauge is ok
    assert (
        client.statsd.socket.recv()
        == f"swh_journal_client.status:0|g|{group_tag},status:idle"
    )
    assert (
        client.statsd.socket.recv()
        == f"swh_journal_client.status:0|g|{group_tag},status:processing"
    )
    assert (
        client.statsd.socket.recv()
        == f"swh_journal_client.status:0|g|{group_tag},status:waiting"
    )
    # processing the batch with only one message
    assert (
        client.statsd.socket.recv()
        == f"swh_journal_client.status:1|g|{group_tag},status:idle"
    )
    assert (
        client.statsd.socket.recv()
        == f"swh_journal_client.status:0|g|{group_tag},status:idle"
    )
    assert (
        client.statsd.socket.recv()
        == f"swh_journal_client.status:1|g|{group_tag},status:waiting"
    )
    assert (
        client.statsd.socket.recv()
        == f"swh_journal_client.status:0|g|{group_tag},status:waiting"
    )
    assert (
        client.statsd.socket.recv()
        == f"swh_journal_client.status:1|g|{group_tag},status:processing"
    )
    assert (
        client.statsd.socket.recv()
        == f"swh_journal_client.status:0|g|{group_tag},status:processing"
    )
    assert (
        client.statsd.socket.recv()
        == f"swh_journal_client.status:1|g|{group_tag},status:idle"
    )
    # from there, status messages can be mixed with a few other ones...
    assert (
        f"swh_journal_client.handle_message_total:1|c|{group_tag}".encode()
        in client.statsd.socket.payloads
    )
    assert (
        f"swh_journal_client.stop_total:1|c|{group_tag}".encode()
        in client.statsd.socket.payloads
    )
    # and some waiting/idle forth and back may have happened, so only check the
    # last 3 messages resetting the status gauges)
    assert list(client.statsd.socket.payloads)[-3:] == [
        f"swh_journal_client.status:0|g|{group_tag},status:idle".encode(),
        f"swh_journal_client.status:0|g|{group_tag},status:processing".encode(),
        f"swh_journal_client.status:0|g|{group_tag},status:waiting".encode(),
    ]


@pytest.mark.parametrize(
    "count,legacy_eof", [(1, True), (2, True), (1, False), (2, False)]
)
def test_client_stop_after_objects(
    kafka_prefix: str,
    kafka_consumer_group: str,
    kafka_server: str,
    count: int,
    legacy_eof: bool,
):
    producer = Producer(
        {
            "bootstrap.servers": kafka_server,
            "client.id": "test producer",
            "acks": "all",
        }
    )

    # Fill Kafka
    revisions = cast(List[Revision], TEST_OBJECTS["revision"])
    for rev in revisions:
        producer.produce(
            topic=kafka_prefix + ".revision",
            key=rev.id,
            value=value_to_kafka(rev.to_dict()),
        )
    producer.flush()

    if legacy_eof:
        with pytest.deprecated_call():
            client = JournalClient(
                brokers=[kafka_server],
                group_id=kafka_consumer_group,
                prefix=kafka_prefix,
                stop_on_eof=False,
                stop_after_objects=count,
            )
    else:
        client = JournalClient(
            brokers=[kafka_server],
            group_id=kafka_consumer_group,
            prefix=kafka_prefix,
            on_eof=EofBehavior.CONTINUE,
            stop_after_objects=count,
        )

    worker_fn = MagicMock()
    client.process(worker_fn)

    # this code below is not pretty, but needed since we have to deal with
    # dicts (so no set) which can have values that are list vs tuple, and we do
    # not know for sure how many calls of the worker_fn will happen during the
    # consumption of the topic...
    worker_fn.assert_called()
    revs = []  # list of (unique) rev dicts we got from the client
    for call in worker_fn.call_args_list:
        callrevs = call[0][0]["revision"]
        for rev in callrevs:
            assert Revision.from_dict(rev) in revisions
            if rev not in revs:
                revs.append(rev)
    assert len(revs) == count


assert len(TEST_OBJECTS["revision"]) < 10, (
    'test_client_restart_and_stop_after_objects expects TEST_OBJECTS["revision"] '
    "to have less than 10 objects to test exhaustively"
)


@pytest.mark.parametrize(
    "count,string_eof", [(1, True), (2, False), (10, True), (20, False)]
)
def test_client_restart_and_stop_after_objects(
    kafka_prefix: str,
    kafka_consumer_group: str,
    kafka_server: str,
    count: int,
    string_eof: bool,
):
    producer = Producer(
        {
            "bootstrap.servers": kafka_server,
            "client.id": "test producer",
            "acks": "all",
        }
    )

    # Fill Kafka
    revisions = cast(List[Revision], TEST_OBJECTS["revision"])
    for rev in revisions:
        producer.produce(
            topic=kafka_prefix + ".revision",
            key=rev.id,
            value=value_to_kafka(rev.to_dict()),
        )
    producer.flush()

    client = JournalClient(
        brokers=[kafka_server],
        group_id=kafka_consumer_group,
        prefix=kafka_prefix,
        on_eof="restart" if string_eof else EofBehavior.RESTART,
        stop_after_objects=count,
    )

    worker_fn = MagicMock()
    client.process(worker_fn)

    # this code below is not pretty, but needed since we have to deal with
    # dicts (so no set) which can have values that are list vs tuple, and we do
    # not know for sure how many calls of the worker_fn will happen during the
    # consumption of the topic...
    worker_fn.assert_called()
    revs = []  # list of (possibly duplicated) rev dicts we got from the client
    unique_revs = []  # list of (unique) rev dicts we got from the client
    for call in worker_fn.call_args_list:
        callrevs = call[0][0]["revision"]
        for rev in callrevs:
            assert Revision.from_dict(rev) in revisions
            if rev not in revs:
                unique_revs.append(rev)
            revs.append(rev)
    assert len(revs) == count
    assert len(unique_revs) == min(len(revisions), count)

    # Each revision should be seen approximately count/len(revisions) times
    rev_ids = [r["id"].hex() for r in revs]  # type: ignore
    for rev in revisions:
        assert (
            math.floor(count / len(revisions))
            <= rev_ids.count(rev.id.hex())
            <= math.ceil(count / len(revisions))
        )

    # Check each run but the last contains all revisions
    for i in range(int(count / len(revisions))):
        assert set(rev_ids[i * len(revisions) : (i + 1) * len(revisions)]) == set(
            rev.id.hex() for rev in revisions
        ), i


@pytest.mark.parametrize("batch_size", [1, 5, 100])
def test_client_batch_size(
    kafka_prefix: str,
    kafka_consumer_group: str,
    kafka_server: str,
    batch_size: int,
):
    num_objects = 2 * batch_size + 1
    assert num_objects < 256, "Too many objects, generation will fail"

    producer = Producer(
        {
            "bootstrap.servers": kafka_server,
            "client.id": "test producer",
            "acks": "all",
        }
    )

    contents = [Content.from_data(bytes([i])) for i in range(num_objects)]

    # Fill Kafka
    for content in contents:
        producer.produce(
            topic=kafka_prefix + ".content",
            key=key_to_kafka(content.sha1),
            value=value_to_kafka(content.to_dict()),
        )

    producer.flush()

    client = JournalClient(
        brokers=[kafka_server],
        group_id=kafka_consumer_group,
        prefix=kafka_prefix,
        on_eof=EofBehavior.STOP,
        batch_size=batch_size,
    )

    collected_output: List[Dict] = []

    def worker_fn(objects):
        received = objects["content"]
        assert len(received) <= batch_size
        collected_output.extend(received)

    client.process(worker_fn)

    expected_output = [content.to_dict() for content in contents]
    assert len(collected_output) == len(expected_output)

    for output in collected_output:
        assert output in expected_output


@pytest.fixture()
def kafka_producer(kafka_prefix: str, kafka_server_base: str):
    producer = Producer(
        {
            "bootstrap.servers": kafka_server_base,
            "client.id": "test producer",
            "acks": "all",
        }
    )

    # Fill Kafka
    producer.produce(
        topic=kafka_prefix + ".something",
        key=key_to_kafka(b"key1"),
        value=value_to_kafka("value1"),
    )
    producer.produce(
        topic=kafka_prefix + ".else",
        key=key_to_kafka(b"key1"),
        value=value_to_kafka("value2"),
    )
    producer.flush()
    return producer


def test_client_subscribe_all(
    kafka_producer: Producer, kafka_prefix: str, kafka_server_base: str
):
    client = JournalClient(
        brokers=[kafka_server_base],
        group_id="whatever",
        prefix=kafka_prefix,
        on_eof=EofBehavior.STOP,
    )
    assert set(client.subscription) == {
        f"{kafka_prefix}.something",
        f"{kafka_prefix}.else",
    }

    worker_fn = MagicMock()
    client.process(worker_fn)
    worker_fn.assert_called_once_with(
        {
            "something": ["value1"],
            "else": ["value2"],
        }
    )


def test_client_subscribe_one_topic(
    kafka_producer: Producer, kafka_prefix: str, kafka_server_base: str
):
    client = JournalClient(
        brokers=[kafka_server_base],
        group_id="whatever",
        prefix=kafka_prefix,
        on_eof=EofBehavior.STOP,
        object_types=["else"],
    )
    assert client.subscription == [f"{kafka_prefix}.else"]

    worker_fn = MagicMock()
    client.process(worker_fn)
    worker_fn.assert_called_once_with({"else": ["value2"]})


def test_client_subscribe_absent_topic(
    kafka_producer: Producer, kafka_prefix: str, kafka_server_base: str
):
    with pytest.raises(ValueError):
        JournalClient(
            brokers=[kafka_server_base],
            group_id="whatever",
            prefix=kafka_prefix,
            on_eof=EofBehavior.STOP,
            object_types=["really"],
        )


def test_client_subscribe_absent_prefix(
    kafka_producer: Producer, kafka_prefix: str, kafka_server_base: str
):
    with pytest.raises(ValueError):
        JournalClient(
            brokers=[kafka_server_base],
            group_id="whatever",
            prefix="wrong.prefix",
            on_eof=EofBehavior.STOP,
        )
    with pytest.raises(ValueError):
        JournalClient(
            brokers=[kafka_server_base],
            group_id="whatever",
            prefix="wrong.prefix",
            on_eof=EofBehavior.STOP,
            object_types=["else"],
        )


def test_client_subscriptions_with_anonymized_topics(
    kafka_prefix: str, kafka_consumer_group: str, kafka_server_base: str
):
    producer = Producer(
        {
            "bootstrap.servers": kafka_server_base,
            "client.id": "test producer",
            "acks": "all",
        }
    )

    # Fill Kafka with revision object on both the regular prefix (normally for
    # anonymized objects in this case) and privileged one
    producer.produce(
        topic=kafka_prefix + ".revision",
        key=REV["id"],
        value=value_to_kafka(REV),
    )
    producer.produce(
        topic=kafka_prefix + "_privileged.revision",
        key=REV["id"],
        value=value_to_kafka(REV),
    )
    producer.flush()

    # without privileged "channels" activated on the client side
    client = JournalClient(
        brokers=[kafka_server_base],
        group_id=kafka_consumer_group,
        prefix=kafka_prefix,
        on_eof=EofBehavior.STOP,
        privileged=False,
    )
    # we only subscribed to "standard" topics
    assert client.subscription == [kafka_prefix + ".revision"]

    # with privileged "channels" activated on the client side
    client = JournalClient(
        brokers=[kafka_server_base],
        group_id=kafka_consumer_group,
        prefix=kafka_prefix,
        privileged=True,
    )
    # we only subscribed to "privileged" topics
    assert client.subscription == [kafka_prefix + "_privileged.revision"]


def test_client_subscriptions_without_anonymized_topics(
    kafka_prefix: str, kafka_consumer_group: str, kafka_server_base: str
):
    producer = Producer(
        {
            "bootstrap.servers": kafka_server_base,
            "client.id": "test producer",
            "acks": "all",
        }
    )

    # Fill Kafka with revision objects only on the standard prefix
    producer.produce(
        topic=kafka_prefix + ".revision",
        key=REV["id"],
        value=value_to_kafka(REV),
    )
    producer.flush()

    # without privileged channel activated on the client side
    client = JournalClient(
        brokers=[kafka_server_base],
        group_id=kafka_consumer_group,
        prefix=kafka_prefix,
        on_eof=EofBehavior.STOP,
        privileged=False,
    )
    # we only subscribed to the standard prefix
    assert client.subscription == [kafka_prefix + ".revision"]

    # with privileged channel activated on the client side
    client = JournalClient(
        brokers=[kafka_server_base],
        group_id=kafka_consumer_group,
        prefix=kafka_prefix,
        on_eof=EofBehavior.STOP,
        privileged=True,
    )
    # we also only subscribed to the standard prefix, since there is no priviled prefix
    # on the kafka broker
    assert client.subscription == [kafka_prefix + ".revision"]


def test_client_with_deserializer(
    kafka_prefix: str, kafka_consumer_group: str, kafka_server: str
):
    producer = Producer(
        {
            "bootstrap.servers": kafka_server,
            "client.id": "test producer",
            "acks": "all",
        }
    )

    # Fill Kafka
    revisions = cast(List[Revision], TEST_OBJECTS["revision"])
    for rev in revisions:
        producer.produce(
            topic=kafka_prefix + ".revision",
            key=rev.id,
            value=value_to_kafka(rev.to_dict()),
        )
    producer.flush()

    def custom_deserializer(object_type, msg):
        assert object_type == "revision"
        obj = kafka_to_value(msg)
        # filter the first revision
        if obj["id"] == revisions[0].id:
            return None
        return Revision.from_dict(obj)

    client = JournalClient(
        brokers=[kafka_server],
        group_id=kafka_consumer_group,
        prefix=kafka_prefix,
        on_eof=EofBehavior.STOP,
        value_deserializer=custom_deserializer,
    )
    worker_fn = MagicMock()
    client.process(worker_fn)

    # a commit seems to be needed to prevent some race condition situation
    # where the worker_fn has not yet been called at this point (not sure how)
    client.consumer.commit()

    # Check the first revision has not been passed to worker_fn
    processed_revisions = set(worker_fn.call_args[0][0]["revision"])
    assert revisions[0] not in processed_revisions
    assert all(rev in processed_revisions for rev in revisions[1:])


def test_client_create_topics(
    kafka_prefix: str, kafka_consumer_group: str, kafka_server_base: str, mocker
):

    # the Mock broker does not support the CreateTopic admin API, so we
    # mock the call to AdminClient.create_topics
    mock_admin = mocker.patch("swh.journal.client.AdminClient")
    mock_topic_future = mocker.Mock()
    mock_topic_future.result.return_value = None
    mock_admin.create_topics.return_value = {
        kafka_prefix + ".revision": mock_topic_future
    }

    client = JournalClient(
        brokers=[kafka_server_base],
        group_id=kafka_consumer_group,
        prefix=kafka_prefix,
        on_eof=EofBehavior.STOP,
        privileged=False,
        object_types=["revision"],
        create_topics=True,
    )

    assert client.subscription == [kafka_prefix + ".revision"]
