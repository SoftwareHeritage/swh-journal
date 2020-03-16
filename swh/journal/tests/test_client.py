# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from subprocess import Popen
from typing import Dict, List, Tuple
from unittest.mock import MagicMock

from confluent_kafka import Producer
import pytest

from swh.model.hypothesis_strategies import revisions
from swh.model.model import Content

from swh.journal.client import JournalClient
from swh.journal.serializers import key_to_kafka, value_to_kafka


def test_client(
        kafka_prefix: str,
        kafka_consumer_group: str,
        kafka_server: Tuple[Popen, int]):
    (_, port) = kafka_server
    kafka_prefix += '.swh.journal.objects'

    producer = Producer({
        'bootstrap.servers': 'localhost:{}'.format(port),
        'client.id': 'test producer',
        'acks': 'all',
    })

    rev = revisions().example()

    # Fill Kafka
    producer.produce(
        topic=kafka_prefix + '.revision', key=key_to_kafka(rev.id),
        value=value_to_kafka(rev.to_dict()),
    )
    producer.flush()

    client = JournalClient(
        brokers='localhost:%d' % kafka_server[1],
        group_id=kafka_consumer_group,
        prefix=kafka_prefix,
        stop_after_objects=1,
    )
    worker_fn = MagicMock()
    client.process(worker_fn)

    worker_fn.assert_called_once_with({'revision': [rev.to_dict()]})


def test_client_eof(
        kafka_prefix: str,
        kafka_consumer_group: str,
        kafka_server: Tuple[Popen, int]):
    (_, port) = kafka_server
    kafka_prefix += '.swh.journal.objects'

    producer = Producer({
        'bootstrap.servers': 'localhost:{}'.format(port),
        'client.id': 'test producer',
        'acks': 'all',
    })

    rev = revisions().example()

    # Fill Kafka
    producer.produce(
        topic=kafka_prefix + '.revision', key=key_to_kafka(rev.id),
        value=value_to_kafka(rev.to_dict()),
    )
    producer.flush()

    client = JournalClient(
        brokers='localhost:%d' % kafka_server[1],
        group_id=kafka_consumer_group,
        prefix=kafka_prefix,
        stop_after_objects=None,
        stop_on_eof=True,
    )

    worker_fn = MagicMock()
    client.process(worker_fn)

    worker_fn.assert_called_once_with({'revision': [rev.to_dict()]})


@pytest.mark.parametrize("batch_size", [1, 5, 100])
def test_client_batch_size(
        kafka_prefix: str,
        kafka_consumer_group: str,
        kafka_server: Tuple[Popen, int],
        batch_size: int,
):
    (_, port) = kafka_server
    kafka_prefix += '.swh.journal.objects'

    num_objects = 2 * batch_size + 1
    assert num_objects < 256, "Too many objects, generation will fail"

    producer = Producer({
        'bootstrap.servers': 'localhost:{}'.format(port),
        'client.id': 'test producer',
        'acks': 'all',
    })

    contents = [Content.from_data(bytes([i])) for i in range(num_objects)]

    # Fill Kafka
    for content in contents:
        producer.produce(
            topic=kafka_prefix + '.content',
            key=key_to_kafka(content.sha1),
            value=value_to_kafka(content.to_dict()),
        )

    producer.flush()

    client = JournalClient(
        brokers=['localhost:%d' % kafka_server[1]],
        group_id=kafka_consumer_group,
        prefix=kafka_prefix,
        stop_after_objects=num_objects,
        batch_size=batch_size,
    )

    collected_output: List[Dict] = []

    def worker_fn(objects):
        received = objects['content']
        assert len(received) <= batch_size
        collected_output.extend(received)

    client.process(worker_fn)

    assert collected_output == [content.to_dict() for content in contents]
