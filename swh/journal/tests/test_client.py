# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from subprocess import Popen
from typing import Tuple
from unittest.mock import MagicMock

from confluent_kafka import Producer

from swh.model.hypothesis_strategies import revisions

from swh.journal.client import JournalClient
from swh.journal.serializers import key_to_kafka, value_to_kafka


def test_client(
        kafka_prefix: str,
        kafka_server: Tuple[Popen, int]):
    (_, port) = kafka_server
    kafka_prefix += '.swh.journal.objects'

    producer = Producer({
        'bootstrap.servers': 'localhost:{}'.format(port),
        'client.id': 'test producer',
        'enable.idempotence': 'true',
    })

    rev = revisions().example()

    # Fill Kafka
    producer.produce(
        topic=kafka_prefix + '.revision', key=key_to_kafka(rev.id),
        value=value_to_kafka(rev.to_dict()),
    )
    producer.flush()

    config = {
        'brokers': 'localhost:%d' % kafka_server[1],
        'group_id': 'replayer',
        'prefix': kafka_prefix,
        'max_messages': 1,
    }
    client = JournalClient(**config)

    worker_fn = MagicMock()
    client.process(worker_fn)

    worker_fn.assert_called_once_with({'revision': [rev.to_dict()]})
