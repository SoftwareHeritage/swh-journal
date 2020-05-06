# Copyright (C) 2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Iterator
from confluent_kafka.admin import AdminClient


def test_kafka_server(kafka_server_base: str):
    ip, port_str = kafka_server_base.split(":")
    assert ip == "127.0.0.1"
    assert int(port_str)

    admin = AdminClient({"bootstrap.servers": kafka_server_base})

    topics = admin.list_topics()

    assert len(topics.brokers) == 1


def test_kafka_server_with_topics(
    kafka_server: str, kafka_prefix: str, object_types: Iterator[str]
):
    admin = AdminClient({"bootstrap.servers": kafka_server})
    topics = {
        topic
        for topic in admin.list_topics().topics
        if topic.startswith(f"{kafka_prefix}.")
    }
    assert topics == {f"{kafka_prefix}.{obj}" for obj in object_types}


def test_test_config(test_config: dict, kafka_prefix: str, kafka_server_base: str):
    assert test_config == {
        "consumer_id": "swh.journal.consumer",
        "stop_after_objects": 1,
        "storage": {"cls": "memory", "args": {}},
        "object_types": {
            "content",
            "directory",
            "origin",
            "origin_visit",
            "release",
            "revision",
            "snapshot",
            "skipped_content",
        },
        "brokers": [kafka_server_base],
        "prefix": kafka_prefix + ".swh.journal.objects",
    }