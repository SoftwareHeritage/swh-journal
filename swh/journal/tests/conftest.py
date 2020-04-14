# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import os
import pytest
import logging
import random
import string

from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, ConfigResource

from hypothesis.strategies import one_of
from subprocess import Popen
from typing import Any, Dict, Iterator, List, Tuple

from pathlib import Path
from pytest_kafka import (
    make_zookeeper_process,
    make_kafka_server,
    KAFKA_SERVER_CONFIG_TEMPLATE,
    ZOOKEEPER_CONFIG_TEMPLATE,
)

from swh.model import hypothesis_strategies as strategies
from swh.model.hashutil import MultiHash, hash_to_bytes

from swh.journal.serializers import ModelObject
from swh.journal.writer.kafka import OBJECT_TYPES


logger = logging.getLogger(__name__)

CONTENTS = [
    {**MultiHash.from_data(b"foo").digest(), "length": 3, "status": "visible",},
]

duplicate_content1 = {
    "length": 4,
    "sha1": hash_to_bytes("44973274ccef6ab4dfaaf86599792fa9c3fe4689"),
    "sha1_git": b"another-foo",
    "blake2s256": b"another-bar",
    "sha256": b"another-baz",
    "status": "visible",
}

# Craft a sha1 collision
duplicate_content2 = duplicate_content1.copy()
sha1_array = bytearray(duplicate_content1["sha1_git"])
sha1_array[0] += 1
duplicate_content2["sha1_git"] = bytes(sha1_array)


DUPLICATE_CONTENTS = [duplicate_content1, duplicate_content2]


COMMITTERS = [
    {"fullname": b"foo", "name": b"foo", "email": b"",},
    {"fullname": b"bar", "name": b"bar", "email": b"",},
]

DATES = [
    {
        "timestamp": {"seconds": 1234567891, "microseconds": 0,},
        "offset": 120,
        "negative_utc": False,
    },
    {
        "timestamp": {"seconds": 1234567892, "microseconds": 0,},
        "offset": 120,
        "negative_utc": False,
    },
]

REVISIONS = [
    {
        "id": hash_to_bytes("7026b7c1a2af56521e951c01ed20f255fa054238"),
        "message": b"hello",
        "date": DATES[0],
        "committer": COMMITTERS[0],
        "author": COMMITTERS[0],
        "committer_date": DATES[0],
        "type": "git",
        "directory": b"\x01" * 20,
        "synthetic": False,
        "metadata": None,
        "parents": [],
    },
    {
        "id": hash_to_bytes("368a48fe15b7db2383775f97c6b247011b3f14f4"),
        "message": b"hello again",
        "date": DATES[1],
        "committer": COMMITTERS[1],
        "author": COMMITTERS[1],
        "committer_date": DATES[1],
        "type": "hg",
        "directory": b"\x02" * 20,
        "synthetic": False,
        "metadata": None,
        "parents": [],
    },
]

RELEASES = [
    {
        "id": hash_to_bytes("d81cc0710eb6cf9efd5b920a8453e1e07157b6cd"),
        "name": b"v0.0.1",
        "date": {
            "timestamp": {"seconds": 1234567890, "microseconds": 0,},
            "offset": 120,
            "negative_utc": False,
        },
        "author": COMMITTERS[0],
        "target_type": "revision",
        "target": b"\x04" * 20,
        "message": b"foo",
        "synthetic": False,
    },
]

ORIGINS = [
    {"url": "https://somewhere.org/den/fox",},
    {"url": "https://overtherainbow.org/fox/den",},
]

ORIGIN_VISITS = [
    {
        "origin": ORIGINS[0]["url"],
        "date": "2013-05-07 04:20:39.369271+00:00",
        "snapshot": None,  # TODO
        "status": "ongoing",  # TODO
        "metadata": {"foo": "bar"},
        "type": "git",
    },
    {
        "origin": ORIGINS[0]["url"],
        "date": "2018-11-27 17:20:39+00:00",
        "snapshot": None,  # TODO
        "status": "ongoing",  # TODO
        "metadata": {"baz": "qux"},
        "type": "git",
    },
]

TEST_OBJECT_DICTS: Dict[str, List[Dict[str, Any]]] = {
    "content": CONTENTS,
    "revision": REVISIONS,
    "release": RELEASES,
    "origin": ORIGINS,
    "origin_visit": ORIGIN_VISITS,
}

MODEL_OBJECTS = {v: k for (k, v) in OBJECT_TYPES.items()}

TEST_OBJECTS: Dict[str, List[ModelObject]] = {}

for object_type, objects in TEST_OBJECT_DICTS.items():
    converted_objects: List[ModelObject] = []
    model = MODEL_OBJECTS[object_type]

    for (num, obj_d) in enumerate(objects):
        if object_type == "origin_visit":
            obj_d = {**obj_d, "visit": num}
        elif object_type == "content":
            obj_d = {**obj_d, "data": b"", "ctime": datetime.datetime.now()}

        converted_objects.append(model.from_dict(obj_d))

    TEST_OBJECTS[object_type] = converted_objects


KAFKA_ROOT = os.environ.get("SWH_KAFKA_ROOT")
KAFKA_ROOT = KAFKA_ROOT if KAFKA_ROOT else os.path.dirname(__file__) + "/kafka"
if not os.path.exists(KAFKA_ROOT):
    msg = (
        "Development error: %s must exist and target an "
        "existing kafka installation" % KAFKA_ROOT
    )
    raise ValueError(msg)

KAFKA_SCRIPTS = Path(KAFKA_ROOT) / "bin"

KAFKA_BIN = str(KAFKA_SCRIPTS / "kafka-server-start.sh")
ZOOKEEPER_BIN = str(KAFKA_SCRIPTS / "zookeeper-server-start.sh")

ZK_CONFIG_TEMPLATE = ZOOKEEPER_CONFIG_TEMPLATE + "\nadmin.enableServer=false\n"
KAFKA_CONFIG_TEMPLATE = KAFKA_SERVER_CONFIG_TEMPLATE + "\nmessage.max.bytes=104857600\n"

# Those defines fixtures
zookeeper_proc = make_zookeeper_process(
    ZOOKEEPER_BIN, zk_config_template=ZK_CONFIG_TEMPLATE, scope="session"
)
os.environ[
    "KAFKA_LOG4J_OPTS"
] = "-Dlog4j.configuration=file:%s/log4j.properties" % os.path.dirname(__file__)
session_kafka_server = make_kafka_server(
    KAFKA_BIN,
    "zookeeper_proc",
    kafka_config_template=KAFKA_CONFIG_TEMPLATE,
    scope="session",
)

kafka_logger = logging.getLogger("kafka")
kafka_logger.setLevel(logging.WARN)


@pytest.fixture(scope="function")
def kafka_prefix():
    """Pick a random prefix for kafka topics on each call"""
    return "".join(random.choice(string.ascii_lowercase) for _ in range(10))


@pytest.fixture(scope="function")
def kafka_consumer_group(kafka_prefix: str):
    """Pick a random consumer group for kafka consumers on each call"""
    return "test-consumer-%s" % kafka_prefix


@pytest.fixture(scope="session")
def kafka_admin_client(session_kafka_server: Tuple[Popen, int]) -> AdminClient:
    return AdminClient({"bootstrap.servers": "localhost:%s" % session_kafka_server[1]})


@pytest.fixture(scope="function")
def kafka_server_config_overrides() -> Dict[str, str]:
    return {}


@pytest.fixture(scope="function")
def kafka_server(
    session_kafka_server: Tuple[Popen, int],
    kafka_admin_client: AdminClient,
    kafka_server_config_overrides: Dict[str, str],
) -> Iterator[Tuple[Popen, int]]:
    # No overrides, we can just return the original broker connection
    if not kafka_server_config_overrides:
        yield session_kafka_server
        return

    # This is the minimal operation that the kafka_admin_client gives to
    # retrieve the cluster metadata, which we need to get the numeric id of the
    # broker spawned by pytest_kafka.
    metadata = kafka_admin_client.list_topics("__consumer_offsets")
    broker_ids = [str(broker) for broker in metadata.brokers.keys()]
    assert len(broker_ids) == 1, "More than one broker found in the kafka cluster?!"

    # Pull the current broker configuration. describe_configs and alter_configs
    # generate a dict containing one concurrent.future per queried
    # ConfigResource, hence the use of .result()
    broker = ConfigResource("broker", broker_ids[0])
    futures = kafka_admin_client.describe_configs([broker])
    original_config = futures[broker].result()

    # Gather the list of settings we need to change in the broker
    # ConfigResource, and their original values in the to_restore dict
    to_restore = {}
    for key, new_value in kafka_server_config_overrides.items():
        if key not in original_config:
            raise ValueError(f"Cannot override unknown configuration {key}")
        orig_value = original_config[key].value
        if orig_value == new_value:
            continue
        if original_config[key].is_read_only:
            raise ValueError(f"Cannot override read-only configuration {key}")

        broker.set_config(key, new_value)
        to_restore[key] = orig_value

    # to_restore will be empty if all the config "overrides" are equal to the
    # original value. No need to wait for a config alteration if that's the
    # case. The result() will raise a KafkaException if the settings change
    # failed.
    if to_restore:
        futures = kafka_admin_client.alter_configs([broker])
        try:
            futures[broker].result()
        except Exception:
            raise

    yield session_kafka_server

    # Now we can restore the old setting values. Again, the result() will raise
    # a KafkaException if the settings change failed.
    if to_restore:
        for key, orig_value in to_restore.items():
            broker.set_config(key, orig_value)

        futures = kafka_admin_client.alter_configs([broker])
        try:
            futures[broker].result()
        except Exception:
            raise


TEST_CONFIG = {
    "consumer_id": "swh.journal.consumer",
    "object_types": TEST_OBJECT_DICTS.keys(),
    "stop_after_objects": 1,  # will read 1 object and stop
    "storage": {"cls": "memory", "args": {}},
}


@pytest.fixture
def test_config(kafka_server: Tuple[Popen, int], kafka_prefix: str):
    """Test configuration needed for producer/consumer

    """
    _, port = kafka_server
    return {
        **TEST_CONFIG,
        "brokers": ["127.0.0.1:{}".format(port)],
        "prefix": kafka_prefix + ".swh.journal.objects",
    }


@pytest.fixture
def consumer(
    kafka_server: Tuple[Popen, int], test_config: Dict, kafka_consumer_group: str,
) -> Consumer:
    """Get a connected Kafka consumer.

    """
    _, kafka_port = kafka_server
    consumer = Consumer(
        {
            "bootstrap.servers": "127.0.0.1:{}".format(kafka_port),
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
            "group.id": kafka_consumer_group,
        }
    )

    kafka_topics = [
        "%s.%s" % (test_config["prefix"], object_type)
        for object_type in test_config["object_types"]
    ]

    consumer.subscribe(kafka_topics)

    yield consumer

    consumer.close()


def objects_d():
    return one_of(
        strategies.origins().map(lambda x: ("origin", x.to_dict())),
        strategies.origin_visits().map(lambda x: ("origin_visit", x.to_dict())),
        strategies.snapshots().map(lambda x: ("snapshot", x.to_dict())),
        strategies.releases().map(lambda x: ("release", x.to_dict())),
        strategies.revisions().map(lambda x: ("revision", x.to_dict())),
        strategies.directories().map(lambda x: ("directory", x.to_dict())),
        strategies.skipped_contents().map(lambda x: ("skipped_content", x.to_dict())),
        strategies.present_contents().map(lambda x: ("content", x.to_dict())),
    )
