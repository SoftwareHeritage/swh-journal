# Copyright (C) 2017-2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import OrderedDict
from datetime import datetime, timedelta, timezone
import itertools
from typing import List

import pytest

from swh.journal import serializers
from swh.model.tests.swh_model_data import TEST_OBJECTS


def test_key_to_kafka_repeatable():
    """Check the kafka key encoding is repeatable"""
    base_dict = {
        "a": "foo",
        "b": "bar",
        "c": "baz",
    }

    key = serializers.key_to_kafka(base_dict)

    for dict_keys in itertools.permutations(base_dict):
        d = OrderedDict()
        for k in dict_keys:
            d[k] = base_dict[k]

        assert key == serializers.key_to_kafka(d)


def test_pprint_key():
    """Test whether get_key works on all our objects"""
    for object_type, objects in TEST_OBJECTS.items():
        for obj in objects:
            key = obj.unique_key()
            pprinted_key = serializers.pprint_key(key)
            assert isinstance(pprinted_key, str)

            if isinstance(key, dict):
                assert pprinted_key[0], pprinted_key[-1] == "{}"
                for dict_key in key.keys():
                    assert f"{dict_key}:" in pprinted_key

            if isinstance(key, bytes):
                assert pprinted_key == key.hex()


def test_kafka_to_key() -> None:
    """Standard back and forth serialization with keys"""
    # All KeyType(s)
    keys: List[serializers.KeyType] = [
        {
            "a": "foo",
            "b": "bar",
            "c": "baz",
        },
        {
            "a": b"foobarbaz",
        },
        b"foo",
    ]
    for object_type, objects in TEST_OBJECTS.items():
        for obj in objects:
            key = obj.unique_key()
            keys.append(key)

    for key in keys:
        ktk = serializers.key_to_kafka(key)
        v = serializers.kafka_to_key(ktk)

        assert v == key


# limits of supported int values by msgpack
MININT = -(2**63)
MAXINT = 2**64 - 1

intvalues = [
    MININT * 2,
    MININT - 1,
    MININT,
    MININT + 1,
    -10,
    0,
    10,
    MAXINT - 1,
    MAXINT,
    MAXINT + 1,
    MAXINT * 2,
]


@pytest.mark.parametrize("value", intvalues)
def test_encode_int(value):
    assert serializers.kafka_to_value(serializers.value_to_kafka(value)) == value


datevalues = [
    datetime.now(tz=timezone.utc),
    datetime.now(tz=timezone(timedelta(hours=-23, minutes=-59))),
    datetime.now(tz=timezone(timedelta(hours=23, minutes=59))),
    datetime(1, 1, 1, 1, 1, tzinfo=timezone.utc),
    datetime(2100, 1, 1, 1, 1, tzinfo=timezone.utc),
]


@pytest.mark.parametrize("value", datevalues)
def test_encode_datetime(value):
    assert serializers.kafka_to_value(serializers.value_to_kafka(value)) == value


@pytest.mark.parametrize("value", datevalues)
def test_encode_datetime_bw(value):
    bwdate = {b"swhtype": "datetime", b"d": value.isoformat()}
    assert serializers.kafka_to_value(serializers.value_to_kafka(bwdate)) == value
