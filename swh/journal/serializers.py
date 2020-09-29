# Copyright (C) 2016-2017 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, Union

import msgpack

from swh.core.api.serializers import msgpack_dumps, msgpack_loads
from swh.model.model import KeyType


def stringify_key_item(k: str, v: Union[str, bytes]) -> str:
    """Turn the item of a dict key into a string"""
    if isinstance(v, str):
        return v
    if k == "url":
        return v.decode("utf-8")
    return v.hex()


def pprint_key(key: KeyType) -> str:
    """Pretty-print a kafka key"""

    if isinstance(key, dict):
        return "{%s}" % ", ".join(
            f"{k}: {stringify_key_item(k, v)}" for k, v in key.items()
        )
    elif isinstance(key, bytes):
        return key.hex()
    else:
        return key


def key_to_kafka(key: KeyType) -> bytes:
    """Serialize a key, possibly a dict, in a predictable way"""
    p = msgpack.Packer(use_bin_type=True)
    if isinstance(key, dict):
        return p.pack_map_pairs(sorted(key.items()))
    else:
        return p.pack(key)


def kafka_to_key(kafka_key: bytes) -> KeyType:
    """Deserialize a key"""
    return msgpack.loads(kafka_key, raw=False)


def value_to_kafka(value: Any) -> bytes:
    """Serialize some data for storage in kafka"""
    return msgpack_dumps(value)


def kafka_to_value(kafka_value: bytes) -> Any:
    """Deserialize some data stored in kafka"""
    value = msgpack_loads(kafka_value)
    return ensure_tuples(value)


def ensure_tuples(value: Any) -> Any:
    if isinstance(value, (tuple, list)):
        return tuple(map(ensure_tuples, value))
    elif isinstance(value, dict):
        return dict(ensure_tuples(list(value.items())))
    else:
        return value
