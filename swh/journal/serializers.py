# Copyright (C) 2016-2022 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
from enum import Enum
from typing import Any, BinaryIO, Union

import msgpack

from swh.model.model import KeyType


class MsgpackExtTypeCodes(Enum):
    LONG_INT = 1
    LONG_NEG_INT = 2


# this as been copied from swh.core.api.serializer
# TODO refactor swh.core to make this function available
def _msgpack_encode_longint(value):
    # needed because msgpack will not handle long integers with more than 64 bits
    # which we unfortunately happen to have to deal with from time to time
    if value > 0:
        code = MsgpackExtTypeCodes.LONG_INT.value
    else:
        code = MsgpackExtTypeCodes.LONG_NEG_INT.value
        value = -value
    length, rem = divmod(value.bit_length(), 8)
    if rem:
        length += 1
    return msgpack.ExtType(code, int.to_bytes(value, length, "big"))


def msgpack_ext_encode_types(obj):
    if isinstance(obj, int):
        return _msgpack_encode_longint(obj)
    return obj


def msgpack_ext_hook(code, data):
    if code == MsgpackExtTypeCodes.LONG_INT.value:
        return int.from_bytes(data, "big")
    if code == MsgpackExtTypeCodes.LONG_NEG_INT.value:
        return -int.from_bytes(data, "big")

    raise ValueError("Unknown msgpack extended code %s" % code)


# for BW compat
def decode_types_bw(obj):
    if set(obj.keys()) == {b"d", b"swhtype"} and obj[b"swhtype"] == "datetime":
        return datetime.datetime.fromisoformat(obj[b"d"])
    return obj


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
    return msgpack.packb(
        value,
        use_bin_type=True,
        datetime=True,  # encode datetime as msgpack.Timestamp
        default=msgpack_ext_encode_types,
    )


def kafka_to_value(kafka_value: bytes) -> Any:
    """Deserialize some data stored in kafka"""
    return msgpack.unpackb(
        kafka_value,
        raw=False,
        object_hook=decode_types_bw,
        ext_hook=msgpack_ext_hook,
        strict_map_key=False,
        timestamp=3,  # convert Timestamp in datetime objects (tz UTC)
    )


def kafka_stream_to_value(file_like: BinaryIO) -> msgpack.Unpacker:
    """Return a deserializer for data stored in kafka"""
    return msgpack.Unpacker(
        file_like,
        raw=False,
        object_hook=decode_types_bw,
        ext_hook=msgpack_ext_hook,
        strict_map_key=False,
        use_list=False,
        timestamp=3,  # convert Timestamp in datetime objects (tz UTC)
    )
