# Copyright (C) 2016-2017 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, Dict, Union, overload

import msgpack

from swh.core.api.serializers import msgpack_dumps, msgpack_loads
from swh.model.hashutil import DEFAULT_ALGORITHMS
from swh.model.model import (
    Content,
    Directory,
    Origin,
    OriginVisit,
    Release,
    Revision,
    SkippedContent,
    Snapshot,
)

ModelObject = Union[
    Content, Directory, Origin, OriginVisit, Release, Revision, SkippedContent, Snapshot
]

KeyType = Union[Dict[str, str], Dict[str, bytes], bytes]


# these @overload'ed versions of the object_key method aim at helping mypy figuring
# the correct type-ing.
@overload
def object_key(
    object_type: str, object_: Union[Content, Directory, Revision, Release, Snapshot]
) -> bytes:
    ...


@overload
def object_key(
    object_type: str, object_: Union[Origin, SkippedContent]
) -> Dict[str, bytes]:
    ...


@overload
def object_key(object_type: str, object_: OriginVisit) -> Dict[str, str]:
    ...


def object_key(object_type: str, object_) -> KeyType:
    if object_type in ("revision", "release", "directory", "snapshot"):
        return object_.id
    elif object_type == "content":
        return object_.sha1  # TODO: use a dict of hashes
    elif object_type == "skipped_content":
        return {hash: getattr(object_, hash) for hash in DEFAULT_ALGORITHMS}
    elif object_type == "origin":
        return {"url": object_.url}
    elif object_type == "origin_visit":
        return {
            "origin": object_.origin,
            "date": str(object_.date),
        }
    elif object_type == "origin_visit_status":
        return {
            "origin": object_.origin,
            "visit": str(object_.visit),
            "date": str(object_.date),
        }
    else:
        raise ValueError("Unknown object type: %s." % object_type)


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
    if isinstance(value, list):
        return tuple(value)
    if isinstance(value, dict):
        return ensure_tuples(value)
    return value


def ensure_tuples(value: Dict) -> Dict:
    return {k: tuple(v) if isinstance(v, list) else v for k, v in value.items()}
