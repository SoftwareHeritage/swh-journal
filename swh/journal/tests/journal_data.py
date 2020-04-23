# Copyright (C) 2019-2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime

from typing import Any, Dict, List

from swh.model.hashutil import MultiHash, hash_to_bytes
from swh.journal.serializers import ModelObject
from swh.journal.writer.kafka import OBJECT_TYPES


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
