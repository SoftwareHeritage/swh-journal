# Copyright (C) 2019-2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime

from typing import Any, Dict, List, Type

from swh.model.hashutil import MultiHash, hash_to_bytes
from swh.journal.serializers import ModelObject

from swh.model.model import (
    BaseModel,
    Content,
    Directory,
    Origin,
    OriginVisit,
    OriginVisitStatus,
    Release,
    Revision,
    SkippedContent,
    Snapshot,
)


OBJECT_TYPES: Dict[Type[BaseModel], str] = {
    Content: "content",
    Directory: "directory",
    Origin: "origin",
    OriginVisit: "origin_visit",
    OriginVisitStatus: "origin_visit_status",
    Release: "release",
    Revision: "revision",
    SkippedContent: "skipped_content",
    Snapshot: "snapshot",
}

UTC = datetime.timezone.utc

CONTENTS = [
    {
        **MultiHash.from_data(f"foo{i}".encode()).digest(),
        "length": 4,
        "status": "visible",
    }
    for i in range(10)
] + [
    {
        **MultiHash.from_data(f"forbidden foo{i}".encode()).digest(),
        "length": 14,
        "status": "hidden",
    }
    for i in range(10)
]

SKIPPED_CONTENTS = [
    {
        **MultiHash.from_data(f"bar{i}".encode()).digest(),
        "length": 4,
        "status": "absent",
        "reason": f"because chr({i}) != '*'",
    }
    for i in range(2)
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
        "parents": (),
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
        "parents": (),
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
        "date": datetime.datetime(2013, 5, 7, 4, 20, 39, 369271, tzinfo=UTC),
        "visit": 1,
        "type": "git",
    },
    {
        "origin": ORIGINS[1]["url"],
        "date": datetime.datetime(2014, 11, 27, 17, 20, 39, tzinfo=UTC),
        "visit": 1,
        "type": "hg",
    },
    {
        "origin": ORIGINS[0]["url"],
        "date": datetime.datetime(2018, 11, 27, 17, 20, 39, tzinfo=UTC),
        "visit": 2,
        "type": "git",
    },
    {
        "origin": ORIGINS[0]["url"],
        "date": datetime.datetime(2018, 11, 27, 17, 20, 39, tzinfo=UTC),
        "visit": 3,
        "type": "git",
    },
    {
        "origin": ORIGINS[1]["url"],
        "date": datetime.datetime(2015, 11, 27, 17, 20, 39, tzinfo=UTC),
        "visit": 2,
        "type": "hg",
    },
]

# The origin-visit-status dates needs to be shifted slightly in the future from their
# visit dates counterpart. Otherwise, we are hitting storage-wise the "on conflict"
# ignore policy (because origin-visit-add creates an origin-visit-status with the same
# parameters from the origin-visit {origin, visit, date}...
ORIGIN_VISIT_STATUSES = [
    {
        "origin": ORIGINS[0]["url"],
        "date": datetime.datetime(2013, 5, 7, 4, 20, 39, 432222, tzinfo=UTC),
        "visit": 1,
        "status": "ongoing",
        "snapshot": None,
        "metadata": None,
    },
    {
        "origin": ORIGINS[1]["url"],
        "date": datetime.datetime(2014, 11, 27, 17, 21, 12, tzinfo=UTC),
        "visit": 1,
        "status": "ongoing",
        "snapshot": None,
        "metadata": None,
    },
    {
        "origin": ORIGINS[0]["url"],
        "date": datetime.datetime(2018, 11, 27, 17, 20, 59, tzinfo=UTC),
        "visit": 2,
        "status": "ongoing",
        "snapshot": None,
        "metadata": None,
    },
    {
        "origin": ORIGINS[0]["url"],
        "date": datetime.datetime(2018, 11, 27, 17, 20, 49, tzinfo=UTC),
        "visit": 3,
        "status": "full",
        "snapshot": hash_to_bytes("742cdc6be7bf6e895b055227c2300070f056e07b"),
        "metadata": None,
    },
    {
        "origin": ORIGINS[1]["url"],
        "date": datetime.datetime(2015, 11, 27, 17, 22, 18, tzinfo=UTC),
        "visit": 2,
        "status": "partial",
        "snapshot": hash_to_bytes("ecee48397a92b0d034e9752a17459f3691a73ef9"),
        "metadata": None,
    },
]


DIRECTORIES = [
    {"id": hash_to_bytes("4b825dc642cb6eb9a060e54bf8d69288fbee4904"), "entries": ()},
    {
        "id": hash_to_bytes("cc13247a0d6584f297ca37b5868d2cbd242aea03"),
        "entries": (
            {
                "name": b"file1.ext",
                "perms": 0o644,
                "type": "file",
                "target": CONTENTS[0]["sha1_git"],
            },
            {
                "name": b"dir1",
                "perms": 0o755,
                "type": "dir",
                "target": hash_to_bytes("4b825dc642cb6eb9a060e54bf8d69288fbee4904"),
            },
            {
                "name": b"subprepo1",
                "perms": 0o160000,
                "type": "rev",
                "target": REVISIONS[1]["id"],
            },
        ),
    },
]


SNAPSHOTS = [
    {
        "id": hash_to_bytes("742cdc6be7bf6e895b055227c2300070f056e07b"),
        "branches": {
            b"master": {"target_type": "revision", "target": REVISIONS[0]["id"]}
        },
    },
    {
        "id": hash_to_bytes("ecee48397a92b0d034e9752a17459f3691a73ef9"),
        "branches": {
            b"target/revision": {
                "target_type": "revision",
                "target": REVISIONS[0]["id"],
            },
            b"target/alias": {"target_type": "alias", "target": b"target/revision"},
            b"target/directory": {
                "target_type": "directory",
                "target": DIRECTORIES[0]["id"],
            },
            b"target/release": {"target_type": "release", "target": RELEASES[0]["id"]},
            b"target/snapshot": {
                "target_type": "snapshot",
                "target": hash_to_bytes("742cdc6be7bf6e895b055227c2300070f056e07b"),
            },
        },
    },
]


TEST_OBJECT_DICTS: Dict[str, List[Dict[str, Any]]] = {
    "content": CONTENTS,
    "directory": DIRECTORIES,
    "origin": ORIGINS,
    "origin_visit": [
        # temporary adaptation to remove when fields are dropped
        {**o, "status": None, "snapshot": None, "metadata": None}
        for o in ORIGIN_VISITS
    ],
    "origin_visit_status": ORIGIN_VISIT_STATUSES,
    "release": RELEASES,
    "revision": REVISIONS,
    "snapshot": SNAPSHOTS,
    "skipped_content": SKIPPED_CONTENTS,
}

MODEL_OBJECTS = {v: k for (k, v) in OBJECT_TYPES.items()}

TEST_OBJECTS: Dict[str, List[ModelObject]] = {}

for object_type, objects in TEST_OBJECT_DICTS.items():
    converted_objects: List[ModelObject] = []
    model = MODEL_OBJECTS[object_type]

    for (num, obj_d) in enumerate(objects):
        if object_type == "content":
            obj_d = {**obj_d, "data": b"", "ctime": datetime.datetime.now(tz=UTC)}

        converted_objects.append(model.from_dict(obj_d))

    TEST_OBJECTS[object_type] = converted_objects
