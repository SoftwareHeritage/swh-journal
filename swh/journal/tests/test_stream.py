# Copyright (C) 2021 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import io

import msgpack

from swh.journal.serializers import msgpack_ext_hook
from swh.journal.writer import get_journal_writer, model_object_dict_sanitizer
from swh.model.tests.swh_model_data import TEST_OBJECTS


def test_write_additions_with_test_objects():
    outs = io.BytesIO()

    writer = get_journal_writer(
        cls="stream", value_sanitizer=model_object_dict_sanitizer, output_stream=outs,
    )
    expected = []

    n = 0
    for object_type, objects in TEST_OBJECTS.items():
        writer.write_additions(object_type, objects)

        for object in objects:
            objd = object.to_dict()
            if object_type == "content":
                objd.pop("data")

            expected.append((object_type, objd))
        n += len(objects)

    outs.seek(0, 0)
    unpacker = msgpack.Unpacker(
        outs,
        raw=False,
        ext_hook=msgpack_ext_hook,
        strict_map_key=False,
        use_list=False,
        timestamp=3,  # convert Timestamp in datetime objects (tz UTC)
    )

    for i, (objtype, objd) in enumerate(unpacker, start=1):
        assert (objtype, objd) in expected
    assert len(expected) == i
