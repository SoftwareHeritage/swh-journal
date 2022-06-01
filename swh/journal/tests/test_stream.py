# Copyright (C) 2021-2022 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import io
from typing import Dict, List, Tuple

from swh.journal.serializers import kafka_stream_to_value
from swh.journal.writer import get_journal_writer, model_object_dict_sanitizer
from swh.journal.writer.interface import JournalWriterInterface
from swh.model.tests.swh_model_data import TEST_OBJECTS


def fill_writer(writer: JournalWriterInterface) -> List[Tuple[str, Dict]]:
    expected = []
    for object_type, objects in TEST_OBJECTS.items():
        writer.write_additions(object_type, objects)

        for object in objects:
            objd = object.to_dict()
            if object_type == "content":
                objd.pop("data")

            expected.append((object_type, objd))
    writer.flush()
    return expected


def test_stream_journal_writer_stream():
    outs = io.BytesIO()

    writer = get_journal_writer(
        cls="stream",
        value_sanitizer=model_object_dict_sanitizer,
        output_stream=outs,
    )
    expected = fill_writer(writer)

    outs.seek(0, 0)
    unpacker = kafka_stream_to_value(outs)
    for i, (objtype, objd) in enumerate(unpacker, start=1):
        assert (objtype, objd) in expected
    assert len(expected) == i


def test_stream_journal_writer_filename(tmp_path):
    out_fname = str(tmp_path / "journal.msgpack")

    writer = get_journal_writer(
        cls="stream",
        value_sanitizer=model_object_dict_sanitizer,
        output_stream=out_fname,
    )
    expected = fill_writer(writer)

    with open(out_fname, "rb") as outs:
        unpacker = kafka_stream_to_value(outs)
        for i, (objtype, objd) in enumerate(unpacker, start=1):
            assert (objtype, objd) in expected
        assert len(expected) == i


def test_stream_journal_writer_stdout(capfdbinary):
    writer = get_journal_writer(
        cls="stream",
        value_sanitizer=model_object_dict_sanitizer,
        output_stream="-",
    )
    expected = fill_writer(writer)

    captured = capfdbinary.readouterr()
    assert captured.err == b""
    outs = io.BytesIO(captured.out)

    unpacker = kafka_stream_to_value(outs)
    for i, (objtype, objd) in enumerate(unpacker, start=1):
        assert (objtype, objd) in expected
    assert len(expected) == i
