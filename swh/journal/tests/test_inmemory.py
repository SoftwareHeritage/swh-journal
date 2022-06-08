# Copyright (C) 2019-2022 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.journal.writer import model_object_dict_sanitizer
from swh.journal.writer.inmemory import InMemoryJournalWriter
from swh.model.model import BaseModel
from swh.model.tests.swh_model_data import TEST_OBJECTS


def test_write_additions_anonymized():
    writer = InMemoryJournalWriter(
        value_sanitizer=model_object_dict_sanitizer, anonymize=True
    )
    expected = []
    priv_expected = []

    for object_type, objects in TEST_OBJECTS.items():
        writer.write_additions(object_type, objects)

        for object in objects:
            if object.anonymize():
                expected.append((object_type, object.anonymize()))
                priv_expected.append((object_type, object))
            else:
                expected.append((object_type, object))

    assert set(priv_expected) == set(writer.privileged_objects)
    assert set(expected) == set(writer.objects)


def test_write_additions():
    writer = InMemoryJournalWriter(value_sanitizer=model_object_dict_sanitizer)
    expected = set()

    for object_type, objects in TEST_OBJECTS.items():
        writer.write_additions(object_type, objects)

        for object in objects:
            expected.add((object_type, object))

    assert not set(writer.privileged_objects)
    assert expected == set(writer.objects)


def test_write_addition_errors_without_unique_key():
    writer = InMemoryJournalWriter(value_sanitizer=model_object_dict_sanitizer)

    with pytest.raises(NotImplementedError):
        writer.write_addition("BaseModel", BaseModel())
