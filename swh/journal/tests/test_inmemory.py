import pytest

from swh.journal.writer import model_object_dict_sanitizer
from swh.journal.writer.inmemory import InMemoryJournalWriter
from swh.model.model import BaseModel
from swh.model.tests.swh_model_data import TEST_OBJECTS


def test_write_additions_with_test_objects():
    writer = InMemoryJournalWriter[BaseModel](
        value_sanitizer=model_object_dict_sanitizer
    )
    expected = []

    for object_type, objects in TEST_OBJECTS.items():
        writer.write_additions(object_type, objects)

        for object in objects:
            expected.append((object_type, object))

    assert list(writer.privileged_objects) == []
    assert set(expected) == set(writer.objects)


def test_write_additions_with_privileged_test_objects():
    writer = InMemoryJournalWriter[BaseModel](
        value_sanitizer=model_object_dict_sanitizer
    )

    expected = []

    for object_type, objects in TEST_OBJECTS.items():
        writer.write_additions(object_type, objects, True)

        for object in objects:
            expected.append((object_type, object))

    assert list(writer.objects) == []
    assert set(expected) == set(writer.privileged_objects)


def test_write_addition_errors_without_unique_key():
    writer = InMemoryJournalWriter[BaseModel](
        value_sanitizer=model_object_dict_sanitizer
    )

    with pytest.raises(NotImplementedError):
        writer.write_addition("BaseModel", BaseModel())
