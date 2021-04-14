import pytest

from swh.journal.writer import model_object_dict_sanitizer
from swh.journal.writer.inmemory import InMemoryJournalWriter
from swh.model.model import BaseModel


def test_write_addition_errors_without_unique_key():
    writer = InMemoryJournalWriter[BaseModel](
        value_sanitizer=model_object_dict_sanitizer
    )

    with pytest.raises(NotImplementedError):
        writer.write_addition("BaseModel", BaseModel())
