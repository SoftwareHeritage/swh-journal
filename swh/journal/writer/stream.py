# Copyright (C) 2021-2022 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
from typing import Any, BinaryIO, Callable, Dict, Iterable

from swh.journal.serializers import value_to_kafka

from .interface import ValueProtocol

logger = logging.getLogger(__name__)


class StreamJournalWriter:
    """A simple JournalWriter which serializes objects in a stream

    Might be used to serialize a storage in a file to generate a test dataset.
    """

    def __init__(
        self,
        output_stream: BinaryIO,
        value_sanitizer: Callable[[str, Dict[str, Any]], Dict[str, Any]],
    ):
        # Share the list of objects across processes, for RemoteAPI tests.
        self.output = output_stream
        self.value_sanitizer = value_sanitizer

    def write_addition(self, object_type: str, object_: ValueProtocol) -> None:
        object_.unique_key()  # Check this does not error, to mimic the kafka writer
        dict_ = self.value_sanitizer(object_type, object_.to_dict())
        self.output.write(value_to_kafka((object_type, dict_)))

    def write_additions(
        self, object_type: str, objects: Iterable[ValueProtocol]
    ) -> None:
        for object_ in objects:
            self.write_addition(object_type, object_)

    def flush(self) -> None:
        self.output.flush()
