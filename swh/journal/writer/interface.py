# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, Dict, Iterable, Optional, TypeVar

from typing_extensions import Protocol, runtime_checkable

from swh.model.model import KeyType

TSelf = TypeVar("TSelf")


class ValueProtocol(Protocol):
    def anonymize(self: TSelf) -> Optional[TSelf]:
        ...

    def unique_key(self) -> KeyType:
        ...

    def to_dict(self) -> Dict[str, Any]:
        ...


@runtime_checkable
class JournalWriterInterface(Protocol):
    def write_addition(self, object_type: str, object_: ValueProtocol) -> None:
        """Add a SWH object of type object_type in the journal."""
        ...

    def write_additions(
        self, object_type: str, objects: Iterable[ValueProtocol]
    ) -> None:
        """Add a list of SWH objects of type object_type in the journal."""
        ...

    def flush(self) -> None:
        """Flush the pending object additions in the backend, if any."""
        ...
