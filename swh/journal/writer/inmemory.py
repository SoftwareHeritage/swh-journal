# Copyright (C) 2019-2022 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
from multiprocessing import Manager
from typing import Any, Callable, Dict, Iterable, List, Tuple

from .interface import ValueProtocol

logger = logging.getLogger(__name__)


class InMemoryJournalWriter:
    objects: List[Tuple[str, ValueProtocol]]
    privileged_objects: List[Tuple[str, ValueProtocol]]

    def __init__(
        self,
        value_sanitizer: Callable[[str, Dict[str, Any]], Dict[str, Any]],
        anonymize: bool = False,
        use_shared_memory: bool = False,
    ):
        # Share the list of objects across processes, for RemoteAPI tests.
        if use_shared_memory:
            self.manager = Manager()
            self.objects = self.manager.list()  # type: ignore[assignment]
            self.privileged_objects = self.manager.list()  # type: ignore[assignment]
        else:
            self.objects = []
            self.privileged_objects = []
        self.anonymize = anonymize

    def write_addition(self, object_type: str, object_: ValueProtocol) -> None:
        object_.unique_key()  # Check this does not error, to mimic the kafka writer
        anon_object_ = None
        if self.anonymize:
            anon_object_ = object_.anonymize()
        if anon_object_ is not None:
            self.privileged_objects.append((object_type, object_))
            self.objects.append((object_type, anon_object_))
        else:
            self.objects.append((object_type, object_))

    def write_additions(
        self, object_type: str, objects: Iterable[ValueProtocol]
    ) -> None:
        for object_ in objects:
            self.write_addition(object_type, object_)

    def flush(self) -> None:
        pass
