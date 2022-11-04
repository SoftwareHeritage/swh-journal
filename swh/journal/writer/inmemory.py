# Copyright (C) 2019-2022 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
from multiprocessing import Manager
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from .interface import KeyType, ValueProtocol

logger = logging.getLogger(__name__)


class InMemoryJournalWriter:
    objects: List[Tuple[str, KeyType, Optional[ValueProtocol]]]
    privileged_objects: List[Tuple[str, KeyType, Optional[ValueProtocol]]]

    def __init__(
        self,
        value_sanitizer: Callable[[str, Dict[str, Any]], Dict[str, Any]],
        anonymize: bool = False,
    ):
        # Share the list of objects across processes, for RemoteAPI tests.
        self.manager = Manager()
        self.objects = self.manager.list()
        self.privileged_objects = self.manager.list()
        self.anonymize = anonymize

    def write_addition(self, object_type: str, object_: ValueProtocol) -> None:
        key = object_.unique_key()
        anon_object_ = None
        if self.anonymize:
            anon_object_ = object_.anonymize()
        if anon_object_ is not None:
            self.privileged_objects.append((object_type, key, object_))
            self.objects.append((object_type, key, anon_object_))
        else:
            self.objects.append((object_type, key, object_))

    def write_additions(
        self, object_type: str, objects: Iterable[ValueProtocol]
    ) -> None:
        for object_ in objects:
            self.write_addition(object_type, object_)

    def flush(self) -> None:
        pass

    def delete(self, object_type: str, object_keys: Iterable[KeyType]) -> None:
        for key in object_keys:
            self.objects.append((object_type, key, None))
