# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
from multiprocessing import Manager
from typing import Any, Generic, List, Tuple, TypeVar

from . import ValueProtocol

logger = logging.getLogger(__name__)


TValue = TypeVar("TValue", bound=ValueProtocol)


class InMemoryJournalWriter(Generic[TValue]):
    objects: List[Tuple[str, TValue]]
    privileged_objects: List[Tuple[str, TValue]]

    def __init__(self, value_sanitizer: Any):
        # Share the list of objects across processes, for RemoteAPI tests.
        self.manager = Manager()
        self.objects = self.manager.list()
        self.privileged_objects = self.manager.list()

    def write_addition(
        self, object_type: str, object_: TValue, privileged: bool = False
    ) -> None:
        if privileged:
            self.privileged_objects.append((object_type, object_))
        else:
            self.objects.append((object_type, object_))

    write_update = write_addition

    def write_additions(
        self, object_type: str, objects: List[TValue], privileged: bool = False
    ) -> None:
        for object_ in objects:
            self.write_addition(object_type, object_, privileged)
