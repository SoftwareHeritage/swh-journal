# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

from multiprocessing import Manager
from typing import List

from swh.model.model import BaseModel

from .kafka import ModelObject

logger = logging.getLogger(__name__)


class InMemoryJournalWriter:
    def __init__(self):
        # Share the list of objects across processes, for RemoteAPI tests.
        self.manager = Manager()
        self.objects = self.manager.list()
        self.privileged_objects = self.manager.list()

    def write_addition(
        self, object_type: str, object_: ModelObject, privileged: bool = False
    ) -> None:
        assert isinstance(object_, BaseModel)
        if privileged:
            self.privileged_objects.append((object_type, object_))
        else:
            self.objects.append((object_type, object_))

    write_update = write_addition

    def write_additions(
        self, object_type: str, objects: List[ModelObject], privileged: bool = False
    ) -> None:
        for object_ in objects:
            self.write_addition(object_type, object_, privileged)
