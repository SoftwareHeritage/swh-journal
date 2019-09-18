# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import copy
from multiprocessing import Manager

from swh.model.model import BaseModel

logger = logging.getLogger(__name__)


class InMemoryJournalWriter:
    def __init__(self):
        # Share the list of objects across processes, for RemoteAPI tests.
        self.manager = Manager()
        self.objects = self.manager.list()

    def write_addition(self, object_type, object_):
        if isinstance(object_, BaseModel):
            object_ = object_.to_dict()
        self.objects.append((object_type, copy.deepcopy(object_)))

    write_update = write_addition

    def write_additions(self, object_type, objects):
        for object_ in objects:
            self.write_addition(object_type, object_)
