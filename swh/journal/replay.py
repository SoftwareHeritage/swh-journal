# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

from swh.storage import HashCollision

from .client import JournalClient


logger = logging.getLogger(__name__)


OBJECT_TYPES = frozenset([
    'origin', 'origin_visit', 'snapshot', 'release', 'revision',
    'directory', 'content',
])


class StorageReplayer(JournalClient):
    def __init__(self, *args, storage, **kwargs):
        super().__init__(*args, **kwargs)
        self.storage = storage

    def process_objects(self, all_objects):
        for (object_type, objects) in all_objects.items():
            self.insert_objects(object_type, objects)

    def insert_objects(self, object_type, objects):
        if object_type in ('content', 'directory', 'revision', 'release',
                           'snapshot', 'origin'):
            if object_type == 'content':
                # TODO: insert 'content' in batches
                for object_ in objects:
                    try:
                        self.storage.content_add_metadata([object_])
                    except HashCollision as e:
                        logger.error('Hash collision: %s', e.args)
            else:
                # TODO: split batches that are too large for the storage
                # to handle?
                method = getattr(self.storage, object_type + '_add')
                method(objects)
        elif object_type == 'origin_visit':
            self.storage.origin_visit_upsert([
                {
                    **obj,
                    'origin': self.storage.origin_add_one(obj['origin'])
                }
                for obj in objects])
        else:
            assert False
