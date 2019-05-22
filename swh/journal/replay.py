# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

from swh.storage import HashCollision


logger = logging.getLogger(__name__)


def process_replay_objects(all_objects, *, storage):
    for (object_type, objects) in all_objects.items():
        _insert_objects(object_type, objects, storage)


def _insert_objects(object_type, objects, storage):
    if object_type == 'content':
        # TODO: insert 'content' in batches
        for object_ in objects:
            try:
                storage.content_add_metadata([object_])
            except HashCollision as e:
                logger.error('Hash collision: %s', e.args)
    elif object_type in ('directory', 'revision', 'release',
                         'snapshot', 'origin'):
        # TODO: split batches that are too large for the storage
        # to handle?
        method = getattr(storage, object_type + '_add')
        method(objects)
    elif object_type == 'origin_visit':
        storage.origin_visit_upsert([
            {
                **obj,
                'origin': storage.origin_add_one(obj['origin'])
            }
            for obj in objects])
    else:
        assert False
