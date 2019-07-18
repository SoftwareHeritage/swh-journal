# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from time import time
import logging
from concurrent.futures import ThreadPoolExecutor

from swh.storage import HashCollision
from swh.model.hashutil import hash_to_hex
from swh.objstorage.objstorage import ID_HASH_ALGO
from swh.core.statsd import statsd

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
        for visit in objects:
            if isinstance(visit['origin'], str):
                # old format; note that it will crash with the pg and
                # in-mem storages if the origin is not already known,
                # but there is no other choice because we can't add an
                # origin without knowing its type. Non-pg storages
                # don't use a numeric FK internally,
                visit['origin'] = {'url': visit['origin']}
            else:
                storage.origin_add_one(visit['origin'])
                if 'type' not in visit:
                    # old format
                    visit['type'] = visit['origin']['type']

        storage.origin_visit_upsert(objects)
    else:
        logger.warning('Received a series of %s, this should not happen',
                       object_type)


def copy_object(obj_id, src, dst):
    statsd_name = 'swh_journal_content_replayer_%s_duration_seconds'
    try:
        with statsd.timed(statsd_name % 'get'):
            obj = src.get(obj_id)
        with statsd.timed(statsd_name % 'put'):
            dst.add(obj, obj_id=obj_id, check_presence=False)
            logger.debug('copied %s', hash_to_hex(obj_id))
        statsd.increment(
            'swh_journal_content_replayer_bytes_total',
            len(obj))
    except Exception:
        obj = ''
        logger.exception('Failed to copy %s', hash_to_hex(obj_id))
    return len(obj)


def process_replay_objects_content(all_objects, *, src, dst, concurrency=8):
    vol = []
    t0 = time()
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        for (object_type, objects) in all_objects.items():
            if object_type != 'content':
                logger.warning(
                    'Received a series of %s, this should not happen',
                    object_type)
                continue
            for obj in objects:
                obj_id = obj[ID_HASH_ALGO]
                if obj['status'] == 'visible':
                    fut = executor.submit(copy_object, obj_id, src, dst)
                    fut.add_done_callback(lambda fn: vol.append(fn.result()))
                else:
                    logger.debug('skipped %s (%s)',
                                 hash_to_hex(obj_id), obj['status'])
    dt = time() - t0
    logger.info(
        'processed %s content objects in %.1fsec '
        '(%.1f obj/sec, %.1fMB/sec) - %s failures',
        len(vol), dt,
        len(vol)/dt,
        sum(vol)/1024/1024/dt, len([x for x in vol if not x]))
