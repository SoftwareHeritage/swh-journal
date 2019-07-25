# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from time import time
import logging
from concurrent.futures import ThreadPoolExecutor

from swh.core.statsd import statsd
from swh.model.hashutil import hash_to_hex
from swh.model.model import SHA1_SIZE
from swh.objstorage.objstorage import ID_HASH_ALGO
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


def is_hash_in_bytearray(hash_, array, nb_hashes, hash_size=SHA1_SIZE):
    """
    Checks if the given hash is in the provided `array`. The array must be
    a *sorted* list of sha1 hashes, and contain `nb_hashes` hashes
    (so its size must by `nb_hashes*hash_size` bytes).

    Args:
        hash_ (bytes): the hash to look for
        array (bytes): a sorted concatenated array of hashes (may be of
            any type supporting slice indexing, eg. :py:cls:`mmap.mmap`)
        nb_hashes (int): number of hashes in the array
        hash_size (int): size of a hash (defaults to 20, for SHA1)

    Example:

    >>> import os
    >>> hash1 = os.urandom(20)
    >>> hash2 = os.urandom(20)
    >>> hash3 = os.urandom(20)
    >>> array = b''.join(sorted([hash1, hash2]))
    >>> is_hash_in_bytearray(hash1, array, 2)
    True
    >>> is_hash_in_bytearray(hash2, array, 2)
    True
    >>> is_hash_in_bytearray(hash3, array, 2)
    False
    """
    if len(hash_) != hash_size:
        raise ValueError('hash_ does not match the provided hash_size.')

    def get_hash(position):
        return array[position*hash_size:(position+1)*hash_size]

    # Regular dichotomy:
    left = 0
    right = nb_hashes
    while left < right-1:
        middle = int((right+left)/2)
        pivot = get_hash(middle)
        if pivot == hash_:
            return True
        elif pivot < hash_:
            left = middle
        else:
            right = middle
    return get_hash(left) == hash_


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


def process_replay_objects_content(all_objects, *, src, dst, concurrency=8,
                                   exclude_fn=None):
    """
    Takes a list of records from Kafka (see
    :py:func:`swh.journal.client.JournalClient.process`) and copies them
    from the `src` objstorage to the `dst` objstorage, if:

    * `obj['status']` is `'visible'`
    * `exclude_fn(obj)` is `False` (if `exclude_fn` is provided)

    Args:
        all_objects Dict[str, List[dict]]: Objects passed by the Kafka client.
            Most importantly, `all_objects['content'][*]['sha1']` is the
            sha1 hash of each content
        src: An object storage (see :py:func:`swh.objstorage.get_objstorage`)
        dst: An object storage (see :py:func:`swh.objstorage.get_objstorage`)
        exclude_fn Optional[Callable[dict, bool]]: Determines whether
            an object should be copied.

    Example:

    >>> from swh.objstorage import get_objstorage
    >>> src = get_objstorage('memory', {})
    >>> dst = get_objstorage('memory', {})
    >>> id1 = src.add(b'foo bar')
    >>> id2 = src.add(b'baz qux')
    >>> kafka_partitions = {
    ...     'content': [
    ...         {
    ...             'sha1': id1,
    ...             'status': 'visible',
    ...         },
    ...         {
    ...             'sha1': id2,
    ...             'status': 'visible',
    ...         },
    ...     ]
    ... }
    >>> process_replay_objects_content(
    ...     kafka_partitions, src=src, dst=dst,
    ...     exclude_fn=lambda obj: obj['sha1'] == id1)
    >>> id1 in dst
    False
    >>> id2 in dst
    True
    """
    vol = []
    nb_skipped = 0
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
                if obj['status'] != 'visible':
                    nb_skipped += 1
                    logger.debug('skipped %s (status=%s)',
                                 hash_to_hex(obj_id), obj['status'])
                elif exclude_fn and exclude_fn(obj):
                    nb_skipped += 1
                    logger.debug('skipped %s (manually excluded)',
                                 hash_to_hex(obj_id))
                else:
                    fut = executor.submit(copy_object, obj_id, src, dst)
                    fut.add_done_callback(lambda fn: vol.append(fn.result()))
    dt = time() - t0
    logger.info(
        'processed %s content objects in %.1fsec '
        '(%.1f obj/sec, %.1fMB/sec) - %d failures - %d skipped',
        len(vol), dt,
        len(vol)/dt,
        sum(vol)/1024/1024/dt,
        len([x for x in vol if not x]),
        nb_skipped)
