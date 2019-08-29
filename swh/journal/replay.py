# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from time import time
import logging
from contextlib import contextmanager

from swh.core.statsd import statsd
from swh.model.identifiers import normalize_timestamp
from swh.model.hashutil import hash_to_hex
from swh.model.model import SHA1_SIZE
from swh.objstorage.objstorage import ID_HASH_ALGO
from swh.storage import HashCollision

logger = logging.getLogger(__name__)


def process_replay_objects(all_objects, *, storage):
    for (object_type, objects) in all_objects.items():
        logger.debug("Inserting %s %s objects", len(objects), object_type)
        _insert_objects(object_type, objects, storage)


def _fix_revision_pypi_empty_string(rev):
    """PyPI loader failed to encode empty strings as bytes, see:
    swh:1:rev:8f0095ee0664867055d03de9bcc8f95b91d8a2b9
    or https://forge.softwareheritage.org/D1772
    """
    rev = {
        **rev,
        'author': rev['author'].copy(),
        'committer': rev['committer'].copy(),
    }
    if rev['author'].get('email') == '':
        rev['author']['email'] = b''
    if rev['author'].get('name') == '':
        rev['author']['name'] = b''
    if rev['committer'].get('email') == '':
        rev['committer']['email'] = b''
    if rev['committer'].get('name') == '':
        rev['committer']['name'] = b''
    return rev


def _check_date(date):
    """Returns whether the date can be represented in backends with sane
    limits on timestamps and timezeones (resp. signed 64-bits and
    signed 16 bits), and that microseconds is valid (ie. between 0 and 10^6).
    """
    date = normalize_timestamp(date)
    return (-2**63 <= date['timestamp']['seconds'] < 2**63) \
        and (0 <= date['timestamp']['microseconds'] < 10**6) \
        and (-2**15 <= date['offset'] < 2**15)


def _check_revision_date(rev):
    """Exclude revisions with invalid dates.
    See https://forge.softwareheritage.org/T1339"""
    return _check_date(rev['date']) and _check_date(rev['committer_date'])


def _fix_revisions(revisions):
    good_revisions = []
    for rev in revisions:
        rev = _fix_revision_pypi_empty_string(rev)
        if not _check_revision_date(rev):
            logging.warning('Excluding revision (invalid date): %r', rev)
            continue
        good_revisions.append(rev)
    return good_revisions


def _fix_origin_visits(visits):
    good_visits = []
    for visit in visits:
        visit = visit.copy()
        if isinstance(visit['origin'], str):
            # note that it will crash with the pg and
            # in-mem storages if the origin is not already known,
            # but there is no other choice because we can't add an
            # origin without knowing its type. Non-pg storages
            # don't use a numeric FK internally,
            visit['origin'] = {'url': visit['origin']}
        else:
            if 'type' not in visit:
                visit['type'] = visit['origin']['type']
        good_visits.append(visit)
    return good_visits


def fix_objects(object_type, objects):
    """Converts a possibly old object from the journal to its current
    expected format.

    List of conversions:

    Empty author name/email in PyPI releases:

    >>> from pprint import pprint
    >>> date = {
    ...     'timestamp': {
    ...         'seconds': 1565096932,
    ...         'microseconds': 0,
    ...     },
    ...     'offset': 0,
    ... }
    >>> pprint(fix_objects('revision', [{
    ...     'author': {'email': '', 'fullname': b'', 'name': ''},
    ...     'committer': {'email': '', 'fullname': b'', 'name': ''},
    ...     'date': date,
    ...     'committer_date': date,
    ... }]))
    [{'author': {'email': b'', 'fullname': b'', 'name': b''},
      'committer': {'email': b'', 'fullname': b'', 'name': b''},
      'committer_date': {'offset': 0,
                         'timestamp': {'microseconds': 0, 'seconds': 1565096932}},
      'date': {'offset': 0,
               'timestamp': {'microseconds': 0, 'seconds': 1565096932}}}]

    Filter out revisions with invalid dates:

    >>> from copy import deepcopy
    >>> invalid_date1 = deepcopy(date)
    >>> invalid_date1['timestamp']['microseconds'] = 1000000000  # > 10^6
    >>> fix_objects('revision', [{
    ...     'author': {'email': '', 'fullname': b'', 'name': b''},
    ...     'committer': {'email': '', 'fullname': b'', 'name': b''},
    ...     'date': invalid_date1,
    ...     'committer_date': date,
    ... }])
    []

    >>> invalid_date2 = deepcopy(date)
    >>> invalid_date2['timestamp']['seconds'] = 2**70  # > 10^63
    >>> fix_objects('revision', [{
    ...     'author': {'email': '', 'fullname': b'', 'name': b''},
    ...     'committer': {'email': '', 'fullname': b'', 'name': b''},
    ...     'date': invalid_date2,
    ...     'committer_date': date,
    ... }])
    []

    >>> invalid_date3 = deepcopy(date)
    >>> invalid_date3['offset'] = 2**20  # > 10^15
    >>> fix_objects('revision', [{
    ...     'author': {'email': '', 'fullname': b'', 'name': b''},
    ...     'committer': {'email': '', 'fullname': b'', 'name': b''},
    ...     'date': date,
    ...     'committer_date': invalid_date3,
    ... }])
    []


    `visit['origin']` is an URL instead of a dict:

    >>> fix_objects('origin_visit', [{'origin': 'http://foo'}])
    [{'origin': {'url': 'http://foo'}}]

    `visit['type']` is missing , but `origin['visit']['type']` exists:

    >>> pprint(fix_objects(
    ...     'origin_visit',
    ...     [{'origin': {'type': 'hg', 'url': 'http://foo'}}]))
    [{'origin': {'type': 'hg', 'url': 'http://foo'}, 'type': 'hg'}]
    """  # noqa

    if object_type == 'revision':
        objects = _fix_revisions(objects)
    elif object_type == 'origin_visit':
        objects = _fix_origin_visits(objects)
    return objects


def _insert_objects(object_type, objects, storage):
    objects = fix_objects(object_type, objects)

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
            if 'type' in visit['origin']:
                storage.origin_add_one(visit['origin'])
            if 'metadata' not in visit:
                visit['metadata'] = None
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


@contextmanager
def retry(max_retries):
    lasterror = None
    for i in range(max_retries):
        try:
            yield
            break
        except Exception as exc:
            lasterror = exc
    else:
        raise lasterror


def copy_object(obj_id, src, dst, max_retries=3):
    statsd_name = 'swh_journal_content_replayer_%s_duration_seconds'
    try:
        with statsd.timed(statsd_name % 'get'):
            with retry(max_retries):
                obj = src.get(obj_id)
                logger.debug('retrieved %s', hash_to_hex(obj_id))

        with statsd.timed(statsd_name % 'put'):
            with retry(max_retries):
                dst.add(obj, obj_id=obj_id, check_presence=False)
                logger.debug('copied %s', hash_to_hex(obj_id))
        statsd.increment(
            'swh_journal_content_replayer_bytes_total',
            len(obj))
    except Exception:
        obj = ''
        logger.error('Failed to copy %s', hash_to_hex(obj_id))
        raise
    return len(obj)


def process_replay_objects_content(all_objects, *, src, dst,
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
                vol.append(copy_object(obj_id, src, dst))

    dt = time() - t0
    logger.info(
        'processed %s content objects in %.1fsec '
        '(%.1f obj/sec, %.1fMB/sec) - %d failures - %d skipped',
        len(vol), dt,
        len(vol)/dt,
        sum(vol)/1024/1024/dt,
        len([x for x in vol if not x]),
        nb_skipped)
