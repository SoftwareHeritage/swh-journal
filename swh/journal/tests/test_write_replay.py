# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import functools

import attr
from hypothesis import given, settings, HealthCheck
from hypothesis.strategies import lists

from swh.model.hypothesis_strategies import object_dicts
from swh.storage.in_memory import Storage
from swh.storage import HashCollision

from swh.journal.replay import process_replay_objects
from swh.journal.replay import process_replay_objects_content

from .utils import MockedJournalClient, MockedKafkaWriter


def empty_person_name_email(rev_or_rel):
    """Empties the 'name' and 'email' fields of the author/committer fields
    of a revision or release; leaving only the fullname."""
    if getattr(rev_or_rel, 'author', None):
        rev_or_rel = attr.evolve(
            rev_or_rel,
            author=attr.evolve(
                rev_or_rel.author,
                name=b'',
                email=b'',
            )
        )

    if getattr(rev_or_rel, 'committer', None):
        rev_or_rel = attr.evolve(
            rev_or_rel,
            committer=attr.evolve(
                rev_or_rel.committer,
                name=b'',
                email=b'',
            )
        )

    return rev_or_rel


@given(lists(object_dicts(), min_size=1))
@settings(suppress_health_check=[HealthCheck.too_slow])
def test_write_replay_same_order_batches(objects):
    queue = []
    replayer = MockedJournalClient(queue)

    storage1 = Storage()
    storage1.journal_writer = MockedKafkaWriter(queue)

    for (obj_type, obj) in objects:
        obj = obj.copy()
        if obj_type == 'origin_visit':
            storage1.origin_add_one({'url': obj['origin']})
            storage1.origin_visit_upsert([obj])
        else:
            method = getattr(storage1, obj_type + '_add')
            try:
                method([obj])
            except HashCollision:
                pass

    queue_size = len(queue)
    assert replayer.max_messages == 0
    replayer.max_messages = queue_size

    storage2 = Storage()
    worker_fn = functools.partial(process_replay_objects, storage=storage2)
    nb_messages = 0
    while nb_messages < queue_size:
        nb_messages += replayer.process(worker_fn)

    assert replayer.consumer.committed

    for attr_name in ('_contents', '_directories',
                      '_snapshots', '_origin_visits', '_origins'):
        assert getattr(storage1, attr_name) == getattr(storage2, attr_name), \
            attr_name

    # When hypothesis generates a revision and a release with same
    # author (or committer) fullname but different name or email, then
    # the storage will use the first name/email it sees.
    # This first one will be either the one from the revision or the release,
    # and since there is no order guarantees, storage2 has 1/2 chance of
    # not seeing the same order as storage1, therefore we need to strip
    # them out before comparing.
    for attr_name in ('_revisions', '_releases'):
        items1 = {k: empty_person_name_email(v)
                  for (k, v) in getattr(storage1, attr_name).items()}
        items2 = {k: empty_person_name_email(v)
                  for (k, v) in getattr(storage2, attr_name).items()}
        assert items1 == items2, attr_name


# TODO: add test for hash collision


@given(lists(object_dicts(), min_size=1))
@settings(suppress_health_check=[HealthCheck.too_slow])
def test_write_replay_content(objects):

    queue = []
    replayer = MockedJournalClient(queue)

    storage1 = Storage()
    storage1.journal_writer = MockedKafkaWriter(queue)

    contents = []
    for (obj_type, obj) in objects:
        obj = obj.copy()
        if obj_type == 'content':
            # avoid hash collision
            if not storage1.content_find(obj):
                storage1.content_add([obj])
                contents.append(obj)

    queue_size = len(queue)
    assert replayer.max_messages == 0
    replayer.max_messages = queue_size

    storage2 = Storage()
    worker_fn = functools.partial(process_replay_objects_content,
                                  src=storage1.objstorage,
                                  dst=storage2.objstorage)
    nb_messages = 0
    while nb_messages < queue_size:
        nb_messages += replayer.process(worker_fn)

    # only content with status visible will be copied in storage2
    expected_objstorage_state = {
        c['sha1']: c['data'] for c in contents if c['status'] == 'visible'
    }

    assert expected_objstorage_state == storage2.objstorage.state
