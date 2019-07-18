# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import functools

from hypothesis import given, settings, HealthCheck
from hypothesis.strategies import lists

from swh.model.hypothesis_strategies import object_dicts
from swh.storage.in_memory import Storage
from swh.storage import HashCollision

from swh.journal.replay import process_replay_objects
from swh.journal.replay import process_replay_objects_content

from .utils import MockedJournalClient, MockedKafkaWriter


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
            origin_id = storage1.origin_add_one(obj.pop('origin'))
            if 'visit' in obj:
                del obj['visit']
            storage1.origin_visit_add(origin_id, **obj)
        else:
            method = getattr(storage1, obj_type + '_add')
            try:
                method([obj])
            except HashCollision:
                pass

    queue_size = sum(len(partition)
                     for batch in queue
                     for partition in batch.values())

    storage2 = Storage()
    worker_fn = functools.partial(process_replay_objects, storage=storage2)
    nb_messages = 0
    while nb_messages < queue_size:
        nb_messages += replayer.process(worker_fn)

    assert replayer.consumer.committed

    for attr_name in ('_contents', '_directories', '_revisions', '_releases',
                      '_snapshots', '_origin_visits', '_origins'):
        assert getattr(storage1, attr_name) == getattr(storage2, attr_name), \
            attr_name


# TODO: add test for hash collision


@given(lists(object_dicts(), min_size=1))
@settings(suppress_health_check=[HealthCheck.too_slow])
def test_write_replay_content(objects):
    queue = []
    replayer = MockedJournalClient(queue)

    storage1 = Storage()
    storage1.journal_writer = MockedKafkaWriter(queue)

    for (obj_type, obj) in objects:
        obj = obj.copy()
        if obj_type == 'content':
            storage1.content_add([obj])

    queue_size = sum(len(partition)
                     for batch in queue
                     for partition in batch.values())

    storage2 = Storage()
    worker_fn = functools.partial(process_replay_objects_content,
                                  src=storage1.objstorage,
                                  dst=storage2.objstorage)
    nb_messages = 0
    while nb_messages < queue_size:
        nb_messages += replayer.process(worker_fn)

    assert storage1.objstorage.state == storage2.objstorage.state
