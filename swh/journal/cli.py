# Copyright (C) 2016-2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import functools
import logging
import mmap
import os

import click

try:
    from systemd.daemon import notify
except ImportError:
    notify = None

from swh.core import config
from swh.core.cli import CONTEXT_SETTINGS
from swh.model.model import SHA1_SIZE
from swh.storage import get_storage
from swh.objstorage import get_objstorage

from swh.journal.client import JournalClient
from swh.journal.replay import is_hash_in_bytearray
from swh.journal.replay import process_replay_objects
from swh.journal.replay import process_replay_objects_content
from swh.journal.backfill import JournalBackfiller


@click.group(name='journal', context_settings=CONTEXT_SETTINGS)
@click.option('--config-file', '-C', default=None,
              type=click.Path(exists=True, dir_okay=False,),
              help="Configuration file.")
@click.pass_context
def cli(ctx, config_file):
    """Software Heritage Journal tools.

    The journal is a persistent logger of changes to the archive, with
    publish-subscribe support.

    """
    if not config_file:
        config_file = os.environ.get('SWH_CONFIG_FILENAME')

    if config_file:
        if not os.path.exists(config_file):
            raise ValueError('%s does not exist' % config_file)
        conf = config.read(config_file)
    else:
        conf = {}

    ctx.ensure_object(dict)

    ctx.obj['config'] = conf


def get_journal_client(ctx, **kwargs):
    conf = ctx.obj['config'].get('journal', {})
    conf.update({k: v for (k, v) in kwargs.items() if v not in (None, ())})
    if not conf.get('brokers'):
        ctx.fail('You must specify at least one kafka broker.')
    if not isinstance(conf['brokers'], (list, tuple)):
        conf['brokers'] = [conf['brokers']]
    return JournalClient(**conf)


@cli.command()
@click.option('--stop-after-objects', '-n', default=None, type=int,
              help='Stop after processing this many objects. Default is to '
                   'run forever.')
@click.pass_context
def replay(ctx, stop_after_objects):
    """Fill a Storage by reading a Journal.

    There can be several 'replayers' filling a Storage as long as they use
    the same `group-id`.
    """
    conf = ctx.obj['config']
    try:
        storage = get_storage(**conf.pop('storage'))
    except KeyError:
        ctx.fail('You must have a storage configured in your config file.')

    client = get_journal_client(
        ctx, stop_after_objects=stop_after_objects)
    worker_fn = functools.partial(process_replay_objects, storage=storage)

    if notify:
        notify('READY=1')

    try:
        client.process(worker_fn)
    except KeyboardInterrupt:
        ctx.exit(0)
    else:
        print('Done.')
    finally:
        if notify:
            notify('STOPPING=1')
        client.close()


@cli.command()
@click.argument('object_type')
@click.option('--start-object', default=None)
@click.option('--end-object', default=None)
@click.option('--dry-run', is_flag=True, default=False)
@click.pass_context
def backfiller(ctx, object_type, start_object, end_object, dry_run):
    """Run the backfiller

    The backfiller list objects from a Storage and produce journal entries from
    there.

    Typically used to rebuild a journal or compensate for missing objects in a
    journal (eg. due to a downtime of this later).

    The configuration file requires the following entries:
    - brokers: a list of kafka endpoints (the journal) in which entries will be
               added.
    - storage_dbconn: URL to connect to the storage DB.
    - prefix: the prefix of the topics (topics will be <prefix>.<object_type>).
    - client_id: the kafka client ID.

    """
    conf = ctx.obj['config']
    backfiller = JournalBackfiller(conf)

    if notify:
        notify('READY=1')

    try:
        backfiller.run(
            object_type=object_type,
            start_object=start_object, end_object=end_object,
            dry_run=dry_run)
    except KeyboardInterrupt:
        if notify:
            notify('STOPPING=1')
        ctx.exit(0)


@cli.command('content-replay')
@click.option('--stop-after-objects', '-n', default=None, type=int,
              help='Stop after processing this many objects. Default is to '
                   'run forever.')
@click.option('--exclude-sha1-file', default=None, type=click.File('rb'),
              help='File containing a sorted array of hashes to be excluded.')
@click.option('--check-dst/--no-check-dst', default=True,
              help='Check whether the destination contains the object before '
                   'copying.')
@click.pass_context
def content_replay(ctx, stop_after_objects, exclude_sha1_file, check_dst):
    """Fill a destination Object Storage (typically a mirror) by reading a Journal
    and retrieving objects from an existing source ObjStorage.

    There can be several 'replayers' filling a given ObjStorage as long as they
    use the same `group-id`. You can use the `KAFKA_GROUP_INSTANCE_ID`
    environment variable to use KIP-345 static group membership.

    This service retrieves object ids to copy from the 'content' topic. It will
    only copy object's content if the object's description in the kafka
    nmessage has the status:visible set.

    `--exclude-sha1-file` may be used to exclude some hashes to speed-up the
    replay in case many of the contents are already in the destination
    objstorage. It must contain a concatenation of all (sha1) hashes,
    and it must be sorted.
    This file will not be fully loaded into memory at any given time,
    so it can be arbitrarily large.

    `--check-dst` sets whether the replayer should check in the destination
    ObjStorage before copying an object. You can turn that off if you know
    you're copying to an empty ObjStorage.
    """
    conf = ctx.obj['config']
    try:
        objstorage_src = get_objstorage(**conf.pop('objstorage_src'))
    except KeyError:
        ctx.fail('You must have a source objstorage configured in '
                 'your config file.')
    try:
        objstorage_dst = get_objstorage(**conf.pop('objstorage_dst'))
    except KeyError:
        ctx.fail('You must have a destination objstorage configured '
                 'in your config file.')

    if exclude_sha1_file:
        map_ = mmap.mmap(exclude_sha1_file.fileno(), 0, prot=mmap.PROT_READ)
        if map_.size() % SHA1_SIZE != 0:
            ctx.fail('--exclude-sha1 must link to a file whose size is an '
                     'exact multiple of %d bytes.' % SHA1_SIZE)
        nb_excluded_hashes = int(map_.size()/SHA1_SIZE)

        def exclude_fn(obj):
            return is_hash_in_bytearray(obj['sha1'], map_, nb_excluded_hashes)
    else:
        exclude_fn = None

    client = get_journal_client(
        ctx, stop_after_objects=stop_after_objects, object_types=('content',))
    worker_fn = functools.partial(
        process_replay_objects_content,
        src=objstorage_src, dst=objstorage_dst, exclude_fn=exclude_fn,
        check_dst=check_dst)

    if notify:
        notify('READY=1')

    try:
        client.process(worker_fn)
    except KeyboardInterrupt:
        ctx.exit(0)
    else:
        print('Done.')
    finally:
        if notify:
            notify('STOPPING=1')
        client.close()


def main():
    logging.basicConfig()
    return cli(auto_envvar_prefix='SWH_JOURNAL')


if __name__ == '__main__':
    main()
