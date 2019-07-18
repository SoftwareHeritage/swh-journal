# Copyright (C) 2016-2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import click
import functools
import logging
import os

from swh.core import config
from swh.core.cli import CONTEXT_SETTINGS
from swh.storage import get_storage
from swh.objstorage import get_objstorage

from swh.journal.client import JournalClient
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

    log_level = ctx.obj.get('log_level', logging.INFO)
    logging.root.setLevel(log_level)
    logging.getLogger('kafka').setLevel(logging.INFO)

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
@click.option('--max-messages', '-m', default=None, type=int,
              help='Maximum number of objects to replay. Default is to '
                   'run forever.')
@click.option('--broker', 'brokers', type=str, multiple=True,
              help='Kafka broker to connect to. '
                   '(deprecated, use the config file instead)')
@click.option('--prefix', type=str, default=None,
              help='Prefix of Kafka topic names to read from. '
                   '(deprecated, use the config file instead)')
@click.option('--group-id', type=str,
              help='Name of the group id for reading from Kafka. '
                   '(deprecated, use the config file instead)')
@click.pass_context
def replay(ctx, brokers, prefix, group_id, max_messages):
    """Fill a Storage by reading a Journal.

    There can be several 'replayers' filling a Storage as long as they use
    the same `group-id`.
    """
    logger = logging.getLogger(__name__)
    conf = ctx.obj['config']
    try:
        storage = get_storage(**conf.pop('storage'))
    except KeyError:
        ctx.fail('You must have a storage configured in your config file.')

    client = get_journal_client(
        ctx, brokers=brokers, prefix=prefix, group_id=group_id)
    worker_fn = functools.partial(process_replay_objects, storage=storage)

    try:
        nb_messages = 0
        while not max_messages or nb_messages < max_messages:
            nb_messages += client.process(worker_fn)
            logger.info('Processed %d messages.' % nb_messages)
    except KeyboardInterrupt:
        ctx.exit(0)
    else:
        print('Done.')


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
    try:
        backfiller.run(
            object_type=object_type,
            start_object=start_object, end_object=end_object,
            dry_run=dry_run)
    except KeyboardInterrupt:
        ctx.exit(0)


@cli.command()
@click.option('--concurrency', type=int,
              default=8,
              help='Concurrentcy level.')
@click.option('--broker', 'brokers', type=str, multiple=True,
              help='Kafka broker to connect to.'
                   '(deprecated, use the config file instead)')
@click.option('--prefix', type=str, default=None,
              help='Prefix of Kafka topic names to read from.'
                   '(deprecated, use the config file instead)')
@click.option('--group-id', type=str,
              help='Name of the group id for reading from Kafka.'
                   '(deprecated, use the config file instead)')
@click.pass_context
def content_replay(ctx, concurrency, brokers, prefix, group_id):
    """Fill a destination Object Storage (typically a mirror) by reading a Journal
    and retrieving objects from an existing source ObjStorage.

    There can be several 'replayers' filling a given ObjStorage as long as they
    use the same `group-id`.

    This service retrieves object ids to copy from the 'content' topic. It will
    only copy object's content if the object's description in the kafka
    nmessage has the status:visible set.
    """
    logger = logging.getLogger(__name__)
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

    client = get_journal_client(
        ctx, brokers=brokers, prefix=prefix, group_id=group_id,
        object_types=('content',))
    worker_fn = functools.partial(process_replay_objects_content,
                                  src=objstorage_src,
                                  dst=objstorage_dst,
                                  concurrency=concurrency)

    try:
        nb_messages = 0
        while True:
            nb_messages += client.process(worker_fn)
            logger.info('Processed %d messages.' % nb_messages)
    except KeyboardInterrupt:
        ctx.exit(0)
    else:
        logger.info('Done.')


def main():
    logging.basicConfig()
    return cli(auto_envvar_prefix='SWH_JOURNAL')


if __name__ == '__main__':
    main()
