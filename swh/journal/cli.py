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

from swh.journal.client import JournalClient
from swh.journal.replay import process_replay_objects
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


@cli.command()
@click.option('--max-messages', '-m', default=None, type=int,
              help='Maximum number of objects to replay. Default is to '
                   'run forever.')
@click.option('--broker', 'brokers', type=str, multiple=True,
              hidden=True,  # prefer config file
              help='Kafka broker to connect to.')
@click.option('--prefix', type=str, default=None,
              hidden=True,  # prefer config file
              help='Prefix of Kafka topic names to read from.')
@click.option('--group-id', '--consumer-id', type=str,
              hidden=True,  # prefer config file
              help='Name of the consumer/group id for reading from Kafka.')
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

    if brokers is None:
        brokers = conf.get('journal', {}).get('brokers')
    if not brokers:
        ctx.fail('You must specify at least one kafka broker.')
    if not isinstance(brokers, (list, tuple)):
        brokers = [brokers]

    if prefix is None:
        prefix = conf.get('journal', {}).get('prefix')

    if group_id is None:
        group_id = conf.get('journal', {}).get('group_id')

    client = JournalClient(brokers=brokers, group_id=group_id, prefix=prefix)
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


def main():
    logging.basicConfig()
    return cli(auto_envvar_prefix='SWH_JOURNAL')


if __name__ == '__main__':
    main()
