# Copyright (C) 2016-2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import click
import logging
import os

from swh.core import config
from swh.storage import get_storage

from swh.journal.replay import StorageReplayer
from swh.journal.backfill import JournalBackfiller

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.group(context_settings=CONTEXT_SETTINGS)
@click.option('--config-file', '-C', default=None,
              type=click.Path(exists=True, dir_okay=False,),
              help="Configuration file.")
@click.option('--log-level', '-l', default='INFO',
              type=click.Choice(logging._nameToLevel.keys()),
              help="Log level (default to INFO)")
@click.pass_context
def cli(ctx, config_file, log_level):
    """Software Heritage Scheduler CLI interface

    Default to use the the local scheduler instance (plugged to the
    main scheduler db).

    """
    if not config_file:
        config_file = os.environ.get('SWH_CONFIG_FILENAME')
    if not config_file:
        raise ValueError('You must either pass a config-file parameter '
                         'or set SWH_CONFIG_FILENAME to target '
                         'the config-file')

    if not os.path.exists(config_file):
        raise ValueError('%s does not exist' % config_file)

    conf = config.read(config_file)
    ctx.ensure_object(dict)

    logging.basicConfig(
        level=log_level,
        format='%(asctime)s %(levelname)s %(name)s %(message)s',
    )

    logging.getLogger('kafka').setLevel(logging.INFO)

    ctx.obj['config'] = conf
    ctx.obj['loglevel'] = log_level


@cli.command()
@click.option('--max-messages', '-m', default=None, type=int,
              help='Maximum number of objects to replay. Default is to '
                   'run forever.')
@click.option('--broker', 'brokers', type=str, multiple=True,
              help='Kafka broker to connect to.')
@click.option('--prefix', type=str, default='swh.journal.objects',
              help='Prefix of Kafka topic names to read from.')
@click.option('--consumer-id', type=str,
              help='Name of the consumer/group id for reading from Kafka.')
@click.pass_context
def replay(ctx, brokers, prefix, consumer_id, max_messages):
    """Fill a new storage by reading a journal.

    """
    conf = ctx.obj['config']
    storage = get_storage(**conf.pop('storage'))
    replayer = StorageReplayer(brokers, prefix, consumer_id)
    try:
        replayer.fill(storage, max_messages=max_messages)
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
    """Manipulate backfiller

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
    return cli(auto_envvar_prefix='SWH_JOURNAL')


if __name__ == '__main__':
    main()
