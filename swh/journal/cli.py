# Copyright (C) 2016-2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import click
import logging
import os

from swh.core import config
from swh.journal.publisher import JournalPublisher

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

    logger = logging.getLogger(__name__)
    logger.setLevel(log_level)

    _log = logging.getLogger('kafka')
    _log.setLevel(logging.INFO)

    ctx.obj['config'] = conf
    ctx.obj['loglevel'] = log_level


@cli.command()
@click.pass_context
def publisher(ctx):
    """Manipulate publisher

    """
    conf = ctx.obj['config']
    publisher = JournalPublisher(conf)
    try:
        while True:
            publisher.poll()
    except KeyboardInterrupt:
        ctx.exit(0)


def main():
    return cli(auto_envvar_prefix='SWH_JOURNAL')


if __name__ == '__main__':
    main()
