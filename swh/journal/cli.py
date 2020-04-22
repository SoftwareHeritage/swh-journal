# Copyright (C) 2016-2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

import click

from swh.core.cli import CONTEXT_SETTINGS


@click.group(name="journal", context_settings=CONTEXT_SETTINGS)
@click.option(
    "--config-file",
    "-C",
    default=None,
    type=click.Path(exists=True, dir_okay=False,),
    help="Configuration file.",
)
@click.pass_context
def cli(ctx, config_file):
    """DEPRECATED Software Heritage Journal tools.
    """
    pass


@cli.command()
@click.option(
    "--stop-after-objects",
    "-n",
    default=None,
    type=int,
    help="Stop after processing this many objects. Default is to " "run forever.",
)
@click.pass_context
def replay(ctx, stop_after_objects):
    """DEPRECATED: use `swh storage replay` instead.

    Requires swh.storage >= 0.0.188.
    """
    ctx.fail("DEPRECATED")


@cli.command()
@click.argument("object_type")
@click.option("--start-object", default=None)
@click.option("--end-object", default=None)
@click.option("--dry-run", is_flag=True, default=False)
@click.pass_context
def backfiller(ctx, object_type, start_object, end_object, dry_run):
    """DEPRECATED: use `swh storage backfill` instead.

    Requires swh.storage >= 0.0.188.

    """
    ctx.fail("DEPRECATED")


@cli.command("content-replay")
@click.option(
    "--stop-after-objects",
    "-n",
    default=None,
    type=int,
    help="Stop after processing this many objects. Default is to " "run forever.",
)
@click.option(
    "--exclude-sha1-file",
    default=None,
    type=click.File("rb"),
    help="File containing a sorted array of hashes to be excluded.",
)
@click.option(
    "--check-dst/--no-check-dst",
    default=True,
    help="Check whether the destination contains the object before " "copying.",
)
@click.pass_context
def content_replay(ctx, stop_after_objects, exclude_sha1_file, check_dst):
    """DEPRECATED: use `swh objstorage replay` instead.

    This needs the swh.objstorage.replayer package."""
    ctx.fail("DEPRECATED")


def main():
    logging.basicConfig()
    return cli(auto_envvar_prefix="SWH_JOURNAL")


if __name__ == "__main__":
    main()
