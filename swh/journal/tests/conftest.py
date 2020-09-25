# Copyright (C) 2019-2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

from hypothesis.strategies import one_of

# for bw compat
from swh.journal.tests.journal_data import *  # noqa
from swh.model import hypothesis_strategies as strategies

logger = logging.getLogger(__name__)


def objects_d():
    return one_of(
        strategies.origins().map(lambda x: ("origin", x.to_dict())),
        strategies.origin_visits().map(lambda x: ("origin_visit", x.to_dict())),
        strategies.snapshots().map(lambda x: ("snapshot", x.to_dict())),
        strategies.releases().map(lambda x: ("release", x.to_dict())),
        strategies.revisions().map(lambda x: ("revision", x.to_dict())),
        strategies.directories().map(lambda x: ("directory", x.to_dict())),
        strategies.skipped_contents().map(lambda x: ("skipped_content", x.to_dict())),
        strategies.present_contents().map(lambda x: ("content", x.to_dict())),
    )
