# Copyright (C) 2019-2021 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

# deprecated, for BW compat only

import warnings

from swh.model.tests.swh_model_data import DUPLICATE_CONTENTS, TEST_OBJECTS  # noqa

warnings.warn(
    "This module is deprecated, please use swh.model.tests.swh_model_data instead",
    category=DeprecationWarning,
)
