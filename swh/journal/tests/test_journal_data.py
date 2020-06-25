# Copyright (C) 2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.journal.tests.journal_data import TEST_OBJECTS


def test_ensure_visit_visit_status_date_consistency():
    """ensure origin-visit-status dates are more recent than their visit counterpart

    The origin-visit-status dates needs to be shifted slightly in the future from their
    visit dates counterpart. Otherwise, we are hitting storage-wise the "on conflict"
    ignore policy (because origin-visit-add creates an origin-visit-status with the same
    parameters from the origin-visit {origin, visit, date}...

    """
    visits = TEST_OBJECTS["origin_visit"]
    visit_statuses = TEST_OBJECTS["origin_visit_status"]
    for visit, visit_status in zip(visits, visit_statuses):
        assert visit.origin == visit_status.origin
        assert visit.visit == visit_status.visit
        assert visit.date < visit_status.date
