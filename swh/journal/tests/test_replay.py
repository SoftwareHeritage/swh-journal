# Copyright (C) 2019-2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from hypothesis import strategies, given, settings

from swh.journal.replay import is_hash_in_bytearray


storage_config = {"cls": "pipeline", "steps": [{"cls": "memory"},]}


def make_topic(kafka_prefix: str, object_type: str) -> str:
    return kafka_prefix + "." + object_type


hash_strategy = strategies.binary(min_size=20, max_size=20)


@settings(max_examples=500)
@given(
    strategies.sets(hash_strategy, min_size=0, max_size=500),
    strategies.sets(hash_strategy, min_size=10),
)
def test_is_hash_in_bytearray(haystack, needles):
    array = b"".join(sorted(haystack))
    needles |= haystack  # Exhaustively test for all objects in the array
    for needle in needles:
        assert is_hash_in_bytearray(needle, array, len(haystack)) == (
            needle in haystack
        )
