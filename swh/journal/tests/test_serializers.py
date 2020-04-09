# Copyright (C) 2017 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import OrderedDict
import itertools

from swh.journal import serializers

from .conftest import TEST_OBJECTS


def test_key_to_kafka_repeatable():
    """Check the kafka key encoding is repeatable"""
    base_dict = {
        "a": "foo",
        "b": "bar",
        "c": "baz",
    }

    key = serializers.key_to_kafka(base_dict)

    for dict_keys in itertools.permutations(base_dict):
        d = OrderedDict()
        for k in dict_keys:
            d[k] = base_dict[k]

        assert key == serializers.key_to_kafka(d)


def test_get_key():
    """Test whether get_key works on all our objects"""
    for object_type, objects in TEST_OBJECTS.items():
        for obj in objects:
            assert serializers.object_key(object_type, obj) is not None
