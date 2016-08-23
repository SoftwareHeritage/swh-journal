# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import msgpack


def value_to_kafka(value):
    """Serialize some data for storage in kafka"""
    return msgpack.dumps(value, use_bin_type=True)


def kafka_to_value(kafka_value):
    """Deserialize some data stored in kafka"""
    return msgpack.loads(kafka_value)
