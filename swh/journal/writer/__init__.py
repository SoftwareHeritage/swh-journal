# Copyright (C) 2019-2022 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import sys
from typing import Any, BinaryIO, Dict, Type
import warnings

from .interface import JournalWriterInterface


def model_object_dict_sanitizer(
    object_type: str, object_dict: Dict[str, Any]
) -> Dict[str, str]:
    object_dict = object_dict.copy()
    if object_type == "content":
        object_dict.pop("data", None)
    return object_dict


def get_journal_writer(cls, **kwargs) -> JournalWriterInterface:

    if "args" in kwargs:
        warnings.warn(
            'Explicit "args" key is deprecated, use keys directly instead.',
            DeprecationWarning,
        )
        kwargs = kwargs["args"]

    kwargs.setdefault("value_sanitizer", model_object_dict_sanitizer)

    if cls == "inmemory":  # FIXME: Remove inmemory in due time
        warnings.warn(
            "cls = 'inmemory' is deprecated, use 'memory' instead", DeprecationWarning
        )
        cls = "memory"

    JournalWriter: Type[JournalWriterInterface]
    if cls == "memory":
        from .inmemory import InMemoryJournalWriter

        JournalWriter = InMemoryJournalWriter
    elif cls == "kafka":
        from .kafka import KafkaJournalWriter

        JournalWriter = KafkaJournalWriter
    elif cls == "stream":
        from .stream import StreamJournalWriter

        JournalWriter = StreamJournalWriter

        assert "output_stream" in kwargs
        outstream: BinaryIO
        if kwargs["output_stream"] in ("-", b"-"):
            outstream = os.fdopen(sys.stdout.fileno(), "wb", closefd=False)
        elif isinstance(kwargs["output_stream"], (str, bytes)):
            outstream = open(kwargs["output_stream"], "wb")
        else:
            outstream = kwargs["output_stream"]
        kwargs["output_stream"] = outstream
    else:
        raise ValueError("Unknown journal writer class `%s`" % cls)

    return JournalWriter(**kwargs)
