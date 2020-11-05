# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, Dict, Optional, TypeVar
import warnings

from typing_extensions import Protocol

from swh.model.model import KeyType

TSelf = TypeVar("TSelf")


class ValueProtocol(Protocol):
    def anonymize(self: TSelf) -> Optional[TSelf]:
        ...

    def unique_key(self) -> KeyType:
        ...

    def to_dict(self) -> Dict[str, Any]:
        ...


def model_object_dict_sanitizer(
    object_type: str, object_dict: Dict[str, Any]
) -> Dict[str, str]:
    object_dict = object_dict.copy()
    if object_type == "content":
        object_dict.pop("data", None)
    return object_dict


def get_journal_writer(cls, **kwargs):
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
    if cls == "memory":
        from .inmemory import InMemoryJournalWriter as JournalWriter
    elif cls == "kafka":
        from .kafka import KafkaJournalWriter as JournalWriter
    else:
        raise ValueError("Unknown journal writer class `%s`" % cls)

    return JournalWriter(**kwargs)
