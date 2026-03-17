from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any, Mapping, TypeVar

RequestT = TypeVar("RequestT")


def request_from_object(
    request_type: type[RequestT],
    source: Any,
    *,
    rename: Mapping[str, str] | None = None,
    exclude: set[str] | None = None,
) -> RequestT:
    data = _object_to_dict(source)
    for old_name, new_name in (rename or {}).items():
        if old_name in data:
            data[new_name] = data.pop(old_name)
    if exclude:
        for field_name in exclude:
            data.pop(field_name, None)
    return request_type(**data)


def _object_to_dict(source: Any) -> dict[str, Any]:
    if is_dataclass(source):
        return asdict(source)
    if hasattr(source, "model_dump"):
        return dict(source.model_dump())
    if isinstance(source, Mapping):
        return dict(source)
    if hasattr(source, "__dict__"):
        return dict(vars(source))
    raise TypeError(f"Unsupported request source type: {type(source)!r}")
