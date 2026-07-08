from __future__ import annotations


def optional_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    return int(value)


__all__ = ["optional_int"]
