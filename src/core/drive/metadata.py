from __future__ import annotations

from typing import Any


def get_root_name(drive, root_id: str | None, logger_obj: Any = None) -> str | None:
    """Return Drive root display name when available."""
    if not root_id:
        return None
    try:
        res = drive.files().get(
            fileId=root_id, fields="name", supportsAllDrives=True
        ).execute()
    except Exception as exc:
        if logger_obj is not None:
            logger_obj.warning("Unable to resolve root folder name: %s", exc)
        return None
    return res.get("name") or None

