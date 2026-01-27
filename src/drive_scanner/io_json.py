import json
from typing import Any


def load_json(path: str) -> Any:
    with open(path, "rb") as f:
        raw = f.read()
    for encoding in ("utf-8", "utf-8-sig", "utf-16", "utf-16-le", "utf-16-be"):
        try:
            return json.loads(raw.decode(encoding))
        except UnicodeDecodeError:
            continue
        except json.JSONDecodeError:
            continue
    # Final attempt with utf-8 and replacement to surface JSON errors.
    return json.loads(raw.decode("utf-8", errors="replace"))


def write_json(path: str, payload: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
