from __future__ import annotations

import csv
from pathlib import Path

import pytest


@pytest.fixture
def write_csv():
    def _write(path: Path, rows: list[dict[str, object]]) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        columns = list(rows[0]) if rows else []
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=columns)
            writer.writeheader()
            writer.writerows(rows)
        return path

    return _write
