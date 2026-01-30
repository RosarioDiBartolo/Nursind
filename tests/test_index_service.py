from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from drive_scripts.index_service import Index  # noqa: E402


def test_load_index_missing_returns_empty(tmp_path):
    missing = tmp_path / "missing.index.json"
    index = Index.load_index(str(missing), strict=False)
    assert index.employee_count == 0
    assert index.files == []
    assert index.generated_at


def test_load_index_missing_strict_raises(tmp_path):
    missing = tmp_path / "missing.index.json"
    with pytest.raises(RuntimeError):
        Index.load_index(str(missing), strict=True)
