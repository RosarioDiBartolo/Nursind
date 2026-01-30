import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from drive_scripts.download_index import _build_output_path, _is_pdf, download_index_files  # noqa: E402
from drive_scripts.index_service import extract_index_files  # noqa: E402


def test_extract_index_files_current_schema():
    data = {
        "files": [
            {"employee": "a", "file_id": "id1", "file_name": "x.pdf"},
            {"employee": "b", "file_id": "id2", "file_name": "y.pdf"},
        ]
    }
    items = extract_index_files(data)
    assert [item["file_id"] for item in items] == ["id1", "id2"]


def test_extract_index_files_missing_files_key():
    data = {"included": [{"employee": "alpha", "files": [{"file_id": "id1"}]}]}
    try:
        extract_index_files(data)
    except ValueError as exc:
        assert "files" in str(exc)
    else:
        raise AssertionError("Expected ValueError for missing files array")


def test_build_output_path():
    out_dir = os.path.join("downloads")
    path = _build_output_path(out_dir, "Mario Rossi", "abc1234567", "Report 01.pdf", flat=False)
    assert path.endswith(os.path.join("Mario Rossi", "Report 01__abc12345.pdf"))


def test_is_pdf_filters():
    assert _is_pdf({"mimeType": "application/pdf", "file_name": "a.pdf"}) is True
    assert _is_pdf({"mimeType": "application/zip", "file_name": "a.zip", "container": "zip"}) is False
    assert _is_pdf({"mimeType": "text/plain", "file_name": "a.pdf"}) is False
    assert _is_pdf({"mimeType": "application/pdf", "file_name": "a.txt"}) is False


def test_download_index_files_dry_run(tmp_path):
    index_path = tmp_path / "index.json"
    data = {
        "files": [
            {"employee": "a", "file_id": "id1", "file_name": "doc.pdf"},
            {"employee": "b", "file_id": None, "file_name": "missing.pdf"},
            {"employee": "c", "file_id": "id2", "file_name": "doc.txt"},
            {"employee": "d", "file_id": "id1", "file_name": "dup.pdf"},
        ]
    }
    index_path.write_text(json.dumps(data), encoding="utf-8")

    out_dir = tmp_path / "out"
    payload = download_index_files(str(index_path), str(out_dir), dry_run=True)

    stats = payload["stats"]
    assert stats["total_items"] == 4
    assert stats["queued"] == 1
    assert stats["skipped_missing_id"] == 1
    assert stats["skipped_non_pdf"] == 1
    assert stats["duplicates"] == 1
    assert stats["dry_run"] == 1

    statuses = {item["status"] for item in payload["items"]}
    assert statuses == {"dry_run", "skipped"}
