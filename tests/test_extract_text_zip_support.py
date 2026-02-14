import io
import threading
import zipfile
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from drive_service.archive_utils import build_archive_member_id  # noqa: E402
from extract_text_from_index.planning import collect_docs  # noqa: E402
import extract_text_from_index.workers as workers  # noqa: E402


def _make_zip_bytes(entries: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, payload in entries.items():
            zf.writestr(name, payload)
    return buf.getvalue()


def test_collect_docs_marks_zip_members_and_unique_stems():
    file_id_a = build_archive_member_id("archive-1", "folder/cedolino.pdf")
    file_id_b = build_archive_member_id("archive-1", "other/cedolino.pdf")
    source_files = {
        file_id_a: {
            "employee": "Mario Rossi",
            "employee_id": "emp-1",
            "file_id": file_id_a,
            "file_name": "cedolino.pdf",
            "drive_path": "/Root/Mario Rossi/archive.zip/folder/cedolino.pdf",
            "type": "file",
        },
        file_id_b: {
            "employee": "Mario Rossi",
            "employee_id": "emp-1",
            "file_id": file_id_b,
            "file_name": "cedolino.pdf",
            "drive_path": "/Root/Mario Rossi/archive.zip/other/cedolino.pdf",
            "type": "file",
        },
    }
    stats = {}
    docs = collect_docs(
        source_files,
        included_map={},
        excluded_map={},
        skip_included=False,
        skip_excluded=False,
        limit=0,
        stats=stats,
    )

    assert len(docs) == 2
    assert all(doc["source_kind"] == "zip_member" for doc in docs)
    assert docs[0]["archive_file_id"] == "archive-1"
    assert docs[0]["archive_member_path"] == "folder/cedolino.pdf"
    assert docs[1]["archive_member_path"] == "other/cedolino.pdf"
    assert docs[0]["out_stem"] != docs[1]["out_stem"]


def test_download_pdf_bytes_from_zip_member_uses_cache(monkeypatch):
    zip_bytes = _make_zip_bytes(
        {
            "folder/a.pdf": b"%PDF-1.4-a",
            "folder/b.pdf": b"%PDF-1.4-b",
        }
    )
    download_calls = {"count": 0}

    def fake_download_file_bytes(_drive, _file_id: str, logger=None):
        download_calls["count"] += 1
        return zip_bytes

    monkeypatch.setattr(workers, "_get_drive", lambda _creds: object())
    monkeypatch.setattr(workers, "download_file_bytes", fake_download_file_bytes)
    workers._thread_local.zip_cache = {}
    workers._thread_local.zip_cache_order = []

    doc_a = {
        "file_id": build_archive_member_id("archive-1", "folder/a.pdf"),
        "source_kind": "zip_member",
    }
    doc_b = {
        "file_id": build_archive_member_id("archive-1", "folder/b.pdf"),
        "source_kind": "zip_member",
    }

    result_a = workers.download_pdf_bytes(None, doc_a, threading.Event())
    result_b = workers.download_pdf_bytes(None, doc_b, threading.Event())

    assert result_a["status"] == "success"
    assert result_b["status"] == "success"
    assert result_a["data"] == b"%PDF-1.4-a"
    assert result_b["data"] == b"%PDF-1.4-b"
    assert download_calls["count"] == 1


def test_download_pdf_bytes_zip_member_not_found(monkeypatch):
    zip_bytes = _make_zip_bytes({"folder/a.pdf": b"%PDF-1.4-a"})
    monkeypatch.setattr(workers, "_get_drive", lambda _creds: object())
    monkeypatch.setattr(workers, "download_file_bytes", lambda *_args, **_kwargs: zip_bytes)
    workers._thread_local.zip_cache = {}
    workers._thread_local.zip_cache_order = []

    doc = {
        "file_id": build_archive_member_id("archive-1", "folder/missing.pdf"),
        "source_kind": "zip_member",
    }
    result = workers.download_pdf_bytes(None, doc, threading.Event())

    assert result["status"] == "failed"
    assert result["stage"] == "download"
    assert result["reason"] == "zip_member_not_found"
