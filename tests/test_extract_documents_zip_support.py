import io
import threading
import zipfile
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from cartellino_parser.drive_service.archive_utils import build_archive_member_id  # noqa: E402
import cartellino_parser.drive_service.index_downloads as index_downloads  # noqa: E402
from cartellino_parser.drive_service.io_json import load_json  # noqa: E402
import cartellino_parser.extract_documents_from_index.workers as workers  # noqa: E402
from cartellino_parser.extract_documents_from_index.planning import collect_docs  # noqa: E402
import cartellino_parser.pdf_text_extraction as pdf_text_extraction  # noqa: E402


@pytest.fixture(autouse=True)
def reset_worker_thread_local_state():
    workers._reset_thread_local_state()
    yield
    workers._reset_thread_local_state()


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
    monkeypatch.setattr(index_downloads, "download_file_bytes", fake_download_file_bytes)

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


def test_download_pdf_bytes_from_local_index_entry_reads_drive_path(monkeypatch, tmp_path: Path):
    local_pdf = tmp_path / "sample.pdf"
    local_pdf.write_bytes(b"%PDF-local")

    def fail_get_drive(_creds):
        raise AssertionError("local files should not request a Drive client")

    monkeypatch.setattr(workers, "_get_drive", fail_get_drive)

    result = workers.download_pdf_bytes(
        None,
        {
            "file_id": "local::sample.pdf",
            "file_name": "sample.pdf",
            "drive_path": str(local_pdf),
            "local": True,
            "source_kind": "local_pdf",
        },
        threading.Event(),
    )

    assert result["status"] == "success"
    assert result["data"] == b"%PDF-local"


def test_download_pdf_bytes_zip_member_not_found(monkeypatch):
    zip_bytes = _make_zip_bytes({"folder/a.pdf": b"%PDF-1.4-a"})
    monkeypatch.setattr(workers, "_get_drive", lambda _creds: object())
    monkeypatch.setattr(index_downloads, "download_file_bytes", lambda *_args, **_kwargs: zip_bytes)

    doc = {
        "file_id": build_archive_member_id("archive-1", "folder/missing.pdf"),
        "source_kind": "zip_member",
    }
    result = workers.download_pdf_bytes(None, doc, threading.Event())

    assert result["status"] == "failed"
    assert result["stage"] == "download"
    assert result["reason"] == "zip_member_not_found"


def test_extract_and_write_returns_drive_link_and_source_ref(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(
        workers,
        "extract_best_text",
        lambda *_args, **_kwargs: {
            "text": "Extracted text",
            "mode": "normal",
            "tried_vertical": False,
            "normal_quality": 0.95,
            "vertical_quality": 0.2,
        },
    )
    monkeypatch.setattr(
        workers,
        "extract_layout",
        lambda *_args, **_kwargs: {
            "page_count": 1,
            "pages": [
                {
                    "page_no": 1,
                    "width": 595.0,
                    "height": 842.0,
                    "words": [],
                    "lines": [],
                }
            ],
        },
    )

    result = workers.extract_and_write(
        b"%PDF-1.4",
        {
            "employee": "Mario Rossi",
            "employee_id": "emp-1",
            "file_id": "pdf-123",
            "file_name": "aprile23.pdf",
            "drive_path": "/Root/Mario Rossi/aprile23.pdf",
            "out_stem": "aprile23",
            "source_kind": "drive_pdf",
        },
        out_dir=str(tmp_path),
        min_normal_score=0.72,
        min_score_delta=0.08,
    )

    assert result["status"] == "success"
    assert result["source_text_ref"] == "Mario Rossi/aprile23.txt"
    assert result["google_drive_file_id"] == "pdf-123"
    assert result["file_link"] == "https://drive.google.com/file/d/pdf-123/view"
    assert result["has_text_layer"] is True
    assert str(result["doc_json"]).startswith("docs/")
    doc_path = tmp_path / str(result["doc_json"])
    assert doc_path.exists()
    payload = load_json(str(doc_path))
    assert payload["schema_version"] == "text_layout_v1"
    assert payload["document"]["full_text"] == "Extracted text"


def test_extract_and_write_marks_missing_text_layer(monkeypatch):
    def fake_extract_best_text(*_args, **_kwargs):
        raise ValueError("PDF_HAS_NO_TEXT_LAYER")

    monkeypatch.setattr(workers, "extract_best_text", fake_extract_best_text)

    result = workers.extract_and_write(
        b"%PDF-1.4",
        {
            "employee": "Mario Rossi",
            "employee_id": "emp-1",
            "file_id": "pdf-123",
            "file_name": "aprile23.pdf",
            "drive_path": "/Root/Mario Rossi/aprile23.pdf",
            "out_stem": "aprile23",
            "source_kind": "drive_pdf",
        },
        out_dir="unused",
        min_normal_score=0.72,
        min_score_delta=0.08,
    )

    assert result["status"] == "failed"
    assert result["stage"] == "extract"
    assert result["reason"] == "missing_text_layer"


def test_has_text_layer_uses_page_chars(monkeypatch):
    class FakePage:
        def __init__(self, chars):
            self.chars = chars

    class FakePdf:
        def __init__(self, pages):
            self.pages = pages

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    opened_sources = []

    def fake_open(source):
        opened_sources.append(source)
        return FakePdf([FakePage([]), FakePage([{"text": "A"}])])

    monkeypatch.setattr(pdf_text_extraction.pdfplumber, "open", fake_open)

    assert pdf_text_extraction.has_text_layer(io.BytesIO(b"%PDF-1.4")) is True
    assert len(opened_sources) == 1
