from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.drive_service.archive_utils import build_archive_member_id  # noqa: E402
from src.drive_service.index import MapIndex  # noqa: E402
import src.download_from_index as download_from_index  # noqa: E402


def test_select_docs_for_download_random_sample_is_deterministic():
    docs = [(f"id-{idx}", idx) for idx in range(10)]

    selected_a = download_from_index._select_docs_for_download(
        docs,
        random_sample=4,
        seed=123,
        limit=0,
    )
    selected_b = download_from_index._select_docs_for_download(
        docs,
        random_sample=4,
        seed=123,
        limit=0,
    )

    assert selected_a == selected_b
    assert len(selected_a) == 4


def test_select_docs_for_download_applies_limit_after_sampling():
    docs = [(f"id-{idx}", idx) for idx in range(12)]

    selected = download_from_index._select_docs_for_download(
        docs,
        random_sample=8,
        seed=77,
        limit=3,
    )

    assert len(selected) == 3


def test_main_downloads_zip_member_index_entry(monkeypatch, tmp_path):
    zip_member_id = build_archive_member_id("archive-file-1", "folder/member.pdf")
    source_index = MapIndex.generate_index(
        root_id="root",
        employee_count=1,
        files={
            zip_member_id: {
                "employee": "Mario Rossi",
                "employee_id": "emp-1",
                "file_id": zip_member_id,
                "file_name": "member.pdf",
                "drive_path": "/Root/Mario Rossi/archive.zip/folder/member.pdf",
                "type": "file",
            }
        },
    )
    pdf_bytes = b"%PDF-zip-member"

    index_path = tmp_path / "index.json"
    index_path.write_text("{}", encoding="utf-8")
    out_dir = tmp_path / "out"

    monkeypatch.setattr(download_from_index.MapIndex, "load_index", lambda *_args, **_kwargs: source_index)
    monkeypatch.setattr(download_from_index, "load_creds", lambda: object())
    monkeypatch.setattr(download_from_index, "get_drive_service", lambda _creds: object())

    calls = {"count": 0}

    def fake_download_pdf_bytes_for_index_item(_drive, **kwargs):
        calls["count"] += 1
        assert kwargs["file_id"] == zip_member_id
        return {"status": "success", "data": pdf_bytes}

    monkeypatch.setattr(
        download_from_index,
        "download_pdf_bytes_for_index_item",
        fake_download_pdf_bytes_for_index_item,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "download_from_index",
            "--index",
            str(index_path),
            "--out",
            str(out_dir),
        ],
    )

    code = download_from_index.main()

    assert code == 0
    assert calls["count"] == 1
    written_files = list(out_dir.rglob("*.pdf"))
    assert len(written_files) == 1
    assert written_files[0].parent == out_dir
    assert written_files[0].read_bytes() == pdf_bytes


def test_main_can_write_per_employee_subfolders(monkeypatch, tmp_path):
    source_index = MapIndex.generate_index(
        root_id="root",
        employee_count=1,
        files={
            "file-1": {
                "employee": "Mario Rossi",
                "employee_id": "emp-1",
                "file_id": "file-1",
                "file_name": "member.pdf",
                "drive_path": "/Root/Mario Rossi/member.pdf",
                "type": "file",
            }
        },
    )
    pdf_bytes = b"%PDF-direct"

    index_path = tmp_path / "index.json"
    index_path.write_text("{}", encoding="utf-8")
    out_dir = tmp_path / "out"

    monkeypatch.setattr(download_from_index.MapIndex, "load_index", lambda *_args, **_kwargs: source_index)
    monkeypatch.setattr(download_from_index, "load_creds", lambda: object())
    monkeypatch.setattr(download_from_index, "get_drive_service", lambda _creds: object())
    monkeypatch.setattr(
        download_from_index,
        "download_pdf_bytes_for_index_item",
        lambda *_args, **_kwargs: {"status": "success", "data": pdf_bytes},
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "download_from_index",
            "--index",
            str(index_path),
            "--out",
            str(out_dir),
            "--no-flat-output",
        ],
    )

    code = download_from_index.main()

    assert code == 0
    written_files = list(out_dir.rglob("*.pdf"))
    assert len(written_files) == 1
    assert written_files[0].parent == out_dir / "Mario Rossi"
    assert written_files[0].read_bytes() == pdf_bytes


def test_main_downloads_local_index_entry_without_drive_creds(monkeypatch, tmp_path):
    local_pdf = tmp_path / "source.pdf"
    local_pdf.write_bytes(b"%PDF-local")
    source_index = MapIndex.generate_index(
        root_id="root",
        employee_count=1,
        files={
            "local::source.pdf": {
                "employee": "Mario Rossi",
                "employee_id": "emp-1",
                "local": True,
                "file_id": "local::source.pdf",
                "file_name": "source.pdf",
                "drive_path": str(local_pdf),
                "type": "file",
            }
        },
    )

    index_path = tmp_path / "index.json"
    index_path.write_text("{}", encoding="utf-8")
    out_dir = tmp_path / "out"

    monkeypatch.setattr(download_from_index.MapIndex, "load_index", lambda *_args, **_kwargs: source_index)
    monkeypatch.setattr(
        download_from_index,
        "load_creds",
        lambda: (_ for _ in ()).throw(AssertionError("local-only download should not load creds")),
    )
    monkeypatch.setattr(
        download_from_index,
        "get_drive_service",
        lambda _creds: (_ for _ in ()).throw(AssertionError("local-only download should not create drive")),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "download_from_index",
            "--index",
            str(index_path),
            "--out",
            str(out_dir),
        ],
    )

    code = download_from_index.main()

    assert code == 0
    written_files = list(out_dir.rglob("*.pdf"))
    assert len(written_files) == 1
    assert written_files[0].read_bytes() == b"%PDF-local"
