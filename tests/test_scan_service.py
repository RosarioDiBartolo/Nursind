import io
from pathlib import Path
import sys
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.drive_service.names import normalize_term  # noqa: E402
import src.scan_directory.scan_service as scan_service  # noqa: E402
from src.scan_directory.scan_service import file_excluded, folder_excluded  # noqa: E402


def test_folder_excluded_normalizes_name():
    terms = [normalize_term("Busta Paga"), normalize_term("Cedolino")]
    assert folder_excluded("Busta    Paga", terms) == "busta paga"
    assert folder_excluded("Altro", terms) is None


def test_file_excluded_matches_terms():
    terms = [normalize_term("busta paga"), normalize_term("cedolino")]
    assert file_excluded("Busta Paga gennaio.pdf", terms) == "busta paga"
    assert file_excluded("report.pdf", terms) is None


def _make_zip_bytes(entries: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, payload in entries.items():
            zf.writestr(name, payload)
    return buf.getvalue()


def test_collect_files_recursive_expands_zip_pdf_members(monkeypatch):
    emp = {"id": "emp-1", "name": "Mario Rossi"}

    def fake_list_children(_drive, folder_id: str):
        if folder_id == "emp-1":
            return [
                {"id": "pdf-1", "name": "direct.pdf", "mimeType": "application/pdf"},
                {"id": "zip-1", "name": "bundle.zip", "mimeType": "application/zip"},
            ]
        return []

    zip_bytes = _make_zip_bytes(
        {
            "inside/cedolino_01.pdf": b"%PDF-1.4 member 1",
            "inside/readme.txt": b"text file",
            "cedolino_02.PDF": b"%PDF-1.4 member 2",
        }
    )

    def fake_download_file_bytes(_drive, file_id: str, logger=None):
        assert file_id == "zip-1"
        return zip_bytes

    monkeypatch.setattr(scan_service, "list_children", fake_list_children)
    monkeypatch.setattr(scan_service, "download_file_bytes", fake_download_file_bytes)

    files, excluded_folders, filtered_files = scan_service.collect_files_recursive(
        object(),
        emp,
        [],
        root_prefix="Root",
    )

    assert excluded_folders == []
    assert filtered_files == []
    assert len(files) == 3
    assert all(f["local"] is False for f in files)
    assert any(f["file_id"] == "pdf-1" and f["file_name"] == "direct.pdf" for f in files)
    assert any(
        f["file_id"].startswith("zip::zip-1::")
        and f["file_name"] == "cedolino_01.pdf"
        and f["drive_path"] == "/Root/Mario Rossi/bundle.zip/inside/cedolino_01.pdf"
        for f in files
    )
    assert any(
        f["file_id"].startswith("zip::zip-1::")
        and f["file_name"] == "cedolino_02.PDF"
        and f["drive_path"] == "/Root/Mario Rossi/bundle.zip/cedolino_02.PDF"
        for f in files
    )


def test_collect_files_recursive_marks_invalid_zip(monkeypatch):
    emp = {"id": "emp-1", "name": "Mario Rossi"}

    def fake_list_children(_drive, folder_id: str):
        if folder_id == "emp-1":
            return [{"id": "zip-1", "name": "bundle.zip", "mimeType": "application/zip"}]
        return []

    monkeypatch.setattr(scan_service, "list_children", fake_list_children)
    monkeypatch.setattr(scan_service, "download_file_bytes", lambda *_args, **_kwargs: b"not a zip")

    files, excluded_folders, filtered_files = scan_service.collect_files_recursive(
        object(),
        emp,
        [],
    )

    assert files == []
    assert excluded_folders == []
    assert len(filtered_files) == 1
    assert filtered_files[0]["local"] is False
    assert filtered_files[0]["file_id"] == "zip-1"
    assert filtered_files[0]["reason"] == "invalid_zip_archive"


def test_build_folder_report_no_stdout_on_excluded_file(monkeypatch, capsys):
    emp = {"id": "emp-1", "name": "Mario Rossi"}

    monkeypatch.setattr(
        scan_service,
        "get_drive_service",
        lambda _creds: object(),
    )
    monkeypatch.setattr(
        scan_service,
        "collect_files_recursive",
        lambda *_args, **_kwargs: (
            [
                {
                    "file_id": "f-1",
                    "file_name": "Busta Paga gennaio.pdf",
                    "drive_path": "/Root/Mario Rossi/Busta Paga gennaio.pdf",
                }
            ],
            [],
            [],
        ),
    )

    terms = [normalize_term("busta paga")]
    _ = scan_service.build_folder_report(object(), emp, terms, root_prefix="Root")

    captured = capsys.readouterr()
    assert captured.out == ""
