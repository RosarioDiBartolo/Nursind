from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from cartellino_parser.drive_service.index.build_local_pdf_index import (  # noqa: E402
    build_local_pdf_index,
    main,
)
from cartellino_parser.drive_service.index import MapIndex  # noqa: E402


def test_build_local_pdf_index_writes_expected_map_index(tmp_path):
    folder = tmp_path / "Mario Rossi"
    folder.mkdir()
    first_pdf = folder / "cartellino-01.pdf"
    second_pdf = folder / "TIMBRATURE.PDF"
    first_pdf.write_bytes(b"%PDF-1")
    second_pdf.write_bytes(b"%PDF-2")
    (folder / "notes.txt").write_text("ignore me", encoding="utf-8")

    index, output_path = build_local_pdf_index(folder)

    assert output_path == folder / "index.json"
    assert output_path.exists()
    assert index.root_id == "Mario Rossi"
    assert index.employee_count == 1
    assert index.total_files == 2

    saved = MapIndex.load_index(str(output_path), strict=True)
    assert saved.root_id == "Mario Rossi"
    assert saved.employee_count == 1
    assert set(saved.files) == {
        "local::cartellino-01.pdf",
        "local::TIMBRATURE.PDF",
    }
    first_entry = saved.files["local::cartellino-01.pdf"]
    assert first_entry.employee == "Mario Rossi"
    assert first_entry.employee_id == "Mario Rossi"
    assert first_entry.local is True
    assert first_entry.file_name == "cartellino-01.pdf"
    assert first_entry.drive_path == str(first_pdf.resolve())
    assert first_entry.type == "file"


def test_build_local_pdf_index_can_override_root_and_employee_identity(tmp_path):
    folder = tmp_path / "Mario Rossi"
    folder.mkdir()
    pdf_path = folder / "cartellino-01.pdf"
    pdf_path.write_bytes(b"%PDF-1")

    index, output_path = build_local_pdf_index(folder, identity="Custom Root")

    assert output_path == folder / "index.json"
    assert index.root_id == "Custom Root"
    saved = MapIndex.load_index(str(output_path), strict=True)
    assert saved.root_id == "Custom Root"
    entry = saved.files["local::cartellino-01.pdf"]
    assert entry.employee == "Custom Root"
    assert entry.employee_id == "Custom Root"
    assert entry.local is True


def test_main_can_index_nested_pdfs_with_recursive_flag(tmp_path):
    folder = tmp_path / "Local Root"
    nested = folder / "nested"
    nested.mkdir(parents=True)
    (folder / "top.pdf").write_bytes(b"%PDF-top")
    (nested / "deep.pdf").write_bytes(b"%PDF-deep")

    rc = main(
        [
            "--folder",
            str(folder),
            "--name",
            "custom.index.json",
            "--recursive",
        ]
    )

    assert rc == 0
    saved = MapIndex.load_index(str(folder / "custom.index.json"), strict=True)
    assert saved.total_files == 2
    assert set(saved.files) == {
        "local::top.pdf",
        "local::nested/deep.pdf",
    }


def test_map_index_load_infers_local_flag_for_legacy_local_entries(tmp_path):
    index_path = tmp_path / "legacy.index.json"
    index_path.write_text(
        (
            "{\n"
            '  "root_id": "legacy",\n'
            '  "generated_at": "2026-03-03T00:00:00Z",\n'
            '  "employee_count": 1,\n'
            '  "total_files": 2,\n'
            '  "files": {\n'
            '    "local::sample.pdf": {\n'
            '      "employee": "Legacy",\n'
            '      "employee_id": "Legacy",\n'
            '      "file_name": "sample.pdf",\n'
            '      "drive_path": "C:\\\\tmp\\\\sample.pdf",\n'
            '      "type": "file"\n'
            "    },\n"
            '    "drive-1": {\n'
            '      "employee": "Legacy",\n'
            '      "employee_id": "Legacy",\n'
            '      "file_name": "remote.pdf",\n'
            '      "drive_path": "/Root/Legacy/remote.pdf",\n'
            '      "type": "file"\n'
            "    }\n"
            "  }\n"
            "}\n"
        ),
        encoding="utf-8",
    )

    loaded = MapIndex.load_index(str(index_path), strict=True)

    assert loaded.files["local::sample.pdf"].local is True
    assert loaded.files["drive-1"].local is False

