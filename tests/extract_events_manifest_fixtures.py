from __future__ import annotations

import csv
from pathlib import Path

from cartellino_parser.drive_service.text_extraction_csv import (
    TEXT_EXTRACTION_COLUMNS,
    TEXT_EXTRACTION_DOC_SCHEMA_VERSION,
    write_text_extraction_doc,
)


def build_manifest_row(
    base_dir: Path,
    *,
    employee: str,
    employee_id: str,
    file_id: str,
    file_name: str,
    full_text: str,
    source_text_ref: str | None = None,
    drive_path: str | None = None,
) -> dict[str, str]:
    resolved_source_ref = source_text_ref or f"{employee}/{Path(file_name).stem}.txt"
    doc_json = write_text_extraction_doc(
        base_dir,
        file_id,
        {
            "schema_version": TEXT_EXTRACTION_DOC_SCHEMA_VERSION,
            "source": {
                "source_text_ref": resolved_source_ref,
                "file_name": file_name,
            },
            "extraction": {
                "has_text_layer": True,
            },
            "document": {
                "page_count": 1,
                "full_text": full_text,
            },
            "layout": {
                "pages": [],
            },
        },
    )
    return {
        "employee": employee,
        "employee_id": employee_id,
        "file_id": file_id,
        "google_drive_file_id": file_id,
        "file_link": f"https://drive.google.com/file/d/{file_id}/view",
        "file_name": file_name,
        "drive_path": drive_path or f"/Root/{employee}/{file_name}",
        "source_kind": "drive_pdf",
        "archive_file_id": "",
        "archive_member_path": "",
        "source_text_ref": resolved_source_ref,
        "doc_json": doc_json,
        "has_text_layer": "True",
        "selected_mode": "normal",
        "tried_vertical": "False",
        "normal_quality": "0.9",
        "vertical_quality": "0.1",
    }


def write_manifest_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=TEXT_EXTRACTION_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

