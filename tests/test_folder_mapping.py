from __future__ import annotations

import csv
import json
from io import BytesIO

import pytest
from reportlab.pdfgen.canvas import Canvas

from core.config import PipelineConfig
from core.documents.workers import extract_and_write
from core.drive.schema import IndexFile
from core.drive.text_extraction_csv import TEXT_EXTRACTION_COLUMNS
from core.events.extraction.identity import employee_identity
from core.folder_mapping.analysis import analyze_document
from core.folder_mapping.runner import mapping_config, run_mapping
from core.folder_mapping.scan import scan_folder
from core.folder_mapping.service import map_extracted_folder


def config_for(tmp_path):
    return mapping_config(PipelineConfig(
        config_path=tmp_path / "pipeline.json", repository_root=tmp_path,
        name="Test", output_dir_stem=tmp_path / "output", drive_root_id="root",
    ))


def pdf_bytes(texts):
    stream = BytesIO()
    canvas = Canvas(stream)
    for text in texts:
        cursor = canvas.beginText(40, 800)
        cursor.setFont("Helvetica", 11)
        for line in text.splitlines():
            cursor.textLine(line)
        canvas.drawText(cursor)
        canvas.showPage()
    canvas.save()
    return stream.getvalue()


@pytest.mark.parametrize("text,name,status", [
    ("Dipendente: Mario Rossi Matricola: 123", "Mario Rossi", "recognized"),
    ("Cognome e nome:\nD'AMICO MARIA", "D'AMICO MARIA", "recognized"),
    ("Cognome: DE LUCA Nome: Maria", "DE LUCA Maria", "recognized"),
    ("Nominativo: Mario Rossi\nDipendente: Anna Bianchi", None, "ambiguous"),
    ("Documento senza nominativo", None, "missing"),
    ("Dipendente: 123456", None, "missing"),
    ("Nominativo del dipendente", None, "missing"),
])
def test_identity_is_content_based(text, name, status):
    result = employee_identity(text)
    assert result["employee"] == name
    assert result["identity_status"] == status


def test_inventory_covers_root_and_nested_pdfs_without_employee_folder_assumptions(tmp_path, monkeypatch):
    config = config_for(tmp_path)
    tree = {
        "root": [
            {"id": "a", "name": "cedolino.pdf", "mimeType": "application/pdf"},
            {"id": "sub", "name": "Scatola", "mimeType": "application/vnd.google-apps.folder"},
            {"id": "note", "name": "note.txt", "mimeType": "text/plain"},
        ],
        "sub": [{"id": "b", "name": "altro.PDF", "mimeType": "application/octet-stream"}],
    }
    monkeypatch.setattr("core.folder_mapping.scan.list_children", lambda drive, folder: tree[folder])
    report = scan_folder(object(), config)
    assert {item["relative_path"] for item in report["files"]} == {"cedolino.pdf", "Scatola/altro.PDF", "note.txt"}
    index = json.loads(config.paths.scan_included_index.read_text())
    assert set(index["files"]) == {"a", "b"}
    assert {item["employee"] for item in index["files"].values()} == {"unknown"}


def test_real_pdf_extraction_maps_mixed_employees_and_failures(tmp_path, monkeypatch):
    config = config_for(tmp_path)
    paths = config.paths
    texts = [
        "Riepilogo presenze/assenze - giugno 2023\nDipendente: Mario Rossi\n01 gi E 08:00 U 14:00",
        "Riepilogo presenze/assenze - luglio 2024\nDipendente: Anna Bianchi\n01 lu E 09:00 U 15:00",
        "Riepilogo presenze/assenze - agosto 2024\n01 gi E 08:00 U 14:00",
        "Nominativo: Mario Rossi\nComunicazione aziendale priva di eventi.",
    ]
    paths.documents_dir.mkdir(parents=True)
    result = extract_and_write(pdf_bytes(texts), {
        "file_id": "pdf", "file_name": "arbitrario.pdf", "employee": "WRONG FOLDER",
        "drive_path": "Scatola/arbitrario.pdf",
    }, str(paths.documents_dir), 0.72, 0.08)
    assert result["status"] == "success"
    with (paths.documents_dir / "unknown.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=TEXT_EXTRACTION_COLUMNS)
        writer.writeheader()
        writer.writerow({key: result.get(key) for key in TEXT_EXTRACTION_COLUMNS})
    paths.scan_dir.mkdir(parents=True)
    inventory = {"root_id": "root", "files": [
        {"file_id": "pdf", "file_name": "arbitrario.pdf", "relative_path": "Scatola/arbitrario.pdf", "is_pdf": True},
        {"file_id": "broken", "file_name": "rotto.pdf", "relative_path": "rotto.pdf", "is_pdf": True},
        {"file_id": "note", "file_name": "note.txt", "relative_path": "note.txt", "is_pdf": False},
    ]}
    (paths.scan_dir / "inventory.json").write_text(json.dumps(inventory), encoding="utf-8")
    from core.drive.index.map_index import MapIndex
    MapIndex.generate_index("root", 0, {"broken": IndexFile(employee="unknown", reason="invalid_pdf")}).save_index(str(paths.documents_excluded_index))
    report = map_extracted_folder(config)
    mapped, broken, note = report["files"]
    assert mapped["is_timbrature"]
    assert mapped["employees"] == ["Anna Bianchi", "Mario Rossi"]
    assert [page["employee"] for page in mapped["pages"]] == ["Mario Rossi", "Anna Bianchi", None, "Mario Rossi"]
    assert [page["is_timbrature"] for page in mapped["pages"]] == [True, True, True, False]
    assert [page.get("page_year") for page in mapped["pages"][:3]] == [2023, 2024, 2024]
    assert broken["is_timbrature"] is False and broken["error"] == "invalid_pdf"
    assert note["reason"] == "not_pdf"
    with paths.events_csv.open(encoding="utf-8") as handle:
        events = list(csv.DictReader(handle))
    assert {row["source_page_no"] for row in events} == {"1", "2", "3"}
    assert {row["source_employee"] for row in events} == {"Mario Rossi", "Anna Bianchi", ""}
    assert report["stats"]["events"] == 6
    # Resuming event analysis uses saved artifacts, with no Drive connection.
    monkeypatch.setattr("core.folder_mapping.runner.load_creds", lambda: pytest.fail("Unexpected Drive access"))
    primary = PipelineConfig(config.config_path, tmp_path, "Test", tmp_path / "output", "root")
    assert run_mapping(primary, start="extract_events", through="summarize_shifts")["stats"] == report["stats"]
    grouped = paths.pipeline_root / "grouped"
    snapshot = grouped / (grouped / "current.txt").read_text()
    assert len(list(snapshot.glob("employees/*/aggregation/turni_employee_summary.csv"))) == 2
    with (snapshot / "unresolved/events/events.csv").open(encoding="utf-8") as handle:
        assert len(list(csv.DictReader(handle))) == 2


def test_page_failure_prevents_file_recognition(tmp_path, monkeypatch):
    config = config_for(tmp_path)
    config.paths.scan_dir.mkdir(parents=True)
    (config.paths.scan_dir / "inventory.json").write_text(json.dumps({"root_id": "root", "files": [
        {"file_id": "x", "is_pdf": True},
    ]}))
    monkeypatch.setattr("core.folder_mapping.service.load_text_extraction_rows", lambda path: {"x": {}})
    monkeypatch.setattr("core.folder_mapping.service.load_manifest_document", lambda *args, **kwargs: {})
    monkeypatch.setattr("core.folder_mapping.service.analyze_document", lambda *args: ([
        {"is_timbrature": True, "error": None}, {"is_timbrature": False, "error": "parser failed"},
    ], [{"event_id": "should_not_export"}]))
    report = map_extracted_folder(config)
    assert report["files"][0]["is_timbrature"] is False
    assert not any(page["is_timbrature"] for page in report["files"][0]["pages"])
    assert report["stats"]["events"] == 0


def test_missing_dates_and_ambiguous_names_are_not_guessed():
    document = {"document": {"page_count": 1, "full_text":
        "Dipendente: Mario Rossi\nDipendente: Anna Bianchi\n01 lu E 08:00 U 14:00"}, "layout": {"pages": []}}
    pages, events = analyze_document(document, {"source_doc_json": "x.json", "source_file_id": "x"})
    assert pages[0]["identity_status"] == "ambiguous"
    assert not pages[0]["is_timbrature"]
    assert not events


def test_unique_document_identity_and_missing_identity_on_event_pages(tmp_path):
    result = extract_and_write(pdf_bytes([
        "Nominativo: Mario Rossi\nCopertina",
        "Riepilogo presenze/assenze - giugno 2023\n01 gi E 08:00 U 14:00",
    ]), {"file_id": "pdf", "file_name": "x.pdf"}, str(tmp_path), 0.72, 0.08)
    document = json.loads((tmp_path / result["doc_json"]).read_text(encoding="utf-8"))
    pages, events = analyze_document(document, {"source_doc_json": "x.json", "source_file_id": "pdf"})
    assert pages[1]["identity_status"] == "document_unique"
    assert pages[1]["employee"] == "Mario Rossi"
    assert pages[1]["is_timbrature"]
    assert {event["source_employee"] for event in events} == {"Mario Rossi"}

@pytest.mark.parametrize("row,expected", [
    ({"file_url": "https://drive.google.com/open?id=Ab12"}, "Ab12"),
    ({"file_id": "Ab12", "file_url": "https://drive.google.com/file/d/Ab12/view"}, "Ab12"),
    ({"file_id": "Ab12", "file_url": "https://drive.google.com/file/d/X/view"}, None),
])
def test_manifest_reference(row, expected):
    from core.folder_mapping.csv_input import drive_id
    if expected:
        assert drive_id(row) == expected
    else:
        with pytest.raises(ValueError):
            drive_id(row)


def test_classic_identity_is_scoped_to_employer():
    identity = employee_identity("AUSL1 - PERSONALE DIPENDENTE\nROSSI MARIA - 7080 Un. Org. UNI")
    assert identity["employee"] == "ROSSI MARIA"
    assert identity["employee_identifier"] == "AUSL1:7080"


def test_csv_inventory_records_duplicates_and_inaccessible_files(tmp_path):
    from unittest.mock import Mock
    from googleapiclient.errors import HttpError
    from httplib2 import Response
    from core.folder_mapping.csv_input import scan_csv

    source = tmp_path / "files.csv"
    source.write_text("file_url\nhttps://drive.google.com/open?id=A\n"
                      "https://drive.google.com/file/d/A/view\nhttps://drive.google.com/open?id=B\n")
    drive = Mock()
    drive.files.return_value.get.return_value.execute.side_effect = [
        {"name": "sample.pdf", "mimeType": "application/pdf"},
        HttpError(Response({"status": "404"}), b"not found"),
    ]
    config = config_for(tmp_path)
    report = scan_csv(drive, config, source)
    assert len(report["files"]) == 2
    assert {r["file_id"] for r in report["files"] if r.get("reason") == "inaccessible"} == {"B"}
    assert json.loads((config.paths.scan_dir / "duplicates.json").read_text())["rows"] == [{"file_id": "A", "line": 3}]
