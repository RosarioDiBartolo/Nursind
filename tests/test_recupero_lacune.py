"""Run recovery from native event tables across a lot boundary and repeat it."""

import csv
from core.events.extraction.writers import EVENT_COLUMNS, write_rows_csv
from core.tools.recupero_lacune.service import run


def test_recount_joins_lots_reuses_cleaned_files_and_cached_counts(tmp_path, write_csv):
    group = "Ragusa Nursind"
    delivery = tmp_path / "Consegna" / group
    write_csv(delivery / "copertura_cartelle.csv", [{"lotto": x} for x in ("A", "B")])
    write_csv(
        delivery / "copertura_file.csv", [{"id": x, "dipendente": "Persona"} for x in ("a", "b")]
    )
    write_csv(
        delivery / "conteggi.csv",
        [
            {
                "dipendente": "Persona",
                "anno": 2025,
                "M": 0,
                "P": 0,
                "N": 0,
                "totale": 0,
                "di_cui_oltre_6_ore": 0,
            }
        ],
    )
    for lot, kind, ts in (("A", "E", "2025-12-31 20:00:00"), ("B", "U", "2026-01-01 07:00:00")):
        folder = tmp_path / group / lot / "artifacts/events"
        row = {
            "source_employee": "Persona",
            "source_file_id": lot.lower(),
            "event_kind": kind,
            "event_ts": ts,
            "event_time_hhmm": ts[11:16],
            "event_raw": kind + ts[11:16],
        }
        write_rows_csv(rows=[row], out_csv=folder / "events.csv", columns=EVENT_COLUMNS)
    assert run(tmp_path, [], False, True, 1) == 0
    out = tmp_path / "Ricalcolo" / group / "conteggi.csv"
    with out.open() as f:
        (result,) = csv.DictReader(f)
    assert (result["anno"], result["totale"], result["di_cui_oltre_6_ore"]) == ("2025", "1", "1")
    saved = out.stat().st_mtime_ns
    assert run(tmp_path, [], False, True, 1) == 0
    assert out.stat().st_mtime_ns == saved


def test_retry_missing_json_from_local_pdf_preserves_cached_result(tmp_path):
    from reportlab.pdfgen.canvas import Canvas
    from core.tools.recupero_lacune.recovery import Originals, parser_version, recover
    from core.drive.text_extraction_csv import build_doc_json_rel_path

    pdf = tmp_path / "timbrature.pdf"
    canvas = Canvas(str(pdf))
    canvas.drawString(30, 730, "RILEVAZIONE DEL MESE DI OTTOBRE 2021")
    canvas.drawString(30, 700, "ve 01 E0650 U1409")
    canvas.save()
    item = {
        "file_id": "test",
        "employee": "Persona",
        "file_name": pdf.name,
        "drive_path": str(pdf),
        "explicit": True,
        "source": tmp_path,
        "document": tmp_path / "documents" / build_doc_json_rel_path("test"),
    }
    output = tmp_path / "recovery"
    version = parser_version()
    first = recover(item, output, version, Originals(True))
    assert first["complete"] and [r["event_kind"] for r in first["events"]] == ["E", "U"]
    pdf.unlink()
    assert recover(item, output, version, Originals(True)) == first
