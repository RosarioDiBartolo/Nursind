from pathlib import Path

import pandas as pd

from src.extract_events_from_documents.service import process_one_text_row
from tests.extract_events_manifest_fixtures import build_manifest_row


def test_traceability_columns_include_source_refs(tmp_path: Path) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "events"
    row = build_manifest_row(
        input_dir,
        employee="Mario Rossi",
        employee_id="emp-1",
        file_id="pdf-201",
        file_name="giugno23.pdf",
        full_text="Header line\n01 lu E 08:00 U 14:00 6 6 6\n",
        source_text_ref="Mario Rossi/giugno23.txt",
    )

    result = process_one_text_row(
        row,
        output_dir=str(output_dir),
        out_name="events.csv",
        pages_name="pages.csv",
        input_dir=str(input_dir),
    )

    assert result["status"] == "ok"
    df = pd.read_csv(Path(str(result["output_events_csv"])))
    assert int(df.loc[0, "source_line_no"]) == 2
    source_doc_json = str(df.loc[0, "source_doc_json"]).replace("\\", "/")
    assert not Path(source_doc_json).is_absolute()
    assert "/docs/" in f"/{source_doc_json}"
    assert str(df.loc[0, "source_file_name"]) == "giugno23.pdf"
    assert str(df.loc[0, "source_drive_path"]) == "/Root/Mario Rossi/giugno23.pdf"
    assert str(df.loc[0, "source_file_link"]) == "https://drive.google.com/file/d/pdf-201/view"
    assert "line_no=2" in str(df.loc[0, "source_event_ref"])


def test_page_and_event_refs_are_root_relative_when_doc_json_is_absolute(
    tmp_path: Path,
) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "events"
    row = build_manifest_row(
        input_dir,
        employee="Mario Rossi",
        employee_id="emp-1",
        file_id="pdf-202",
        file_name="luglio23.pdf",
        full_text="Riepilogo presenze/assenze - luglio 2023\n01 lu E 08:00 U 14:00 6 6 6\n",
        source_text_ref="Mario Rossi/luglio23.txt",
    )
    row["doc_json"] = str((input_dir / row["doc_json"]).resolve())

    result = process_one_text_row(
        row,
        output_dir=str(output_dir),
        out_name="events.csv",
        pages_name="pages.csv",
        input_dir=str(input_dir),
    )

    assert result["status"] == "ok"
    events_df = pd.read_csv(Path(str(result["output_events_csv"])))
    pages_df = pd.read_csv(Path(str(result["output_pages_csv"])))
    source_doc_json = str(events_df.loc[0, "source_doc_json"]).replace("\\", "/")
    source_event_ref = str(events_df.loc[0, "source_event_ref"]).replace("\\", "/")
    page_ref = str(pages_df.loc[0, "page_ref"]).replace("\\", "/")

    source_event_ref_path = source_event_ref.split("#", 1)[0]
    page_ref_path = page_ref.split("#", 1)[0]

    assert not Path(source_doc_json).is_absolute()
    assert not Path(source_event_ref_path).is_absolute()
    assert not Path(page_ref_path).is_absolute()

    assert "/docs/" in f"/{source_doc_json}"
    assert "/docs/" in f"/{source_event_ref_path}"
    assert "/docs/" in f"/{page_ref_path}"
    assert str(events_df.loc[0, "source_file_link"]) == "https://drive.google.com/file/d/pdf-202/view"
    assert str(pages_df.loc[0, "source_drive_path"]) == "/Root/Mario Rossi/luglio23.pdf"

