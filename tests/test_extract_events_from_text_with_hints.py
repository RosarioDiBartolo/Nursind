from pathlib import Path

import pandas as pd

from src.extract_events_from_documents.service import process_one_text_row
from tests.extract_events_manifest_fixtures import build_manifest_row


def test_layout_parser_keeps_line_level_provenance(tmp_path: Path) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "events"
    row = build_manifest_row(
        input_dir,
        employee="Mario Rossi",
        employee_id="emp-1",
        file_id="pdf-301",
        file_name="ottobre23.pdf",
        full_text=(
            "R R I I L L E E V V A A Z Z I I O O N N E E\n"
            "02 Lu *76 6,54 13,11 ! 6,15 6,00! 15 ! !\n"
        ),
        source_text_ref="Mario Rossi/ottobre23.txt",
    )

    result = process_one_text_row(
        row,
        output_dir=str(output_dir),
        out_name="events.csv",
        pages_name="pages.csv",
        input_dir=str(input_dir),
    )

    assert result["status"] == "ok"
    assert str(result["parser_id"]) == "cartellino_ocr"
    assert int(result["rows_with_events"]) == 1
    assert int(result["events_extracted"]) == 2

    df = pd.read_csv(Path(str(result["output_events_csv"])))
    assert list(df["event_kind"]) == ["E", "U"]
    assert list(df["event_time_hhmm"]) == ["06:54", "13:11"]
    assert "line_no=2" in str(df.loc[0, "source_event_ref"])

