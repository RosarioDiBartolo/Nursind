import json

import pytest
import pypdfium2 as pdfium

from core.tools.deviation_export.comparison import compare_shift, validate_schedules
from core.tools.deviation_export.service import export_deviations
from core.events.extraction.writers import write_rows_csv
from core.tools.deviation_export.source_codes import SourceCodes


@pytest.fixture
def schedules():
    return {
        "T01M": {"entry": "07:00", "exit": "14:00"},
        "T01N": {"entry": "21:00", "exit": "24:00", "continuation": "T01A"},
        "T01A": {"entry": "00:00", "exit": "07:00", "continuation_only": True},
    }


def test_threshold_is_strict_and_uses_absolute_signed_minutes(schedules):
    row = {"entry_ts": "2021-01-01 06:50:00", "exit_ts": "2021-01-01 14:10:00"}
    comparison = compare_shift(row, "T01M", "T01M", schedules, 10)
    assert comparison["diff_entry_minutes"] == -10
    assert comparison["diff_exit_minutes"] == 10
    assert not comparison["over_threshold"]
    row["entry_ts"] = "2021-01-01 06:49:00"
    assert compare_shift(row, "T01M", "T01M", schedules, 10)["over_threshold"]


def test_night_is_one_comparison_across_month_boundary(schedules):
    row = {"entry_ts": "2021-01-31 20:40:00", "exit_ts": "2021-02-01 07:12:00"}
    result = compare_shift(row, "T01N", "T01A", schedules, 10)
    assert result["month"] == "2021-01"
    assert result["theoretical_exit"] == "2021-02-01 07:00:00"
    assert result["diff_entry_minutes"] == -20
    assert result["diff_exit_minutes"] == 12
    with pytest.raises(ValueError, match="continuation"):
        compare_shift(row, "T01N", "T01M", schedules, 10)
    with pytest.raises(ValueError, match="Unconfigured"):
        compare_shift(row, "UNKNOWN", "T01A", schedules, 10)


def test_invalid_schedule_and_zero_duration_are_rejected(schedules):
    with pytest.raises(ValueError, match="Invalid theoretical"):
        validate_schedules({"BAD": {"entry": "25:00", "exit": "26:00"}})
    with pytest.raises(ValueError, match="Non-positive"):
        compare_shift({"entry_ts": "2021-01-01 07:00:00", "exit_ts": "2021-01-01 07:00:00"},
                      "T01M", "T01M", schedules, 10)


def test_export_produces_traceable_post_filter_pdf_and_exclusion_audit(tmp_path, schedules):
    root = tmp_path / "pipeline"
    (root / "events").mkdir(parents=True)
    (root / "shifts").mkdir()
    document = tmp_path / "source.json"
    document.write_text(json.dumps({"layout": {"pages": [
        {"page_no": 1, "lines": [{"line_id": "night", "text": "31 DO E 20:40 U(24:00) | |31 T01N"}]},
        {"page_no": 2, "lines": [{"line_id": "morning", "text": "01 LU E(00:00) U 07:12 | |01 T01A"}]},
    ]}}), encoding="utf-8")
    entry_ref = f"{document}#p1:line=night:w1"
    exit_ref = f"{document}#p2:line=morning:w1"
    cleaned = root / "events/events.cleaned.csv"
    write_rows_csv(out_csv=cleaned, rows=[
        {"source_event_ref": entry_ref, "event_ts": "2021-01-31 20:40:00", "event_kind": "E", "source_employee": "Synthetic"},
        {"source_event_ref": exit_ref, "event_ts": "2021-02-01 07:12:00", "event_kind": "U", "source_employee": "Synthetic"},
    ], columns=["source_event_ref", "event_ts", "event_kind", "source_employee"])
    pair = {"entry_ts": "2021-01-31 20:40:00", "exit_ts": "2021-02-01 07:12:00",
            "entry_event_ref": entry_ref, "exit_event_ref": exit_ref, "source_csv": str(cleaned)}
    write_rows_csv(out_csv=root / "shifts/Synthetic.pairs.csv", rows=[pair], columns=list(pair))
    schedule_file = tmp_path / "schedules.json"
    schedule_file.write_text(json.dumps({"source": "Synthetic schedules", "schedules": schedules}))
    report = export_deviations(root, tmp_path, schedules_path=schedule_file)
    assert report["stats"]["rows_total"] == report["stats"]["rows_comparable"] == 1
    assert report["stats"]["rows_selected"] == 1
    pdf = pdfium.PdfDocument(report["items"][0]["pdf"])
    text = "\n".join(page.get_textpage().get_text_range() for page in pdf)
    assert "T01N + T01A" in text
    assert "07:00 (+1)" in text
    assert "-20 min" in text
    assert "+12 min" in text
    assert str(document.resolve()) in report["inputs"]["sha256"]
    pdf.close()
    codes = SourceCodes(tmp_path)
    assert codes.code(entry_ref) == "T01N"
    # A stale or pre-filter pair must never be presented as a valid comparison.
    pair["entry_ts"] = "2021-01-31 20:41:00"
    write_rows_csv(out_csv=root / "shifts/Synthetic.pairs.csv", rows=[pair], columns=list(pair))
    stale = export_deviations(root, tmp_path, schedules_path=schedule_file)
    assert stale["stats"]["rows_comparable"] == 0
    assert stale["stats"]["rows_excluded"] == 1
    # Two intervals on one attendance row cannot each represent the full schedule.
    pair["entry_ts"] = "2021-01-31 20:40:00"
    write_rows_csv(out_csv=root / "shifts/Synthetic.pairs.csv", rows=[pair, pair], columns=list(pair))
    fragmented = export_deviations(root, tmp_path, schedules_path=schedule_file)
    assert fragmented["stats"]["rows_comparable"] == 0
    assert fragmented["stats"]["rows_excluded"] == 2
    assert any(reason.startswith("Multiple paired intervals") for reason in next(
        issue for issue in fragmented["issues"] if issue["code"] == "uncomparable_shifts"
    )["reasons"])
