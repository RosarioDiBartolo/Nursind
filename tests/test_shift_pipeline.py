from __future__ import annotations

from pathlib import Path
import warnings

import pandas as pd
import pytest

from core.events.filtering.service import build_filter_midnight_events_from_dir
from core.shifts.enrichment.service import build_turni_enrichment_from_dir
from core.shifts.pairing.runtime import build_pair_employee_events_from_dir
from core.shifts.summary.service import build_turni_employee_summary_from_dir


def test_filter_pair_enrich_and_summarize_filesystem_pipeline(tmp_path: Path, write_csv) -> None:
    events_dir = tmp_path / "events"
    write_csv(
        events_dir / "events.csv",
        [
            {
                "event_id": "a",
                "event_kind": "E",
                "event_time_hhmm": "00:00",
                "event_ts": "2024-01-01 00:00:00",
                "event_raw": "E 00:00",
                "source_employee": "Mario Rossi",
                "source_file_id": "f1",
                "source_file_name": "sample.pdf",
                "source_line_no": 1,
                "event_index": 0,
            },
            {
                "event_id": "b",
                "event_kind": "E",
                "event_time_hhmm": "08:00",
                "event_ts": "2024-01-02 08:00:00",
                "event_raw": "E 08:00",
                "source_employee": "Mario Rossi",
                "source_file_id": "f1",
                "source_file_name": "sample.pdf",
                "source_line_no": 2,
                "event_index": 0,
            },
            {
                "event_id": "c",
                "event_kind": "U",
                "event_time_hhmm": "16:00",
                "event_ts": "2024-01-02 16:00:00",
                "event_raw": "U 16:00",
                "source_employee": "Mario Rossi",
                "source_file_id": "f1",
                "source_file_name": "sample.pdf",
                "source_line_no": 2,
                "event_index": 1,
            },
        ],
    )

    filter_report = build_filter_midnight_events_from_dir(input_dir=str(events_dir))
    shifts_dir = tmp_path / "shifts"
    pair_report = build_pair_employee_events_from_dir(
        input_dir=str(events_dir),
        output_dir=str(shifts_dir),
    )
    enrichment_dir = tmp_path / "enrichment"
    enrich_report = build_turni_enrichment_from_dir(
        input_dir=str(shifts_dir),
        output_dir=str(enrichment_dir),
        include_holidays=False,
    )
    summary_path = tmp_path / "aggregation" / "summary.csv"
    summary_report = build_turni_employee_summary_from_dir(
        enriched_dir=str(enrichment_dir),
        out=str(summary_path),
        report_json=str(tmp_path / "aggregation" / "report.json"),
        year_start=2024,
        year_end=2024,
    )

    assert filter_report["stats"]["rows_removed_midnight"] == 1
    assert pair_report["stats"]["pairs_out"] == 1
    assert enrich_report["stats"]["rows_enriched"] == 1
    assert summary_report["status"] == "ok"
    assert not pd.read_csv(summary_path).empty


def test_pairing_respects_max_gap(tmp_path: Path, write_csv) -> None:
    events = write_csv(
        tmp_path / "events" / "events.cleaned.csv",
        [
            {
                "event_id": "a",
                "event_kind": "E",
                "event_ts": "2024-01-01 08:00:00",
                "event_raw": "E 08:00",
                "source_employee": "Mario",
                "source_file_id": "f",
                "source_file_name": "x",
                "source_line_no": 1,
                "event_index": 0,
            },
            {
                "event_id": "b",
                "event_kind": "U",
                "event_ts": "2024-01-02 10:00:00",
                "event_raw": "U 10:00",
                "source_employee": "Mario",
                "source_file_id": "f",
                "source_file_name": "x",
                "source_line_no": 2,
                "event_index": 1,
            },
        ],
    )
    assert events.exists()
    report = build_pair_employee_events_from_dir(
        input_dir=str(events.parent),
        output_dir=str(tmp_path / "shifts"),
        max_gap_hours=16,
    )
    assert report["stats"]["pairs_out"] == 0


def test_summary_counts_legacy_enriched_shape(tmp_path: Path, write_csv) -> None:
    enriched = write_csv(
        tmp_path / "enrichment" / "Mario.enriched.csv",
        [
            {
                "year": 2024,
                "entry_ts": "2024-01-02 08:00:00",
                "exit_ts": "2024-01-02 16:00:00",
                "duration_hhmm": "08:00",
                "turno": "Mattina",
            },
            {
                "year": 2024,
                "entry_ts": "2024-01-03 14:00:00",
                "exit_ts": "2024-01-03 21:30:00",
                "duration_hhmm": "07:30",
                "turno": "Pomeriggio",
            },
            {
                "year": 2024,
                "entry_ts": "2024-01-04 20:00:00",
                "exit_ts": "2024-01-05 06:00:00",
                "duration_hhmm": "10:00",
                "turno": "Notte",
            },
            {
                "year": 2024,
                "entry_ts": "2024-01-05 14:00:00",
                "exit_ts": "2024-01-05 15:00:00",
                "duration_hhmm": "01:00",
                "turno": "Pomeriggio",
            },
        ],
    )
    summary_path = tmp_path / "aggregation" / "summary.csv"

    report = build_turni_employee_summary_from_dir(
        enriched_dir=str(enriched.parent),
        out=str(summary_path),
        report_json=str(tmp_path / "aggregation" / "report.json"),
        year_start=2024,
        year_end=2024,
    )

    rows = pd.read_csv(summary_path).set_index("turno")
    assert report["stats"]["rows_classified"] == 4
    assert rows.loc["M", "2024"] == 1
    assert rows.loc["P", "2024"] == 1
    assert rows.loc["N", "2024"] == 1
    assert rows.loc["S", "2024"] == 1


def test_enrichment_fixes_overnight_exit(tmp_path: Path, write_csv) -> None:
    pair = write_csv(
        tmp_path / "shifts" / "Mario.pairs.csv",
        [
            {
                "entry_ts": "2024-01-01 20:00:00",
                "exit_ts": "2024-01-01 06:00:00",
                "entry_raw": "E 20:00",
                "exit_raw": "U 06:00",
                "duration": "",
                "turno": "",
                "file_id": "f",
                "file_name": "x",
                "source_csv": "x",
            }
        ],
    )
    report = build_turni_enrichment_from_dir(
        input_dir=str(pair.parent),
        output_dir=str(tmp_path / "enrichment"),
        include_holidays=False,
    )
    assert report["stats"]["overnight_fix"] == 1


def test_shift_pipeline_is_clean_under_pandas_copy_warnings(tmp_path: Path, write_csv) -> None:
    pair = write_csv(
        tmp_path / "shifts" / "Mario.pairs.csv",
        [
            {
                "entry_ts": "2024-01-01 08:00:00",
                "exit_ts": "2024-01-01 16:00:00",
                "turno": "Mattina",
            }
        ],
    )

    with warnings.catch_warnings():
        warnings.filterwarnings("error", category=FutureWarning, module=r"core\..*")
        try:
            report = build_turni_enrichment_from_dir(
                input_dir=str(pair.parent),
                output_dir=str(tmp_path / "enrichment"),
                include_holidays=False,
            )
        except FutureWarning as exc:
            pytest.fail(f"pandas future warning raised by shift pipeline: {exc}")

    assert report["stats"]["rows_enriched"] == 1
