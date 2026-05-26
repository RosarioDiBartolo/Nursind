from pathlib import Path

import pandas as pd

from cartellino_parser.turni_afternoon_long_export.service import (
    export_afternoon_long_from_dir,
    process_many_enriched_files,
    process_one_enriched_file,
)


def _write_enriched_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)


def test_turni_afternoon_long_export_process_one_contract(tmp_path: Path) -> None:
    source = tmp_path / "enrichment" / "Mario Rossi.enriched.csv"
    output_dir = tmp_path / "afternoon_long"
    _write_enriched_csv(
        source,
        [
            {
                "employee": "Mario Rossi",
                "entry_ts": "2025-05-10 13:10:00",
                "exit_ts": "2025-05-10 21:10:00",
                "duration_hours": 8.0,
                "is_holiday": "false",
                "is_afternoon": "true",
                "is_long": "true",
                "turno": "Pomeriggio",
            },
            {
                "employee": "Mario Rossi",
                "entry_ts": "2025-05-10 13:40:00",
                "exit_ts": "2025-05-10 21:40:00",
                "duration_hours": 8.0,
                "is_holiday": "true",
                "is_afternoon": "true",
                "is_long": "true",
                "turno": "Pomeriggio",
            },
            {"employee": "Mario Rossi", "is_afternoon": "false", "is_long": "true", "turno": "Mattina"},
            {"employee": "Mario Rossi", "is_afternoon": "true", "is_long": "false", "turno": "Pomeriggio"},
        ],
    )

    result = process_one_enriched_file(source, output_dir=str(output_dir))

    assert result["status"] == "ok"
    assert result["employee"] == "Mario Rossi"
    assert int(result["rows_total"]) == 4
    assert int(result["rows_selected"]) == 1

    out_csv = Path(str(result["output_filtered_csv"]))
    assert out_csv.exists()
    filtered = pd.read_csv(out_csv)
    assert len(filtered) == 1
    assert list(filtered.columns) == [
        "dipendente",
        "entrata",
        "uscita",
        "durata turno",
        "Festivo",
        "Turno",
        "Data",
    ]
    assert str(filtered.loc[0, "dipendente"]) == "Mario Rossi"
    assert str(filtered.loc[0, "entrata"]) == "2025-05-10 13:40:00"
    assert str(filtered.loc[0, "uscita"]) == "2025-05-10 21:40:00"
    assert str(filtered.loc[0, "durata turno"]) == "08:00"
    assert str(filtered.loc[0, "Festivo"]) == "Festivo"
    assert str(filtered.loc[0, "Turno"]) == "Pomeriggio"
    assert str(filtered.loc[0, "Data"]) == "2025-05-10"


def test_turni_afternoon_long_export_process_many_contract(tmp_path: Path) -> None:
    source_dir = tmp_path / "enrichment"
    output_dir = tmp_path / "afternoon_long"
    first = source_dir / "Mario Rossi.enriched.csv"
    second = source_dir / "Anna Bianchi.enriched.csv"
    _write_enriched_csv(
        first,
        [
            {"employee": "Mario Rossi", "entry_ts": "2025-05-10 13:15:00", "is_afternoon": "true", "is_long": "true"},
            {"employee": "Mario Rossi", "entry_ts": "2025-05-10 13:45:00", "is_afternoon": "true", "is_long": "true"},
            {"employee": "Mario Rossi", "entry_ts": "2025-05-10 15:00:00", "is_afternoon": "false", "is_long": "true"},
        ],
    )
    _write_enriched_csv(
        second,
        [
            {"employee": "Anna Bianchi", "entry_ts": "2025-05-10 14:10:00", "is_afternoon": "false", "is_long": "false"},
        ],
    )

    report = process_many_enriched_files(
        [second, first],
        output_dir=str(output_dir),
        enriched_dir=str(source_dir),
    )

    stats = report["stats"]
    assert int(stats["files_total"]) == 2
    assert int(stats["files_processed"]) == 2
    assert int(stats["files_error"]) == 0
    assert int(stats["rows_total"]) == 4
    assert int(stats["rows_selected"]) == 1
    assert int(stats["files_with_selected_rows"]) == 1
    assert int(stats["files_without_selected_rows"]) == 1
    assert len(report["items"]) == 2
    assert len(report["issues"]) == 0


def test_turni_afternoon_long_export_from_dir_writes_report(tmp_path: Path) -> None:
    source_dir = tmp_path / "enrichment"
    output_dir = tmp_path / "afternoon_long"
    report_json = tmp_path / "afternoon_long.report.json"
    _write_enriched_csv(
        source_dir / "Mario Rossi.enriched.csv",
        [
            {"employee": "Mario Rossi", "entry_ts": "2025-05-10 13:40:00", "is_afternoon": "true", "is_long": "true"},
        ],
    )

    report = export_afternoon_long_from_dir(
        enriched_dir=str(source_dir),
        output_dir=str(output_dir),
        report_json=str(report_json),
    )

    assert report["stage"] == "turni_afternoon_long_export"
    assert report_json.exists()
    assert (output_dir / "Mario Rossi.afternoon_long.csv").exists()
