from datetime import date
from pathlib import Path
import json
import sys

import holidays
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from turni_summary import build_turni_summary  # noqa: E402


def _write_index(tmp_path: Path, pairs_rel: str) -> Path:
    index = {
        "root_id": "root",
        "generated_at": "2026-01-30T00:00:00Z",
        "employee_count": 1,
        "total_files": 1,
        "files": {
            "file-1": {
                "employee": "Mario Rossi",
                "employee_id": "E1",
                "file_id": "file-1",
                "file_name": "doc.pdf",
                "outputs": {"pairs_csv": pairs_rel},
            }
        },
    }
    path = tmp_path / "index.json"
    path.write_text(json.dumps(index), encoding="utf-8")
    return path


def _pick_non_sunday_holiday(year: int) -> date | None:
    it_holidays = holidays.country_holidays("IT", years=year)
    for holiday_date in sorted(it_holidays):
        if holiday_date.weekday() != 6:
            return holiday_date
    return None


def test_turni_summary_counts_italian_holidays(tmp_path: Path) -> None:
    year = 2024
    holiday_date = _pick_non_sunday_holiday(year)
    assert holiday_date is not None
    assert holiday_date.weekday() != 6

    pairs_path = tmp_path / "pairs.csv"
    df = pd.DataFrame(
        [
            {
                "entry_ts": f"{holiday_date} 08:00:00",
                "exit_ts": f"{holiday_date} 16:00:00",
                "turno": "",
            }
        ]
    )
    df.to_csv(pairs_path, index=False)

    index_path = _write_index(tmp_path, "pairs.csv")
    rows, _stats, years_used = build_turni_summary(str(index_path), [year])

    assert years_used == [year]
    f_row = next(row for row in rows if row["turno"] == "F")
    p_row = next(row for row in rows if row["turno"] == "P")
    n_row = next(row for row in rows if row["turno"] == "N")

    assert f_row[str(year)] == 1
    assert p_row[str(year)] == 0
    assert n_row[str(year)] == 0
