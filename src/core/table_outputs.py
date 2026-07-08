from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from core.drive.fs_utils import ensure_parent_dir


def write_csv_and_excel(
    rows: list[dict[str, Any]],
    csv_path: str | Path,
    *,
    columns: list[str] | None = None,
    sheet_name: str = "Summary",
) -> tuple[Path, Path]:
    csv_output = Path(csv_path)
    excel_output = csv_output.with_suffix(".xlsx")
    df = pd.DataFrame(rows, columns=columns)

    ensure_parent_dir(str(csv_output))
    df.to_csv(csv_output, index=False)
    df.to_excel(excel_output, index=False, sheet_name=sheet_name)
    return csv_output, excel_output


__all__ = ["write_csv_and_excel"]
