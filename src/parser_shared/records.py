from __future__ import annotations

from dataclasses import asdict
from typing import Any, Dict, Iterable, List

import pandas as pd

from parser_shared.models import DayRecord


def records_to_df(records: Iterable[DayRecord]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for record in records:
        rows.append(asdict(record))
    return pd.DataFrame(
        rows,
        columns=["year", "month", "day", "dow", "mo_f", "mo_t", "mo_lav", "raw"],
    )
