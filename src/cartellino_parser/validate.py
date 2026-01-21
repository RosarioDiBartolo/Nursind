from __future__ import annotations

from typing import Any, Dict

import pandas as pd


def validate_cartellino(days_df: pd.DataFrame, totals: Dict[str, Any]) -> Dict[str, Any]:
    ore_lavorate_total = totals.get("ore_lavorate")
    ore_lavorate_row_sum = float(days_df["mo_lav"].fillna(0).sum())
    if ore_lavorate_total is None:
        ore_lavorate_diff = None
        is_ok = False
    else:
        ore_lavorate_diff = float(ore_lavorate_row_sum - float(ore_lavorate_total))
        is_ok = abs(ore_lavorate_diff) < 0.05

    return {
        "ore_lavorate_row_sum": ore_lavorate_row_sum,
        "ore_lavorate_total": ore_lavorate_total,
        "ore_lavorate_diff": ore_lavorate_diff,
        "is_ok": is_ok,
    }
