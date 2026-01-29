from __future__ import annotations

from typing import Any, Dict, Optional

import pandas as pd


def _pairs_duration_sum(pairs_df: pd.DataFrame) -> Optional[float]:
    if pairs_df is None or pairs_df.empty:
        return None
    if "entry_ts" not in pairs_df.columns or "exit_ts" not in pairs_df.columns:
        return None
    durations = pairs_df[["entry_ts", "exit_ts"]].copy()
    durations["entry_ts"] = pd.to_datetime(durations["entry_ts"], errors="coerce")
    durations["exit_ts"] = pd.to_datetime(durations["exit_ts"], errors="coerce")
    durations = durations.dropna(subset=["entry_ts", "exit_ts"])
    if durations.empty:
        return None
    delta = durations["exit_ts"] - durations["entry_ts"]
    hours = delta.dt.total_seconds() / 3600.0
    return float(hours.sum())


def validate_cartellino(pairs_df: pd.DataFrame, totals: Dict[str, Any]) -> Dict[str, Any]:
    ore_lavorate_total = totals.get("ore_lavorate")
    ore_lavorate_pair_sum = _pairs_duration_sum(pairs_df)
    if ore_lavorate_total is None or ore_lavorate_pair_sum is None:
        ore_lavorate_diff = None
        is_ok = None
    else:
        ore_lavorate_diff = float(ore_lavorate_pair_sum - float(ore_lavorate_total))
        is_ok = abs(ore_lavorate_diff) < 0.05

    return {
        "ore_lavorate_pair_sum": ore_lavorate_pair_sum,
        "ore_lavorate_total": ore_lavorate_total,
        "ore_lavorate_diff": ore_lavorate_diff,
        "is_ok": is_ok,
    }


def validate_timbrature(
    pairs_df: pd.DataFrame,
    totals: Dict[str, Any],
    tolerance: float = 1.0,
) -> Dict[str, Any]:
    ore_lavorate_total = totals.get("ore_lavorate")
    ore_lavorate_pair_sum = _pairs_duration_sum(pairs_df)
    if ore_lavorate_total is None or ore_lavorate_pair_sum is None:
        ore_lavorate_diff = None
        is_ok = None
    else:
        ore_lavorate_diff = float(ore_lavorate_pair_sum - float(ore_lavorate_total))
        is_ok = abs(ore_lavorate_diff) <= tolerance

    return {
        "ore_lavorate_pair_sum": ore_lavorate_pair_sum,
        "ore_lavorate_total": ore_lavorate_total,
        "ore_lavorate_diff": ore_lavorate_diff,
        "is_ok": is_ok,
    }
