from __future__ import annotations

from typing import Dict, List, Optional, Tuple

import pandas as pd

from timbrature_shared.day_values import extract_day_values


def _build_candidates(raw_line: str) -> tuple[Optional[float], Optional[float], bool]:
    extracted = extract_day_values(raw_line)
    if not extracted:
        return None, None, False
    _day, _dow, values, has_event = extracted
    if not values:
        return None, None, has_event
    large = [value for value in values if value >= 1.0]
    if not large:
        return None, None, has_event

    if has_event:
        primary = large[1] if len(large) >= 2 else large[0]
        alternative = large[0] if len(large) >= 2 else None
    else:
        primary = 0.0
        alternative = large[0]
    return primary, alternative, has_event


def adjust_mo_lav_to_totals(days_df: pd.DataFrame, totals: dict) -> pd.DataFrame:
    target = totals.get("ore_lavorate")
    if target is None or days_df.empty or "raw" not in days_df.columns:
        return days_df

    primary: List[Optional[float]] = []
    alternative: List[Optional[float]] = []
    has_event_flags: List[bool] = []

    for raw in days_df["raw"].tolist():
        value_primary, value_alt, has_event = _build_candidates(raw)
        primary.append(value_primary)
        alternative.append(value_alt)
        has_event_flags.append(has_event)

    values = [value if value is not None else 0.0 for value in primary]
    total = float(target)
    current_sum = sum(values)

    def to_minutes(hours_value: float) -> int:
        return int(round(hours_value * 60))

    def best_subset(values_list: List[int], target_value: int) -> Tuple[int, List[int]]:
        prev: Dict[int, Tuple[int, int]] = {0: (-1, -1)}
        for idx, value in enumerate(values_list):
            additions: Dict[int, Tuple[int, int]] = {}
            for current in prev.keys():
                next_sum = current + value
                if next_sum not in prev and next_sum not in additions:
                    additions[next_sum] = (current, idx)
            prev.update(additions)
        best_sum = min(prev.keys(), key=lambda s: abs(target_value - s))
        chosen: List[int] = []
        cursor = best_sum
        while cursor != 0:
            prev_sum, idx = prev[cursor]
            if idx == -1:
                break
            chosen.append(idx)
            cursor = prev_sum
        return best_sum, chosen

    # Reduce sum by switching event values to their alternative (smaller) values.
    if current_sum > total + 0.05:
        deltas: List[int] = []
        delta_map: List[Tuple[int, float]] = []
        for idx, (value_primary, value_alt, has_event) in enumerate(
            zip(primary, alternative, has_event_flags)
        ):
            if not has_event:
                continue
            if value_primary is None or value_alt is None:
                continue
            delta = value_primary - value_alt
            if delta > 0:
                delta_minutes = to_minutes(delta)
                deltas.append(delta_minutes)
                delta_map.append((idx, value_alt))
        if deltas:
            target_delta = to_minutes(current_sum - total)
            best_delta, chosen_idxs = best_subset(deltas, target_delta)
            for local_idx in chosen_idxs:
                idx, value_alt = delta_map[local_idx]
                values[idx] = value_alt
            current_sum = current_sum - (best_delta / 60.0)

    # Increase sum by adding non-event values when needed.
    if current_sum < total - 0.05:
        adds: List[int] = []
        add_map: List[Tuple[int, float]] = []
        for idx, (value_primary, value_alt, has_event) in enumerate(
            zip(primary, alternative, has_event_flags)
        ):
            if has_event:
                continue
            if value_alt is None or value_alt <= 0:
                continue
            adds.append(to_minutes(value_alt))
            add_map.append((idx, value_alt))
        if adds:
            target_add = to_minutes(total - current_sum)
            best_add, chosen_idxs = best_subset(adds, target_add)
            for local_idx in chosen_idxs:
                idx, value_alt = add_map[local_idx]
                values[idx] = value_alt
            current_sum = current_sum + (best_add / 60.0)

    updated = days_df.copy()
    updated["mo_lav"] = values
    return updated
