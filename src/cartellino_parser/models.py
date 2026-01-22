from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional

import pandas as pd


class CartellinoParseError(RuntimeError):
    pass


@dataclass(frozen=True)
class DayRecord:
    year: Optional[int]
    month: Optional[int]
    day: int
    dow: str
    mo_f: float
    mo_t: float
    mo_lav: float
    raw: str


@dataclass(frozen=True)
class PairRecord:
    year: Optional[int]
    month: Optional[int]
    day: int
    dow: str
    pair_index: int
    entry_ts: Optional[datetime]
    exit_ts: Optional[datetime]
    duration_hhmm: Optional[str]
    turno: Optional[str]
    entry_raw: Optional[str]
    exit_raw: Optional[str]


@dataclass(frozen=True)
class ParsedCartellino:
    meta: Dict[str, Any]
    days_df: pd.DataFrame
    pairs_df: pd.DataFrame
    totals: Dict[str, Any]
    validation: Dict[str, Any]
