from __future__ import annotations

from dataclasses import dataclass
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
class ParsedCartellino:
    meta: Dict[str, Any]
    days_df: pd.DataFrame
    totals: Dict[str, Any]
    validation: Dict[str, Any]
