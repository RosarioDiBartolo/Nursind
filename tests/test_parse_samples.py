from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from cartellino_parser import parse_pdf  # noqa: E402


def _iter_sample_pdfs() -> list[Path]:
    samples_dir = ROOT / "samples" / "cartellino"
    if not samples_dir.exists():
        return []
    return sorted(samples_dir.glob("*.pdf"))


def _pair_duration_sum(pairs_df: pd.DataFrame) -> float | None:
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


def test_parse_cartellino_samples() -> None:
    pdfs = _iter_sample_pdfs()
    if not pdfs:
        pytest.skip("No cartellino samples found under samples/cartellino/")

    for pdf_path in pdfs:
        parsed = parse_pdf(pdf_path)

        assert isinstance(parsed.days_df, pd.DataFrame)
        assert 28 <= len(parsed.days_df) <= 31

        totals = parsed.totals
        for key in [
            "ore_lavorate",
            "ore_dovute_programmate",
            "ore_dovute_contrattuali",
            "dbcr_netto",
        ]:
            assert key in totals

        if "saldo_al_mese_corrente" in totals:
            assert totals["saldo_al_mese_corrente"] is not None

        if totals.get("ore_lavorate") is not None:
            pair_sum = _pair_duration_sum(parsed.pairs_df)
            if pair_sum is not None:
                diff = abs(pair_sum - totals["ore_lavorate"])
                assert diff < 0.05
