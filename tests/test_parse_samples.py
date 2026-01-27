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
            diff = abs(parsed.days_df["mo_lav"].sum() - totals["ore_lavorate"])
            assert diff < 0.05
