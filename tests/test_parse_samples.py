from __future__ import annotations

from pathlib import Path

import pandas as pd

import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from cartellino_parser import parse_pdf  # noqa: E402


PDF_NAMES = [
    "Cartellino mensile-2020-07.pdf",
    "Cartellino mensile-2022-01.pdf",
    "Cartellino mensile-2022-07.pdf",
    "Cartellino mensile-2022-12.pdf",
    "Cartellino mensile-2023-03-12.pdf",
    "Cartellino mensile-2023-11-19.pdf",
]


def test_parse_samples() -> None:
    documents = ROOT / "documents"
    for name in PDF_NAMES:
        pdf_path = documents / name
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

        diff = abs(parsed.days_df["mo_lav"].sum() - totals["ore_lavorate"])
        assert diff < 0.05
