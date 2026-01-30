from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from parsing.parsers.cartellino import parse_pdf  # noqa: E402


def _iter_rotated_pdfs() -> list[Path]:
    rotated_dir = ROOT / "rotated"
    if not rotated_dir.exists():
        return []
    return sorted(rotated_dir.rglob("*.pdf"))


def test_parse_rotated_cartellino_samples() -> None:
    pdfs = _iter_rotated_pdfs()
    if not pdfs:
        pytest.skip("No rotated cartellino samples found under rotated/.")

    for pdf_path in pdfs:
        parsed = parse_pdf(pdf_path)

        assert isinstance(parsed.days_df, pd.DataFrame)
        assert 28 <= len(parsed.days_df) <= 31
