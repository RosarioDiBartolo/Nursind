from __future__ import annotations

from pathlib import Path

import pandas as pd

import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from timbrature_elenco_compact_parser import parse_pdf as parse_pdf_compact  # noqa: E402
from timbrature_elenco_parser import parse_pdf as parse_pdf_elenco  # noqa: E402


def _iter_sample_pdfs(folder: str) -> list[Path]:
    samples_dir = ROOT / "samples" / folder
    if not samples_dir.exists():
        return []
    return sorted(samples_dir.glob("*.pdf"))


def _assert_timbrature_parse(pdf_path: Path, parsed) -> None:
    assert isinstance(parsed.days_df, pd.DataFrame)
    assert 28 <= len(parsed.days_df) <= 31

    totals = parsed.totals
    if "ore_lavorate" in totals:
        assert totals["ore_lavorate"] is not None
        if parsed.validation.get("is_ok"):
            diff = abs(parsed.days_df["mo_lav"].sum() - totals["ore_lavorate"])
            assert diff < 0.05


def test_parse_timbrature_type_2_samples() -> None:
    pdfs = _iter_sample_pdfs("timbrature_compact")
    if not pdfs:
        import pytest

        pytest.skip("No timbrature samples found under samples/timbrature_compact/")

    for pdf_path in pdfs:
        parsed = parse_pdf_compact(pdf_path)
        _assert_timbrature_parse(pdf_path, parsed)


def test_parse_timbrature_type_3_samples() -> None:
    pdfs = _iter_sample_pdfs("timbrature_elenco")
    if not pdfs:
        import pytest

        pytest.skip("No timbrature samples found under samples/timbrature_elenco/")

    for pdf_path in pdfs:
        parsed = parse_pdf_elenco(pdf_path)
        _assert_timbrature_parse(pdf_path, parsed)
