from pathlib import Path

from src.raw_text_parsing import (
    infer_year_month_from_filename,
    infer_year_month_from_header,
    parse_day_header,
    resolve_year_month,
)


def test_infer_year_month_from_header_handles_noisy_mese_line() -> None:
    text = (
        "Ente:039 AUSL Toscana Sud-Est Elaborazione del 12/01/2026\n"
        "RR II LL EE VV AA ZZ II OO NN EE PP RR EE SS EE NN ZZ EE "
        "ddeell mmeessee ddii AAggoossttoo 22002233 Pag. 1\n"
    )
    year, month = infer_year_month_from_header(text)
    assert (year, month) == (2023, 8)


def test_infer_year_month_from_filename_supports_compact_month_year() -> None:
    assert infer_year_month_from_filename(Path("agosto23.txt")) == (2023, 8)
    assert infer_year_month_from_filename(Path("012024.txt")) == (2024, 1)


def test_resolve_year_month_prefers_filename_before_header_date_fallback() -> None:
    text = "Ente:039 AUSL Toscana Sud-Est Elaborazione del 12/01/2026\n"
    year, month = resolve_year_month(text, Path("febbraio23.txt"))
    assert (year, month) == (2023, 2)


def test_resolve_year_month_uses_header_date_as_last_resort() -> None:
    text = "Ente:039 AUSL Toscana Sud-Est Elaborazione del 12/01/2026\n"
    year, month = resolve_year_month(text, Path("documento_senza_data.txt"))
    assert (year, month) == (2026, 1)


def test_parse_day_header_handles_doubled_ocr_prefix_and_dow() -> None:
    assert parse_day_header("0011 DDoo RRR ! ! ! !") == (1, "DO")
