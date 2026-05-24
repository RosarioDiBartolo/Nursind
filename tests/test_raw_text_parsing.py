from pathlib import Path

from cartellino_parser.raw_text_parsing import (
    extract_events,
    infer_year_month_from_filename,
    infer_year_month_from_header,
    infer_year_month_from_header_date,
    infer_year_month_from_text,
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


def test_parse_day_header_handles_spaced_ocr_prefix_and_dow() -> None:
    assert parse_day_header('2 6 L u " R E C 1 9 , 4 6') == (26, "LU")
    assert parse_day_header('1 18 8 D Do o " R R R ! ! !') == (18, "DO")


def test_parse_day_header_supports_dow_day_order_with_optional_marker() -> None:
    assert parse_day_header("ve 01 E0650 U1409 07.09 01.09 01.09 2CARD") == (1, "VE")
    assert parse_day_header("do*03 GG:RS 2CARD") == (3, "DO")


def test_extract_events_supports_compact_tokens() -> None:
    matches = extract_events("ve 01 E0650 U1409 e1402-R u1452-R")
    assert [(match.kind, match.time_str) for match in matches] == [
        ("E", "06:50"),
        ("U", "14:09"),
        ("E", "14:02"),
        ("U", "14:52"),
    ]


def test_infer_year_month_from_text_supports_rilevazione_header() -> None:
    text = (
        "Azienda Ospedaliero Universitaria\n"
        "RILEVAZIONE DEL MESE DI OTTOBRE 2021\n"
        "ve 01 E0650 U1409 07.09 01.09 01.09 2CARD\n"
    )
    assert infer_year_month_from_text(text) == (2021, 10)


def test_resolve_year_month_avoids_birth_date_when_report_header_exists() -> None:
    text = (
        "01/01/71 MTARRS71A01I035V AMATO ROSARIO SALVATORE\n"
        "RILEVAZIONE DEL MESE DI OTTOBRE 2021\n"
        "ve 01 E0650 U1409 07.09 01.09 01.09 2CARD\n"
    )
    year, month = resolve_year_month(text, Path("10.pdf"))
    assert (year, month) == (2021, 10)


def test_infer_year_month_from_header_date_ignores_unlabeled_birth_dates() -> None:
    text = "01/01/71 MTARRS71A01I035V AMATO ROSARIO SALVATORE\n"
    assert infer_year_month_from_header_date(text) == (None, None)

