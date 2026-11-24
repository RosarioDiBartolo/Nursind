from __future__ import annotations

from core.events.extraction.parsers import load_parsers, resolve_parser


def _document(text: str) -> dict[str, object]:
    return {
        "document": {
            "full_text": text,
        },
        "layout": {
            "pages": [],
        },
    }


def test_load_parsers_discovers_expected_ids() -> None:
    parser_ids = {parser.parser_id for parser in load_parsers()}
    assert {
        "timbrature_web",
        "cartellino_classic",
        "situazione_mensile",
        "cartellino_unico",
        "cartellino_ocr",
    }.issubset(parser_ids)


def test_resolve_parser_uses_deterministic_fallback() -> None:
    parser = resolve_parser(_document("documento senza indicatori forti"))
    assert parser.parser_id == "timbrature_web"


def test_resolve_parser_and_parse_document_for_rilevazione_compact_rows() -> None:
    text = (
        "Azienda Ospedaliero Universitaria ''Policlinico - Vittorio Emanuele''\n"
        "RILEVAZIONE DEL MESE DI OTTOBRE 2021\n"
        "ve 01 E0650 U1409 07.09 01.09 01.09 2CARD\n"
        "lu 04 E1258 U2008 07.08 01.08 01.08 2CARD\n"
        "do*10 GG:RS 2CARD\n"
    )

    parser = resolve_parser(_document(text))

    assert parser.parser_id == "cartellino_classic"
    rows = parser.parse_document(_document(text))
    assert len(rows) == 3
    assert [(event.event_kind, event.event_time_hhmm) for event in rows[0].events] == [
        ("E", "06:50"),
        ("U", "14:09"),
    ]


def test_cartellino_unico_explicit_punches_exclude_hours_totals() -> None:
    document = _document(
        "Cartellino Unico\n"
        "Giorno Timbrature Mensa Anomalie FISICO PROGR LAVORO SALDO\n"
        "Sab 01 E20:49 1570 Ind.3T 10:26 10:00 10:15 00:15\n"
        "Dom 02 U07:15 E20:36 10:34 10:00\n"
        "Lun 03 FERIE 06:00 08:00\n"
    )
    parser = resolve_parser(document)

    assert parser.parser_id == "cartellino_unico"
    assert [
        [(event.event_kind, event.event_time_hhmm) for event in row.events]
        for row in parser.parse_document(document)
    ] == [[("E", "20:49")], [("U", "07:15"), ("E", "20:36")], []]


def test_cartellino_unico_columns_keep_exit_without_an_entry() -> None:
    header = [("Giorno", 10), ("Orario", 50), ("Anomalie", 80)]
    for index in range(12):
        pos = 114 + 31 * index
        header.extend([("Usc" if index % 2 else "Ent", pos), (str(index // 2 + 1), pos + 13)])
    raw_lines = [
        header,
        [("1", 14), ("ven", 24), ("07:16", 145), ("o", 164), ("07:03", 496)],
        [("2", 14), ("sab", 24), ("08:02", 114), ("13:10", 145), ("05:08", 725)],
    ]
    words = []
    lines = []
    for index, raw_line in enumerate(raw_lines):
        first = len(words)
        words.extend(
            {
                "word_index": first + offset,
                "text": token,
                "x0": x,
                "x1": x + 10,
                "y0": index * 10,
                "y1": index * 10 + 8,
            }
            for offset, (token, x) in enumerate(raw_line)
        )
        lines.append({
            "text": " ".join(token for token, _ in raw_line),
            "word_indices": list(range(first, len(words))),
        })
    document = {
        "document": {"full_text": "Cartellino Unico"},
        "layout": {"pages": [{"page_no": 1, "words": words, "lines": lines}]},
    }
    parser = resolve_parser(document)

    assert [
        [(event.source_slot, event.event_time_hhmm) for event in row.events]
        for row in parser.parse_document(document)
    ] == [[("usc_1", "07:16")], [("ent_1", "08:02"), ("usc_1", "13:10")]]


