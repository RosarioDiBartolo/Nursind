from __future__ import annotations

from cartellino_parser.extract_events_from_documents.parsers.cartellino_ocr import (
    CartellinoOcrParser,
)


def _layout_word(word_index: int, text: str, x0: float, x1: float) -> dict[str, object]:
    return {
        "word_index": word_index,
        "text": text,
        "x0": x0,
        "x1": x1,
        "y0": 0.0,
        "y1": 10.0,
    }


def test_cartellino_ocr_parser_handles_spaced_day_headers_and_split_times() -> None:
    parser = CartellinoOcrParser()
    document = {
        "document": {
            "full_text": "",
        },
        "layout": {
            "pages": [
                {
                    "page_no": 1,
                    "words": [
                        _layout_word(0, "Data", 0.0, 10.0),
                        _layout_word(1, "Ent-1", 80.0, 95.0),
                        _layout_word(2, "Usc-1", 110.0, 125.0),
                        _layout_word(3, "Ent-2", 140.0, 155.0),
                        _layout_word(4, "Usc-2", 170.0, 185.0),
                        _layout_word(5, "2", 17.0, 21.2),
                        _layout_word(6, "6", 21.2, 25.4),
                        _layout_word(7, "L", 29.6, 33.8),
                        _layout_word(8, "u", 33.8, 38.0),
                        _layout_word(9, '"', 46.4, 50.6),
                        _layout_word(10, "R", 59.0, 63.2),
                        _layout_word(11, "E", 63.2, 67.4),
                        _layout_word(12, "C", 67.4, 71.6),
                        _layout_word(13, "1", 75.8, 80.0),
                        _layout_word(14, "9", 80.0, 84.2),
                        _layout_word(15, ",", 84.2, 88.4),
                        _layout_word(16, "4", 88.4, 92.6),
                        _layout_word(17, "6", 92.6, 96.8),
                        _layout_word(18, "1_0_,_4_0_", 134.6, 155.6),
                        _layout_word(19, "1_4_,_2_4_", 164.0, 185.0),
                    ],
                    "lines": [
                        {
                            "line_id": "header",
                            "text": "Data Ent-1 Usc-1 Ent-2 Usc-2",
                            "word_indices": [0, 1, 2, 3, 4],
                        },
                        {
                            "line_id": "row-26",
                            "text": '2 6 L u " R E C 1 9 , 4 6 1_0_,_4_0_ 1_4_,_2_4_ ! 6,00! 3,44 6,00- !',
                            "word_indices": list(range(5, 20)),
                        },
                    ],
                }
            ]
        },
    }

    rows = parser.parse_document(document)

    assert len(rows) == 1
    row = rows[0]
    assert (row.day, row.dow) == (26, "LU")
    assert [(event.event_kind, event.event_time_hhmm) for event in row.events] == [
        ("E", "19:46"),
        ("E", "10:40"),
        ("U", "14:24"),
    ]
