from __future__ import annotations

from io import BytesIO

from core.pdf import extract_text, extract_text_vertical, has_text_layer


def score_text_quality(text: str) -> dict:
    total_chars = len(text)
    if total_chars == 0:
        return {
            "score": 0.0,
            "total_chars": 0,
            "printable_ratio": 0.0,
            "alpha_ratio": 0.0,
            "replacement_ratio": 0.0,
            "avg_line_len": 0.0,
        }

    printable_chars = sum(1 for ch in text if ch.isprintable() or ch in "\n\r\t")
    non_ws_chars = [ch for ch in text if not ch.isspace()]
    non_ws_count = len(non_ws_chars)
    alpha_chars = sum(1 for ch in non_ws_chars if ch.isalpha())
    replacement_chars = text.count("\ufffd")
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    avg_line_len = sum(len(line) for line in lines) / len(lines) if lines else 0.0

    printable_ratio = printable_chars / total_chars
    alpha_ratio = (alpha_chars / non_ws_count) if non_ws_count else 0.0
    replacement_ratio = replacement_chars / total_chars
    length_score = min(1.0, total_chars / 1500.0)
    line_score = min(1.0, avg_line_len / 20.0)

    score = (
        (0.45 * printable_ratio)
        + (0.30 * min(1.0, alpha_ratio / 0.35))
        + (0.15 * line_score)
        + (0.10 * length_score)
        - min(0.20, replacement_ratio * 4.0)
    )
    score = max(0.0, min(1.0, score))

    return {
        "score": round(score, 6),
        "total_chars": total_chars,
        "printable_ratio": round(printable_ratio, 6),
        "alpha_ratio": round(alpha_ratio, 6),
        "replacement_ratio": round(replacement_ratio, 6),
        "avg_line_len": round(avg_line_len, 6),
    }


def extract_best_text(pdf_bytes: bytes, min_normal_score: float, min_score_delta: float) -> dict:
    stream = BytesIO(pdf_bytes)
    if not has_text_layer(stream):
        raise ValueError("PDF_HAS_NO_TEXT_LAYER")
    normal_text = extract_text(stream)
    normal_quality = score_text_quality(normal_text)
    normal_score = normal_quality["score"]

    selected_text = normal_text
    selected_mode = "normal"
    tried_vertical = False
    vertical_quality = None

    if normal_score < min_normal_score or not normal_text.strip():
        tried_vertical = True
        vertical_text = extract_text_vertical(stream)
        vertical_quality = score_text_quality(vertical_text)
        vertical_score = vertical_quality["score"]
        if vertical_text.strip() and (
            not normal_text.strip() or vertical_score >= normal_score + min_score_delta
        ):
            selected_text = vertical_text
            selected_mode = "vertical"

    if not selected_text.strip():
        raise ValueError("Extracted text is empty after normal/vertical attempts")

    return {
        "text": selected_text,
        "mode": selected_mode,
        "tried_vertical": tried_vertical,
        "normal_quality": normal_quality,
        "vertical_quality": vertical_quality,
    }

