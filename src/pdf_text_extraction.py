from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import BinaryIO, Iterable, Union

import pdfplumber


def _extract_text_from_pdf(pdf: pdfplumber.PDF) -> str:
    return "\n".join((page.extract_text() or "") for page in pdf.pages)


def _pdf_has_text_layer(pdf: pdfplumber.PDF) -> bool:
    return any(bool(getattr(page, "chars", None)) for page in pdf.pages)


def _cluster_by_x(chars: Iterable[dict], x_tolerance: float) -> list[list[dict]]:
    clusters: list[list[dict]] = []
    centers: list[float] = []
    for char in chars:
        x_center = (char["x0"] + char["x1"]) / 2.0
        for idx, center in enumerate(centers):
            if abs(center - x_center) <= x_tolerance:
                cluster = clusters[idx]
                cluster.append(char)
                centers[idx] = (centers[idx] * (len(cluster) - 1) + x_center) / len(cluster)
                break
        else:
            clusters.append([char])
            centers.append(x_center)
    ordered = sorted(zip(centers, clusters), key=lambda item: item[0])
    return [cluster for _, cluster in ordered]


def _extract_vertical_text_from_page(page: pdfplumber.page.Page, x_tolerance: float) -> str:
    chars = page.chars
    if not chars:
        return ""
    lines: list[str] = []
    for cluster in _cluster_by_x(chars, x_tolerance=x_tolerance):
        ordered = sorted(cluster, key=lambda char: char["top"])
        line = "".join(char["text"] for char in ordered)
        if line.strip():
            lines.append(line)
    return "\n".join(lines)


def _extract_layout_from_pdf(
    pdf: pdfplumber.PDF,
    *,
    word_x_tolerance: float,
    word_y_tolerance: float,
    line_y_tolerance: float,
) -> dict:
    pages: list[dict] = []
    for page_index, page in enumerate(pdf.pages, start=1):
        raw_words = page.extract_words(
            x_tolerance=word_x_tolerance,
            y_tolerance=word_y_tolerance,
            keep_blank_chars=False,
            use_text_flow=False,
        )
        words = [
            {
                "text": str(word.get("text") or ""),
                "x0": round(float(word["x0"]), 3),
                "y0": round(float(word["top"]), 3),
                "x1": round(float(word["x1"]), 3),
                "y1": round(float(word["bottom"]), 3),
                "word_index": word_index,
            }
            for word_index, word in enumerate(raw_words)
            if str(word.get("text") or "").strip()
        ]
        lines = _group_words_into_lines(words, page_index=page_index, y_tolerance=line_y_tolerance)
        pages.append(
            {
                "page_no": page_index,
                "width": round(float(page.width), 3),
                "height": round(float(page.height), 3),
                "words": words,
                "lines": lines,
            }
        )
    return {
        "page_count": len(pages),
        "pages": pages,
    }


def _group_words_into_lines(
    words: list[dict],
    *,
    page_index: int,
    y_tolerance: float,
) -> list[dict]:
    if not words:
        return []

    ordered_words = sorted(words, key=lambda item: (item["y0"], item["x0"], item["word_index"]))
    groups: list[list[dict]] = []
    for word in ordered_words:
        if not groups:
            groups.append([word])
            continue
        current = groups[-1]
        avg_y = sum(item["y0"] for item in current) / len(current)
        if abs(word["y0"] - avg_y) <= y_tolerance:
            current.append(word)
        else:
            groups.append([word])

    lines: list[dict] = []
    for line_index, group in enumerate(groups, start=1):
        line_words = sorted(group, key=lambda item: (item["x0"], item["word_index"]))
        line_id = f"p{page_index}_l{line_index}"
        for item in line_words:
            item["line_id"] = line_id
        lines.append(
            {
                "line_id": line_id,
                "text": " ".join(item["text"] for item in line_words),
                "x0": round(min(item["x0"] for item in line_words), 3),
                "y0": round(min(item["y0"] for item in line_words), 3),
                "x1": round(max(item["x1"] for item in line_words), 3),
                "y1": round(max(item["y1"] for item in line_words), 3),
                "word_indices": [int(item["word_index"]) for item in line_words],
            }
        )
    return lines


def extract_text(source: Union[str, Path, BinaryIO]) -> str:
    if isinstance(source, (str, Path)):
        path = Path(source)
        with pdfplumber.open(path) as pdf:
            return _extract_text_from_pdf(pdf)
    source.seek(0)
    with pdfplumber.open(source) as pdf:
        return _extract_text_from_pdf(pdf)


def has_text_layer(source: Union[str, Path, BinaryIO]) -> bool:
    if isinstance(source, (str, Path)):
        path = Path(source)
        with pdfplumber.open(path) as pdf:
            return _pdf_has_text_layer(pdf)
    source.seek(0)
    with pdfplumber.open(source) as pdf:
        return _pdf_has_text_layer(pdf)


def extract_text_vertical(source: Union[str, Path, BinaryIO], x_tolerance: float = 2.0) -> str:
    if isinstance(source, (str, Path)):
        path = Path(source)
        with pdfplumber.open(path) as pdf:
            return "\n".join(
                _extract_vertical_text_from_page(page, x_tolerance=x_tolerance)
                for page in pdf.pages
            )
    source.seek(0)
    with pdfplumber.open(source) as pdf:
        return "\n".join(
            _extract_vertical_text_from_page(page, x_tolerance=x_tolerance)
            for page in pdf.pages
        )


def extract_layout(
    source: Union[str, Path, BinaryIO, bytes],
    *,
    word_x_tolerance: float = 1.0,
    word_y_tolerance: float = 3.0,
    line_y_tolerance: float = 3.0,
) -> dict:
    if isinstance(source, bytes):
        source = BytesIO(source)
    if isinstance(source, (str, Path)):
        path = Path(source)
        with pdfplumber.open(path) as pdf:
            return _extract_layout_from_pdf(
                pdf,
                word_x_tolerance=word_x_tolerance,
                word_y_tolerance=word_y_tolerance,
                line_y_tolerance=line_y_tolerance,
            )
    source.seek(0)
    with pdfplumber.open(source) as pdf:
        return _extract_layout_from_pdf(
            pdf,
            word_x_tolerance=word_x_tolerance,
            word_y_tolerance=word_y_tolerance,
            line_y_tolerance=line_y_tolerance,
        )
