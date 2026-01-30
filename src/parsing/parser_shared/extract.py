from __future__ import annotations

from pathlib import Path
from typing import BinaryIO, Iterable, List, Union

import pdfplumber


def _extract_text_from_pdf(pdf: pdfplumber.PDF) -> str:
    return "\n".join((page.extract_text() or "") for page in pdf.pages)


def _cluster_by_x(chars: Iterable[dict], x_tolerance: float) -> List[List[dict]]:
    clusters: List[List[dict]] = []
    centers: List[float] = []
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
    lines = []
    for cluster in _cluster_by_x(chars, x_tolerance=x_tolerance):
        ordered = sorted(cluster, key=lambda char: char["top"])
        line = "".join(char["text"] for char in ordered)
        if line.strip():
            lines.append(line)
    return "\n".join(lines)


def extract_text(source: Union[str, Path, BinaryIO]) -> str:
    if isinstance(source, (str, Path)):
        path = Path(source)
        with pdfplumber.open(path) as pdf:
            return _extract_text_from_pdf(pdf)
    source.seek(0)
    with pdfplumber.open(source) as pdf:
        return _extract_text_from_pdf(pdf)


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
