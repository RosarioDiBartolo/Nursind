from __future__ import annotations

import csv
from functools import lru_cache
from io import StringIO
import math
import logging
import os
import re
from pathlib import Path
import shutil
import subprocess
from tempfile import TemporaryDirectory

from core.documents.ocr_options import OcrOptions
from core.pdf import group_words_into_lines
from core.parsing import parse_day_header


logger = logging.getLogger(__name__)


class OcrError(RuntimeError):
    """An OCR dependency or page failed; never silently accept partial documents."""


def _run_tesseract(arguments: list[str], options: OcrOptions) -> str:
    try:
        result = subprocess.run(
            arguments,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=options.timeout_seconds,
            check=False,
            env={**os.environ, "OMP_THREAD_LIMIT": "1"},
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
        )
    except FileNotFoundError as exc:
        raise OcrError("ocr_unavailable: install Tesseract or set ocr.executable") from exc
    except subprocess.TimeoutExpired as exc:
        raise OcrError(f"ocr_timeout: exceeded {options.timeout_seconds}s") from exc
    if result.returncode:
        raise OcrError(f"ocr_failed: {result.stderr.strip()[:1000]}")
    return result.stdout


@lru_cache(maxsize=8)
def check_tesseract(options: OcrOptions) -> str:
    executable = shutil.which(options.executable)
    if executable is None and options.executable == "tesseract" and os.name == "nt":
        candidate = (
            Path(os.environ.get("ProgramFiles", "C:/Program Files")) / "Tesseract-OCR/tesseract.exe"
        )
        if candidate.is_file():
            executable = str(candidate)
    if executable is None:
        raise OcrError("ocr_unavailable: install Tesseract and set ocr.executable or PATH")
    languages = _run_tesseract([executable, "--list-langs"], options).splitlines()
    missing = set(options.language.split("+")) - {line.strip() for line in languages}
    if missing:
        raise OcrError(f"ocr_missing_languages: {', '.join(sorted(missing))}")
    return executable


def layout_from_tsv(
    tsv: str,
    *,
    page_no: int,
    width: float,
    height: float,
    image_size: tuple[int, int],
    origin: tuple[float, float] = (0, 0),
) -> dict:
    """Convert OCR pixels to the existing PDF point coordinates and line IDs."""
    scale_x, scale_y = width / image_size[0], height / image_size[1]
    words: list[dict] = []
    reader = csv.DictReader(StringIO(tsv), delimiter="\t", quoting=csv.QUOTE_NONE)
    required = {"level", "text", "left", "top", "width", "height", "conf"}
    if not required.issubset(reader.fieldnames or []):
        raise OcrError("ocr_invalid_output: missing TSV columns")
    for row in reader:
        if row["level"] != "5" or not (row["text"] or "").strip():
            continue
        x, y = float(row["left"]), float(row["top"])
        w, h = float(row["width"]), float(row["height"])
        words.append(
            {
                "text": row["text"].strip(),
                "word_index": len(words),
                "x0": round(origin[0] + x * scale_x, 3),
                "y0": round(origin[1] + y * scale_y, 3),
                "x1": round(origin[0] + (x + w) * scale_x, 3),
                "y1": round(origin[1] + (y + h) * scale_y, 3),
                "confidence": float(row["conf"]),
            }
        )
    return {
        "page_no": page_no,
        "width": round(width, 3),
        "height": round(height, 3),
        "words": words,
        "lines": group_words_into_lines(words, page_index=page_no, y_tolerance=3.0),
    }


def recognize_page(page, options: OcrOptions) -> dict:
    executable = check_tesseract(options)
    dpi = options.dpi
    while math.ceil(page.width * dpi / 72) * math.ceil(page.height * dpi / 72) > options.max_pixels:
        dpi -= 1
        if dpi < 1:
            raise OcrError(f"ocr_page_too_large: page {page.page_number}")
    image = page.to_image(resolution=dpi, force_mediabox=True).original
    try:
        with TemporaryDirectory(prefix="nursind-ocr-") as directory:
            image_path = Path(directory) / "page.png"
            image.save(image_path)
            arguments = [
                executable, str(image_path), "stdout", "-l", options.language,
                "--dpi", str(dpi), "--psm", str(options.psm), "tsv",
            ]

            def recognize(image_size, width, height):
                return layout_from_tsv(
                    _run_tesseract(arguments, options), page_no=page.page_number,
                    width=float(width), height=float(height), image_size=image_size,
                    origin=(float(page.bbox[0]), float(page.bbox[1])),
                )

            original = recognize(image.size, page.width, page.height)
            if len(original["words"]) < 30 or _attendance_rows(original) >= 3:
                return original
            # Only dense, low-confidence output warrants the extra renderings.
            confidences = sorted(w["confidence"] for w in original["words"])
            if confidences[len(confidences) // 2] >= 75:
                return original
            image.close()
            image = page.to_image(resolution=dpi, force_mediabox=True, antialias=True).original
            for angle in (90, 270):
                with image.rotate(angle, expand=True) as rotated:
                    rotated.save(image_path)
                    try:
                        candidate = recognize(rotated.size, page.height, page.width)
                    except OcrError:
                        continue
                title = " ".join(l["text"] for l in candidate["lines"]).lower()
                if _attendance_rows(candidate) >= 3 and re.search(
                    r"timbratur|cartellino|rilevazione presenze", title
                ):
                    candidate["rotation_ccw"] = angle
                    return candidate
            return original
    finally:
        image.close()


def _attendance_rows(layout: dict) -> int:
    return len({day[0] for line in layout["lines"] if (day := parse_day_header(line["text"]))})
