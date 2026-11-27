from __future__ import annotations

from io import BytesIO
import logging

import pdfplumber

from core.documents.ocr import OcrError, recognize_page
from core.documents.ocr_options import OcrOptions
from core.documents.quality import score_text_quality, select_best_text
from core.pdf import extract_page_layout, extract_page_text


logger = logging.getLogger(__name__)


def ocr_reason(
    page, native_text: str, layout: dict, options: OcrOptions, min_normal_score: float
) -> str | None:
    """Conservative page heuristic: short digital pages alone do not require OCR."""
    if not native_text.strip():
        if page.images or page.curves or page.lines or page.rects or page.chars:
            return "no_usable_text"
        return None  # An actually empty PDF page.
    page_area = float(page.width * page.height)
    for image in page.images:
        x0, y0 = max(page.bbox[0], image["x0"]), max(page.bbox[1], image["top"])
        x1, y1 = min(page.bbox[2], image["x1"]), min(page.bbox[3], image["bottom"])
        if max(0, x1 - x0) * max(0, y1 - y0) < page_area * options.min_image_area_ratio:
            continue
        # Inspect the image region so a native header cannot hide a scanned body.
        region_words = [
            w
            for w in layout["words"]
            if x0 <= (w["x0"] + w["x1"]) / 2 <= x1 and y0 <= (w["y0"] + w["y1"]) / 2 <= y1
        ]
        chars = sum(len(w["text"]) for w in region_words)
        lines = {w["line_id"] for w in region_words}
        if chars < options.min_native_chars or len(lines) < options.min_native_lines:
            return "image_region_without_sufficient_text"
    quality = score_text_quality(native_text)
    if quality["score"] < min_normal_score and (
        len(native_text.strip()) >= options.min_native_chars or quality["replacement_ratio"] > 0
    ):
        return "low_native_quality"
    return None


def extract_document_pages(
    pdf_bytes: bytes,
    *,
    min_normal_score: float,
    min_score_delta: float,
    ocr: OcrOptions,
    document_label: str = "PDF",
) -> dict:
    """Extract mixed PDFs sequentially, preserving page positions and provenance."""
    layouts, metadata, texts, normal_texts, vertical_texts = [], [], [], [], []
    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        total_pages = len(pdf.pages)
        logger.info("PDF opened: %s | %s pages", document_label, total_pages)
        for page in pdf.pages:
            index = page.page_number
            if index == 1:
                logger.info("Page %s/%s started: %s", index, total_pages, document_label)
            native_text = selected_text = ""
            mode = "blank"
            reason = error = confidence = None
            tried_vertical = has_text_layer = False
            layout = {
                "page_no": index,
                "width": float(page.width),
                "height": float(page.height),
                "words": [],
                "lines": [],
            }
            try:
                has_text_layer = bool(page.chars)
                native_text = extract_page_text(page)
                selected_text = native_text
                mode = "normal" if native_text.strip() else "blank"
                layout = extract_page_layout(page, page_index=index)
                if native_text.strip():
                    selection = select_best_text(
                        native_text,
                        lambda: extract_page_text(page, vertical=True),
                        min_normal_score,
                        min_score_delta,
                    )
                    selected_text, mode = selection["text"], selection["mode"]
                    tried_vertical = selection["tried_vertical"]
                    if tried_vertical:
                        vertical_texts.append(extract_page_text(page, vertical=True))
                if ocr.enabled:
                    reason = ocr_reason(page, selected_text, layout, ocr, min_normal_score)
                if reason:
                    recognized = recognize_page(page, ocr)
                    text = "\n".join(line["text"] for line in recognized["lines"])
                    if not text.strip():
                        raise OcrError(f"ocr_empty_page: page {index}")
                    confidence = round(
                        sum(w["confidence"] for w in recognized["words"])
                        / len(recognized["words"]),
                        3,
                    )
                    layout, selected_text, mode = recognized, text, "ocr"
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"
                logger.warning("Page %s of %s: %s", index, document_label, error)
            finally:
                page.close()
            normal_texts.append(native_text)
            metadata.append(
                {
                    "page_no": index,
                    "method": mode,
                    "has_text_layer": has_text_layer,
                    "ocr_reason": reason,
                    "tried_vertical": tried_vertical,
                    "error": error,
                    "normal_quality": score_text_quality(native_text),
                    "selected_quality": score_text_quality(selected_text),
                    "ocr_confidence": confidence,
                }
            )
            layouts.append(layout)
            texts.append(selected_text)
            if index == total_pages:
                logger.info("Page %s/%s finished: %s", index, total_pages, document_label)
    text = "\n".join(texts)
    page_errors = [{"page_no": p["page_no"], "error": p["error"]} for p in metadata if p["error"]]
    if not text.strip():
        if page_errors:
            raise OcrError(str(page_errors[0]["error"]))
        raise ValueError("Extracted text is empty after native/OCR attempts")
    modes = {p["method"] for p in metadata if p["method"] != "blank"}
    mode = next(iter(modes)) if len(modes) == 1 else "mixed"
    return {
        "text": text,
        "mode": mode,
        "has_text_layer": any(p["has_text_layer"] for p in metadata),
        "tried_vertical": any(p["tried_vertical"] for p in metadata),
        "normal_quality": score_text_quality("\n".join(normal_texts)),
        "vertical_quality": score_text_quality("\n".join(vertical_texts))
        if vertical_texts
        else None,
        "pages": metadata,
        "page_errors": page_errors,
        "ocr_pages": sum(p["method"] == "ocr" for p in metadata),
        "layout": {"page_count": len(layouts), "pages": layouts},
    }
