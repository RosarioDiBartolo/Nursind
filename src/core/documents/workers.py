from __future__ import annotations

import os
import threading
from io import BytesIO

from core.drive.drive_client import get_drive_service
from core.drive.index_downloads import download_pdf_bytes_for_index_item
from core.drive.logging_utils import get_logger
from core.drive.names import safe_name
from core.drive.text_extraction_csv import (
    TEXT_EXTRACTION_DOC_SCHEMA_VERSION,
    build_google_drive_file_id,
    build_google_drive_file_link,
    build_source_text_ref,
    write_text_extraction_doc,
)
from core.pdf import extract_layout

from .quality import extract_best_text

logger = get_logger()
_thread_local = threading.local()
_ZIP_CACHE_MAX_ITEMS = 16


def _get_drive(creds):
    drive = getattr(_thread_local, "drive", None)
    if drive is None:
        _thread_local.drive = get_drive_service(creds)
        drive = _thread_local.drive
    return drive


def _get_zip_cache() -> tuple[dict[str, bytes], list[str]]:
    cache = getattr(_thread_local, "zip_cache", None)
    order = getattr(_thread_local, "zip_cache_order", None)
    if cache is None or order is None:
        cache = {}
        order = []
        _thread_local.zip_cache = cache
        _thread_local.zip_cache_order = order
    return cache, order


def _reset_thread_local_state(*, clear_drive: bool = True) -> None:
    for attr in ("zip_cache", "zip_cache_order"):
        if hasattr(_thread_local, attr):
            delattr(_thread_local, attr)
    if clear_drive and hasattr(_thread_local, "drive"):
        delattr(_thread_local, "drive")


def _resolve_output_stem(
    file_name: str,
    file_id: str | None,
    out_stem: str | None = None,
) -> str:
    base_name = safe_name(file_name or file_id or "unknown.pdf")
    stem, _ = os.path.splitext(base_name)
    return safe_name(out_stem) if out_stem else stem


def download_pdf_bytes(
    creds,
    doc_info: dict,
    stop_event: threading.Event,
):
    if stop_event.is_set():
        return {"status": "failed", "stage": "cancel", "reason": "cancelled", "doc": doc_info}

    file_id = doc_info.get("file_id")
    if not file_id:
        return {
            "status": "failed",
            "stage": "precheck",
            "reason": "missing file_id",
            "doc": doc_info,
        }

    is_local = bool(doc_info.get("local")) or str(file_id).startswith("local::")

    try:
        drive = None if is_local else _get_drive(creds)
        if is_local:
            zip_cache: dict[str, bytes] = {}
            zip_cache_order: list[str] = []
        else:
            zip_cache, zip_cache_order = _get_zip_cache()
        result = download_pdf_bytes_for_index_item(
            drive,
            file_id=file_id,
            local=is_local,
            drive_path=doc_info.get("drive_path"),
            source_kind=doc_info.get("source_kind"),
            archive_file_id=doc_info.get("archive_file_id"),
            archive_member_path=doc_info.get("archive_member_path"),
            logger=logger,
            zip_cache=zip_cache,
            zip_cache_order=zip_cache_order,
            zip_cache_max_items=_ZIP_CACHE_MAX_ITEMS,
        )
    except Exception as exc:
        return {
            "status": "failed",
            "stage": "download",
            "reason": f"{type(exc).__name__}: {exc}",
            "doc": doc_info,
        }

    if result["status"] != "success":
        return {
            "status": "failed",
            "stage": "download",
            "reason": str(result.get("reason") or "download_failed"),
            "doc": doc_info,
        }

    return {
        "status": "success",
        "data": result["data"],
        "doc": doc_info,
    }


def extract_and_write(
    pdf_bytes: bytes,
    doc_info: dict,
    out_dir: str,
    min_normal_score: float,
    min_score_delta: float,
):
    file_id = doc_info.get("file_id")
    file_name = doc_info.get("file_name") or file_id or "unknown.pdf"
    out_stem = doc_info.get("out_stem")
    employee = doc_info.get("employee") or "unknown"
    employee_id = doc_info.get("employee_id")
    drive_path = doc_info.get("drive_path")
    local = bool(doc_info.get("local")) or str(file_id or "").startswith("local::")
    source_kind = doc_info.get("source_kind")
    archive_file_id = doc_info.get("archive_file_id")
    archive_member_path = doc_info.get("archive_member_path")
    chosen_stem = _resolve_output_stem(file_name, file_id, out_stem=out_stem)
    source_text_ref = build_source_text_ref(employee, chosen_stem)
    google_drive_file_id = build_google_drive_file_id(
        file_id=file_id,
        source_kind=source_kind,
        archive_file_id=archive_file_id,
    )
    file_link = build_google_drive_file_link(
        file_id=file_id,
        source_kind=source_kind,
        archive_file_id=archive_file_id,
    )

    try:
        extracted = extract_best_text(
            pdf_bytes,
            min_normal_score=min_normal_score,
            min_score_delta=min_score_delta,
        )
        extracted_text = extracted.get("text", "")
        if not extracted_text.strip():
            raise ValueError("Extracted text is empty; skipping output write")
        layout = extract_layout(BytesIO(pdf_bytes))
        doc_json = write_text_extraction_doc(
            out_dir,
            file_id,
            {
                "schema_version": TEXT_EXTRACTION_DOC_SCHEMA_VERSION,
                "source": {
                    "employee": employee,
                    "employee_id": employee_id,
                    "local": local,
                    "file_id": file_id,
                    "google_drive_file_id": google_drive_file_id,
                    "file_name": file_name,
                    "file_link": file_link,
                    "drive_path": drive_path,
                    "source_kind": source_kind,
                    "archive_file_id": archive_file_id,
                    "archive_member_path": archive_member_path,
                    "source_text_ref": source_text_ref,
                },
                "extraction": {
                    "has_text_layer": True,
                    "selected_mode": extracted["mode"],
                    "tried_vertical": extracted["tried_vertical"],
                    "normal_quality": extracted["normal_quality"],
                    "vertical_quality": extracted["vertical_quality"],
                },
                "document": {
                    "page_count": layout["page_count"],
                    "full_text": extracted_text,
                },
                "layout": {
                    "pages": layout["pages"],
                },
            },
        )
    except Exception as exc:
        if isinstance(exc, ValueError) and str(exc) == "PDF_HAS_NO_TEXT_LAYER":
            reason = "missing_text_layer"
        else:
            reason = f"{type(exc).__name__}: {exc}"
        return {
            "status": "failed",
            "stage": "extract",
            "reason": reason,
            "doc": doc_info,
        }

    return {
        "status": "success",
        "employee": employee,
        "employee_id": employee_id,
        "local": local,
        "file_id": file_id,
        "file_name": file_name,
        "drive_path": drive_path,
        "source_kind": source_kind,
        "archive_file_id": archive_file_id,
        "archive_member_path": archive_member_path,
        "source_text_ref": source_text_ref,
        "doc_json": doc_json,
        "has_text_layer": True,
        "google_drive_file_id": google_drive_file_id,
        "file_link": file_link,
        "selected_mode": extracted["mode"],
        "tried_vertical": extracted["tried_vertical"],
        "normal_quality": extracted["normal_quality"],
        "vertical_quality": extracted["vertical_quality"],
    }


__all__ = ["_reset_thread_local_state", "download_pdf_bytes", "extract_and_write"]

