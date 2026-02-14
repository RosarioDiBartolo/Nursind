from __future__ import annotations

import os
import threading

from src.drive_service.archive_utils import BadZipFile, extract_zip_member_bytes, parse_archive_member_id
from src.drive_service.downloads import download_file_bytes
from src.drive_service.drive_client import get_drive_service
from src.drive_service.fs_utils import ensure_dir
from src.drive_service.logging_utils import get_logger
from src.drive_service.names import safe_name

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


def _get_zip_bytes_with_cache(drive, archive_file_id: str) -> bytes:
    cache, order = _get_zip_cache()
    cached = cache.get(archive_file_id)
    if cached is not None:
        if archive_file_id in order:
            order.remove(archive_file_id)
        order.append(archive_file_id)
        return cached

    zip_bytes = download_file_bytes(drive, archive_file_id, logger=logger)
    cache[archive_file_id] = zip_bytes
    order.append(archive_file_id)
    while len(order) > _ZIP_CACHE_MAX_ITEMS:
        oldest = order.pop(0)
        cache.pop(oldest, None)
    return zip_bytes


def _build_output_paths(
    emp_name: str,
    file_name: str,
    file_id: str,
    out_dir: str,
    out_stem: str | None = None,
):
    safe_emp = safe_name(emp_name)
    base_name = safe_name(file_name or file_id or "unknown.pdf")
    stem, _ = os.path.splitext(base_name)
    chosen_stem = safe_name(out_stem) if out_stem else stem
    out_name = f"{chosen_stem}.txt"
    file_dir = os.path.join(out_dir, safe_emp)
    text_path = os.path.join(file_dir, out_name)
    rel_text = os.path.join(safe_emp, out_name)
    return {"file_dir": file_dir, "text_path": text_path, "rel_text": rel_text}


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

    source_kind = doc_info.get("source_kind")
    archive_file_id = doc_info.get("archive_file_id")
    archive_member_path = doc_info.get("archive_member_path")
    parsed = parse_archive_member_id(file_id)
    if parsed:
        source_kind = "zip_member"
        if not archive_file_id:
            archive_file_id = parsed[0]
        if not archive_member_path:
            archive_member_path = parsed[1]

    try:
        drive = _get_drive(creds)
        if source_kind == "zip_member":
            if not archive_file_id:
                return {
                    "status": "failed",
                    "stage": "download",
                    "reason": "zip_archive_file_id_missing",
                    "doc": doc_info,
                }
            if not archive_member_path:
                return {
                    "status": "failed",
                    "stage": "download",
                    "reason": "zip_member_path_missing",
                    "doc": doc_info,
                }
            try:
                zip_bytes = _get_zip_bytes_with_cache(drive, archive_file_id)
            except Exception as exc:
                return {
                    "status": "failed",
                    "stage": "download",
                    "reason": f"zip_archive_download_error:{type(exc).__name__}",
                    "doc": doc_info,
                }
            try:
                pdf_bytes = extract_zip_member_bytes(zip_bytes, archive_member_path)
            except KeyError:
                return {
                    "status": "failed",
                    "stage": "download",
                    "reason": "zip_member_not_found",
                    "doc": doc_info,
                }
            except (BadZipFile, ValueError):
                return {
                    "status": "failed",
                    "stage": "download",
                    "reason": "zip_member_invalid_pdf",
                    "doc": doc_info,
                }
            except Exception as exc:
                return {
                    "status": "failed",
                    "stage": "download",
                    "reason": f"zip_member_read_error:{type(exc).__name__}",
                    "doc": doc_info,
                }
        else:
            pdf_bytes = download_file_bytes(drive, file_id, logger=logger)
    except Exception as exc:
        return {
            "status": "failed",
            "stage": "download",
            "reason": f"{type(exc).__name__}: {exc}",
            "doc": doc_info,
        }

    return {
        "status": "success",
        "data": pdf_bytes,
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
    paths = _build_output_paths(employee, file_name, file_id, out_dir, out_stem=out_stem)

    try:
        extracted = extract_best_text(
            pdf_bytes,
            min_normal_score=min_normal_score,
            min_score_delta=min_score_delta,
        )
        extracted_text = extracted.get("text", "")
        if not extracted_text.strip():
            raise ValueError("Extracted text is empty; skipping output write")
        ensure_dir(paths["file_dir"])
        with open(paths["text_path"], "w", encoding="utf-8") as out_file:
            out_file.write(extracted_text)
    except Exception as exc:
        return {
            "status": "failed",
            "stage": "extract",
            "reason": f"{type(exc).__name__}: {exc}",
            "doc": doc_info,
        }

    return {
        "status": "success",
        "employee": employee,
        "employee_id": employee_id,
        "file_id": file_id,
        "file_name": file_name,
        "drive_path": drive_path,
        "text_output": paths["rel_text"],
        "selected_mode": extracted["mode"],
        "tried_vertical": extracted["tried_vertical"],
        "normal_quality": extracted["normal_quality"],
        "vertical_quality": extracted["vertical_quality"],
    }
