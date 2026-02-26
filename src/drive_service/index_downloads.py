from __future__ import annotations

from src.drive_service.archive_utils import (
    BadZipFile,
    extract_zip_member_bytes,
    parse_archive_member_id,
)
from src.drive_service.downloads import download_file_bytes


def resolve_index_pdf_source(
    file_id: str,
    *,
    source_kind: str | None = None,
    archive_file_id: str | None = None,
    archive_member_path: str | None = None,
) -> tuple[str | None, str | None, str | None]:
    resolved_source_kind = source_kind
    resolved_archive_file_id = archive_file_id
    resolved_archive_member_path = archive_member_path

    parsed = parse_archive_member_id(file_id)
    if parsed:
        resolved_source_kind = "zip_member"
        if not resolved_archive_file_id:
            resolved_archive_file_id = parsed[0]
        if not resolved_archive_member_path:
            resolved_archive_member_path = parsed[1]

    return resolved_source_kind, resolved_archive_file_id, resolved_archive_member_path


def _get_archive_bytes(
    drive,
    archive_file_id: str,
    *,
    logger=None,
    zip_cache: dict[str, bytes] | None = None,
    zip_cache_order: list[str] | None = None,
    zip_cache_max_items: int = 0,
) -> bytes:
    if zip_cache is None or zip_cache_order is None or zip_cache_max_items <= 0:
        return download_file_bytes(drive, archive_file_id, logger=logger)

    cached = zip_cache.get(archive_file_id)
    if cached is not None:
        if archive_file_id in zip_cache_order:
            zip_cache_order.remove(archive_file_id)
        zip_cache_order.append(archive_file_id)
        return cached

    zip_bytes = download_file_bytes(drive, archive_file_id, logger=logger)
    zip_cache[archive_file_id] = zip_bytes
    zip_cache_order.append(archive_file_id)
    while len(zip_cache_order) > zip_cache_max_items:
        oldest = zip_cache_order.pop(0)
        zip_cache.pop(oldest, None)
    return zip_bytes


def download_pdf_bytes_for_index_item(
    drive,
    *,
    file_id: str,
    source_kind: str | None = None,
    archive_file_id: str | None = None,
    archive_member_path: str | None = None,
    logger=None,
    zip_cache: dict[str, bytes] | None = None,
    zip_cache_order: list[str] | None = None,
    zip_cache_max_items: int = 0,
) -> dict:
    resolved_source_kind, resolved_archive_file_id, resolved_archive_member_path = (
        resolve_index_pdf_source(
            file_id,
            source_kind=source_kind,
            archive_file_id=archive_file_id,
            archive_member_path=archive_member_path,
        )
    )

    if resolved_source_kind == "zip_member":
        if not resolved_archive_file_id:
            return {"status": "failed", "reason": "zip_archive_file_id_missing"}
        if not resolved_archive_member_path:
            return {"status": "failed", "reason": "zip_member_path_missing"}
        try:
            zip_bytes = _get_archive_bytes(
                drive,
                resolved_archive_file_id,
                logger=logger,
                zip_cache=zip_cache,
                zip_cache_order=zip_cache_order,
                zip_cache_max_items=zip_cache_max_items,
            )
        except Exception as exc:
            return {
                "status": "failed",
                "reason": f"zip_archive_download_error:{type(exc).__name__}",
            }
        try:
            pdf_bytes = extract_zip_member_bytes(zip_bytes, resolved_archive_member_path)
        except KeyError:
            return {"status": "failed", "reason": "zip_member_not_found"}
        except (BadZipFile, ValueError):
            return {"status": "failed", "reason": "zip_member_invalid_pdf"}
        except Exception as exc:
            return {"status": "failed", "reason": f"zip_member_read_error:{type(exc).__name__}"}
        return {
            "status": "success",
            "data": pdf_bytes,
            "source_kind": resolved_source_kind,
            "archive_file_id": resolved_archive_file_id,
            "archive_member_path": resolved_archive_member_path,
        }

    try:
        pdf_bytes = download_file_bytes(drive, file_id, logger=logger)
    except Exception as exc:
        return {"status": "failed", "reason": f"{type(exc).__name__}: {exc}"}

    return {
        "status": "success",
        "data": pdf_bytes,
        "source_kind": resolved_source_kind,
        "archive_file_id": resolved_archive_file_id,
        "archive_member_path": resolved_archive_member_path,
    }


__all__ = ["download_pdf_bytes_for_index_item", "resolve_index_pdf_source"]
