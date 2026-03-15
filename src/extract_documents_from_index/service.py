from __future__ import annotations

import os
import threading
import time
from typing import Any, Iterable

from src.drive_service.fs_utils import ensure_dir
from src.drive_service.index import MapIndex
from src.drive_service.index_runtime import resolve_output_path, update_index_meta
from src.drive_service.io_json import write_json
from src.drive_service.schema import IndexFile, Outputs
from src.drive_service.text_extraction_csv import (
    build_employee_csv_rel_path,
    build_text_extraction_row,
    load_text_extraction_rows,
    prune_stale_text_extraction_docs,
    write_text_extraction_rows,
)
from src.reporting import build_stage_report

from .options import ExtractDocumentsFromIndexOptions
from .planning import build_initial_stats, collect_docs
from .workers import download_pdf_bytes, extract_and_write


def _normalize_failure(doc_info: dict[str, Any], exc: Exception, *, stage: str) -> dict[str, Any]:
    return {
        "status": "failed",
        "stage": stage,
        "reason": f"{type(exc).__name__}: {exc}",
        "doc": doc_info,
    }


def _run_one_index_document(
    doc_info: dict[str, Any],
    *,
    creds,
    out_dir: str,
    min_normal_score: float,
    min_score_delta: float,
    stop_event: threading.Event | None = None,
) -> dict[str, Any]:
    stop_flag = stop_event or threading.Event()
    download_result = download_pdf_bytes(creds, doc_info, stop_flag)
    if download_result.get("status") != "success":
        return dict(download_result)
    try:
        return extract_and_write(
            download_result["data"],
            doc_info,
            out_dir,
            min_normal_score,
            min_score_delta,
        )
    except Exception as exc:  # pragma: no cover - defensive parity with runtime
        return _normalize_failure(doc_info, exc, stage="extract")


def process_one_index_document(
    doc_info: dict[str, Any],
    *,
    creds,
    out_dir: str,
    min_normal_score: float,
    min_score_delta: float,
) -> dict[str, Any]:
    stop_event = threading.Event()
    result = _run_one_index_document(
        doc_info,
        creds=creds,
        out_dir=out_dir,
        min_normal_score=min_normal_score,
        min_score_delta=min_score_delta,
        stop_event=stop_event,
    )
    if result.get("status") == "success":
        return {
            "status": "ok",
            "error": None,
            "error_code": None,
            "source_file_id": result.get("file_id"),
            "source_file_name": result.get("file_name"),
            "source_employee": result.get("employee"),
            "doc_json": result.get("doc_json"),
            "selected_mode": result.get("selected_mode"),
            "source_text_ref": result.get("source_text_ref"),
        }

    doc = result.get("doc") or {}
    reason = str(result.get("reason") or "processing_failed")
    return {
        "status": "error",
        "error": reason,
        "error_code": str(result.get("stage") or "processing_error"),
        "source_file_id": doc.get("file_id"),
        "source_file_name": doc.get("file_name"),
        "source_employee": doc.get("employee"),
        "doc_json": None,
        "selected_mode": None,
        "source_text_ref": doc.get("source_text_ref"),
    }


def process_many_index_documents(
    docs: Iterable[dict[str, Any]],
    *,
    creds,
    out_dir: str,
    min_normal_score: float,
    min_score_delta: float,
) -> dict[str, Any]:
    normalized_docs = list(docs)
    items: list[dict[str, Any]] = []
    issues: list[dict[str, Any]] = []
    stats = {
        "files_total": len(normalized_docs),
        "files_processed": 0,
        "files_error": 0,
    }

    for doc_info in normalized_docs:
        result = process_one_index_document(
            doc_info,
            creds=creds,
            out_dir=out_dir,
            min_normal_score=min_normal_score,
            min_score_delta=min_score_delta,
        )
        items.append(dict(result))
        if result.get("status") == "ok":
            stats["files_processed"] += 1
            continue
        stats["files_error"] += 1
        issues.append(
            {
                "code": str(result.get("error_code") or "processing_error"),
                "source_file_id": str(result.get("source_file_id") or ""),
                "message": str(result.get("error") or "processing_failed"),
            }
        )
    return build_stage_report(
        stage="extract_documents_from_index",
        inputs={
            "output_dir": os.path.abspath(out_dir),
            "min_normal_score": float(min_normal_score),
            "min_score_delta": float(min_score_delta),
        },
        outputs={"output_dir": os.path.abspath(out_dir)},
        stats=stats,
        row_totals={"items": len(items), "issues": len(issues)},
        items=items,
        issues=issues,
    )


def prepare_extraction_run(options: ExtractDocumentsFromIndexOptions) -> dict[str, Any]:
    ensure_dir(options.out)
    index_path = options.index if os.path.isabs(options.index) else os.path.abspath(options.index)
    included_path = resolve_output_path(options.out, options.included)
    excluded_path = resolve_output_path(options.out, options.excluded)
    report_path = resolve_output_path(options.out, options.report)

    source = MapIndex.load_index(index_path, strict=True)
    existing_included = MapIndex.load_index(included_path, strict=False)
    existing_excluded = MapIndex.load_index(excluded_path, strict=False)
    included_map: dict[str, IndexFile] = dict(existing_included.files)
    excluded_map: dict[str, IndexFile] = dict(existing_excluded.files)
    text_rows_by_file_id = {
        file_id: row
        for file_id, row in load_text_extraction_rows(options.out).items()
        if file_id in included_map
    }

    skip_included = options.skip_included and not options.reprocess_included
    skip_excluded = not options.reprocess_excluded
    download_workers = max(
        1,
        options.download_workers if options.download_workers is not None else options.workers,
    )
    extract_workers = max(1, options.extract_workers)
    max_in_flight = max(1, options.max_in_flight)
    flush_every = max(1, options.flush_every)
    log_every = max(1, options.log_every)

    stats = build_initial_stats(len(source.files))
    docs = collect_docs(
        source.files,
        included_map,
        excluded_map,
        existing_text_rows=text_rows_by_file_id,
        skip_included=skip_included,
        skip_excluded=skip_excluded,
        limit=options.limit,
        stats=stats,
    )
    included_index = MapIndex.generate_index(source.root_id, source.employee_count, included_map)
    excluded_index = MapIndex.generate_index(source.root_id, source.employee_count, excluded_map)

    return {
        "index_path": index_path,
        "included_path": included_path,
        "excluded_path": excluded_path,
        "report_path": report_path,
        "source": source,
        "included_index": included_index,
        "excluded_index": excluded_index,
        "included_map": included_map,
        "excluded_map": excluded_map,
        "text_rows_by_file_id": text_rows_by_file_id,
        "download_workers": download_workers,
        "extract_workers": extract_workers,
        "max_in_flight": max_in_flight,
        "flush_every": flush_every,
        "log_every": log_every,
        "skip_included": skip_included,
        "skip_excluded": skip_excluded,
        "stats": stats,
        "docs": docs,
    }


def apply_extraction_result(
    result: dict[str, Any],
    *,
    stats: dict[str, Any],
    items: list[dict[str, Any]],
    included_map: dict[str, IndexFile],
    excluded_map: dict[str, IndexFile],
    text_rows_by_file_id: dict[str, dict[str, object]],
) -> None:
    items.append(dict(result))
    if result["status"] == "success":
        stats["succeeded"] += 1
        if result.get("selected_mode") == "vertical":
            stats["used_vertical"] += 1
        file_id = result.get("file_id")
        if file_id:
            employee = result.get("employee") or "unknown"
            text_rows_by_file_id[file_id] = build_text_extraction_row(
                employee=employee,
                employee_id=result.get("employee_id"),
                file_id=file_id,
                file_name=result.get("file_name"),
                drive_path=result.get("drive_path"),
                source_kind=result.get("source_kind"),
                archive_file_id=result.get("archive_file_id"),
                archive_member_path=result.get("archive_member_path"),
                source_text_ref=result.get("source_text_ref"),
                doc_json=result.get("doc_json"),
                has_text_layer=result.get("has_text_layer"),
                selected_mode=result.get("selected_mode"),
                tried_vertical=result.get("tried_vertical"),
                normal_quality=result.get("normal_quality"),
                vertical_quality=result.get("vertical_quality"),
            )
            included_map[file_id] = IndexFile(
                employee=employee,
                employee_id=result.get("employee_id"),
                local=bool(result.get("local")),
                file_id=file_id,
                file_name=result.get("file_name"),
                drive_path=result.get("drive_path"),
                outputs=Outputs(
                    text_csv=build_employee_csv_rel_path(employee),
                    doc_json=result.get("doc_json"),
                ),
            )
            excluded_map.pop(file_id, None)
        return

    stats["failed"] += 1
    stage = result.get("stage")
    if stage == "download":
        stats["download_failed"] += 1
    elif stage == "extract":
        stats["extract_failed"] += 1
        if result.get("reason") == "missing_text_layer":
            stats["excluded_missing_text_layer"] += 1

    doc_info = result.get("doc") or {}
    file_id = doc_info.get("file_id")
    if file_id:
        text_rows_by_file_id.pop(file_id, None)
        included_map.pop(file_id, None)
        excluded_map[file_id] = IndexFile(
            employee=doc_info.get("employee") or "unknown",
            employee_id=doc_info.get("employee_id"),
            local=bool(doc_info.get("local")),
            file_id=file_id,
            file_name=doc_info.get("file_name"),
            drive_path=doc_info.get("drive_path"),
            reason=result.get("reason"),
            type="file",
        )


def finalize_extraction_run(
    *,
    options: ExtractDocumentsFromIndexOptions,
    state: dict[str, Any],
    items: list[dict[str, Any]],
    start_ts: float,
) -> dict[str, Any]:
    included_index = state["included_index"]
    excluded_index = state["excluded_index"]
    included_map = state["included_map"]
    excluded_map = state["excluded_map"]
    text_rows_by_file_id = state["text_rows_by_file_id"]

    included_index.files = dict(included_map)
    excluded_index.files = dict(excluded_map)
    update_index_meta(included_index)
    update_index_meta(excluded_index)
    included_index.save_index(state["included_path"])
    excluded_index.save_index(state["excluded_path"])
    employee_csv_files = write_text_extraction_rows(options.out, text_rows_by_file_id)
    prune_stale_text_extraction_docs(options.out, text_rows_by_file_id)

    payload = {
        **build_stage_report(
            stage="extract_documents_from_index",
            inputs={
                "source_index": state["index_path"],
                "output_dir": options.out,
                "skip_included": state["skip_included"],
                "skip_excluded": state["skip_excluded"],
            },
            outputs={
                "included_index": state["included_path"],
                "excluded_index": state["excluded_path"],
                "employee_csv_files": employee_csv_files,
                "report_json": state["report_path"],
            },
            stats=state["stats"],
            row_totals={"items": len(items), "issues": int(state["stats"].get("failed") or 0)},
            items=items,
            issues=[
                {
                    "code": str(item.get("stage") or "processing_error"),
                    "source_file_id": str((item.get("doc") or {}).get("file_id") or ""),
                    "message": str(item.get("reason") or "processing_failed"),
                }
                for item in items
                if item.get("status") != "success"
            ],
        ),
        "duration_s": round(time.time() - start_ts, 3),
    }
    ensure_dir(os.path.dirname(state["report_path"]) or ".")
    write_json(state["report_path"], payload)
    return payload


__all__ = [
    "apply_extraction_result",
    "finalize_extraction_run",
    "prepare_extraction_run",
    "process_many_index_documents",
    "process_one_index_document",
]
