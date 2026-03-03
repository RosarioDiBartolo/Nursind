from __future__ import annotations

import os
import threading
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

from src.drive_service.auth_service import load_creds
from src.drive_service.fs_utils import ensure_dir
from src.drive_service.index import MapIndex
from src.drive_service.index_runtime import (
    maybe_flush_indexes,
    resolve_output_path,
    update_index_meta,
)
from src.drive_service.io_json import write_json
from src.drive_service.logging_utils import get_logger, setup_logging
from src.drive_service.schema import IndexFile, Outputs
from src.drive_service.text_extraction_csv import (
    build_employee_csv_rel_path,
    build_text_extraction_row,
    load_text_extraction_rows,
    prune_stale_text_extraction_docs,
    write_text_extraction_rows,
)

from .options import ExtractDocumentsFromIndexOptions
from .planning import build_initial_stats, collect_docs
from .workers import download_pdf_bytes, extract_and_write


def _apply_result(
    result: dict,
    stats: dict,
    items: list,
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


def _flush_progress(
    *,
    processed: int,
    flush_every: int,
    log_every: int,
    start_ts: float,
    included_index,
    excluded_index,
    included_map: dict[str, IndexFile],
    excluded_map: dict[str, IndexFile],
    included_path: str,
    excluded_path: str,
    logger,
) -> None:
    maybe_flush_indexes(
        processed=processed,
        flush_every=flush_every,
        log_every=log_every,
        start_ts=start_ts,
        included_index=included_index,
        excluded_index=excluded_index,
        included_map=included_map,
        excluded_map=excluded_map,
        included_path=included_path,
        excluded_path=excluded_path,
        logger=logger,
    )


def run_extraction(options: ExtractDocumentsFromIndexOptions) -> int:
    setup_logging(options.verbose)
    logger = get_logger()
    logger.info("Starting document extraction pipeline at out=%s", options.out)

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

    logger.info("Queued %s files for extraction", stats["queued"])
    if not skip_excluded:
        logger.info("Reprocess-excluded enabled")
    if skip_included:
        logger.info("Skip-included enabled")
    logger.info(
        "Workers: download=%s, extract=%s, max_in_flight=%s",
        download_workers,
        extract_workers,
        max_in_flight,
    )

    included_index = MapIndex.generate_index(source.root_id, source.employee_count, included_map)
    excluded_index = MapIndex.generate_index(source.root_id, source.employee_count, excluded_map)

    items: list[dict] = []
    t0 = time.time()
    processed = 0
    stop_event = threading.Event()

    if docs:
        needs_drive = any(not bool(doc.get("local")) for doc in docs)
        if needs_drive:
            logger.info("Loading credentials...")
            creds = load_creds()
            logger.info("Credentials loaded")
        else:
            logger.info("No remote documents queued; skipping credential load")
            creds = None

        extract_futures: dict = {}
        with ThreadPoolExecutor(max_workers=download_workers) as download_pool, ProcessPoolExecutor(
            max_workers=extract_workers
        ) as extract_pool:
            download_futures = [
                download_pool.submit(download_pdf_bytes, creds, doc, stop_event)
                for doc in docs
            ]
            try:
                for future in as_completed(download_futures):
                    try:
                        download_result = future.result()
                    except Exception as exc:
                        download_result = {
                            "status": "failed",
                            "stage": "download",
                            "reason": f"{type(exc).__name__}: {exc}",
                            "doc": {},
                        }

                    if download_result["status"] != "success":
                        processed += 1
                        stats["processed"] = processed
                        _apply_result(
                            download_result,
                            stats,
                            items,
                            included_map,
                            excluded_map,
                            text_rows_by_file_id,
                        )
                        _flush_progress(
                            processed=processed,
                            flush_every=flush_every,
                            log_every=log_every,
                            start_ts=t0,
                            included_index=included_index,
                            excluded_index=excluded_index,
                            included_map=included_map,
                            excluded_map=excluded_map,
                            included_path=included_path,
                            excluded_path=excluded_path,
                            logger=logger,
                        )
                        continue

                    while len(extract_futures) >= max_in_flight:
                        done = next(as_completed(extract_futures))
                        doc_info = extract_futures.pop(done, {})
                        try:
                            result = done.result()
                        except Exception as exc:
                            result = {
                                "status": "failed",
                                "stage": "extract",
                                "reason": f"{type(exc).__name__}: {exc}",
                                "doc": doc_info,
                            }
                        processed += 1
                        stats["processed"] = processed
                        _apply_result(
                            result,
                            stats,
                            items,
                            included_map,
                            excluded_map,
                            text_rows_by_file_id,
                        )
                        _flush_progress(
                            processed=processed,
                            flush_every=flush_every,
                            log_every=log_every,
                            start_ts=t0,
                            included_index=included_index,
                            excluded_index=excluded_index,
                            included_map=included_map,
                            excluded_map=excluded_map,
                            included_path=included_path,
                            excluded_path=excluded_path,
                            logger=logger,
                        )

                    doc_info = download_result["doc"]
                    extract_future = extract_pool.submit(
                        extract_and_write,
                        download_result["data"],
                        doc_info,
                        options.out,
                        options.min_normal_score,
                        options.min_score_delta,
                    )
                    extract_futures[extract_future] = doc_info

                for done in as_completed(list(extract_futures)):
                    doc_info = extract_futures.pop(done, {})
                    try:
                        result = done.result()
                    except Exception as exc:
                        result = {
                            "status": "failed",
                            "stage": "extract",
                            "reason": f"{type(exc).__name__}: {exc}",
                            "doc": doc_info,
                        }
                    processed += 1
                    stats["processed"] = processed
                    _apply_result(
                        result,
                        stats,
                        items,
                        included_map,
                        excluded_map,
                        text_rows_by_file_id,
                    )
                    _flush_progress(
                        processed=processed,
                        flush_every=flush_every,
                        log_every=log_every,
                        start_ts=t0,
                        included_index=included_index,
                        excluded_index=excluded_index,
                        included_map=included_map,
                        excluded_map=excluded_map,
                        included_path=included_path,
                        excluded_path=excluded_path,
                        logger=logger,
                    )
            except KeyboardInterrupt:
                stop_event.set()
                logger.warning("Interrupted by user, flushing indexes...")

    included_index.files = dict(included_map)
    excluded_index.files = dict(excluded_map)
    update_index_meta(included_index)
    update_index_meta(excluded_index)
    included_index.save_index(included_path)
    excluded_index.save_index(excluded_path)
    employee_csv_files = write_text_extraction_rows(options.out, text_rows_by_file_id)
    prune_stale_text_extraction_docs(options.out, text_rows_by_file_id)

    payload = {
        "source_index": index_path,
        "included_index": included_path,
        "excluded_index": excluded_path,
        "out_dir": options.out,
        "employee_csv_files": employee_csv_files,
        "stats": stats,
        "items": items,
        "duration_s": round(time.time() - t0, 3),
    }
    ensure_dir(os.path.dirname(report_path) or ".")
    write_json(report_path, payload)
    logger.info("Report saved to %s", report_path)
    logger.info("Done in %.1fs", time.time() - t0)
    return 0


__all__ = ["_apply_result", "_flush_progress", "run_extraction"]
