import os
import re
import time
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from .map_index_service import MapIndex
from .names import safe_name 

from . import config
from .auth_service import load_creds
from .drive_client import get_drive_service
from .downloads import download_pdf_stream
from .fs_utils import ensure_dir 
from .io_json import write_json
from .logging_utils import setup_logging, get_logger
 
from .schema import IndexFile, Outputs
from parsing import parse_pdf

logger = None
DEFAULT_SKIP_REASONS = {
    "PdfminerException: No /Root object! - Is this really a PDF?",
}


def _doc_attr(doc, name: str):
    if hasattr(doc, name):
        return getattr(doc, name)
    if isinstance(doc, dict):
        return doc.get(name)
    return None


def _update_index_meta(index: MapIndex) -> None:
    index.generated_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    employees = {
        item.employee for item in index.files.values() if getattr(item, "employee", None)
    }
    index.employee_count = len(employees)
    index.total_files = len(index.files)


def _compile_reason_regexes(patterns: list[str]) -> list[re.Pattern]:
    compiled = []
    for pattern in patterns:
        try:
            compiled.append(re.compile(pattern))
        except re.error as exc:
            raise ValueError(f"Invalid --skip-reason-regex pattern '{pattern}': {exc}") from exc
    return compiled


def _matches_reason(
    reason: str | None,
    *,
    exact: set[str],
    contains: list[str],
    regexes: list[re.Pattern],
) -> bool:
    if not reason:
        return False
    if reason in exact:
        return True
    if any(token in reason for token in contains):
        return True
    if any(rx.search(reason) for rx in regexes):
        return True
    return False


def _should_skip_by_reason(
    reason: str | None,
    *,
    exact: set[str],
    contains: list[str],
    regexes: list[re.Pattern],
) -> bool:
    return _matches_reason(reason, exact=exact, contains=contains, regexes=regexes)


def process_document(
    creds,
    doc: IndexFile,
    out_dir: str,
    stop_event: threading.Event,
    skip_existing_outputs: bool = False,
):
    drive = get_drive_service(creds)
    emp_name = _doc_attr(doc, "employee") or "unknown"
    emp_id = _doc_attr(doc, "employee_id")
    file_id = _doc_attr(doc, "file_id")
    file_name = _doc_attr(doc, "file_name") or file_id or "unknown.pdf"
    reason = _doc_attr(doc, "reason")


    #Only for when source is excluded
    if reason =="PdfminerException: No /Root object! - Is this really a PDF?":
        return {
            "status": "failed",
            "employee": emp_name,
            "employee_id": emp_id,
            "file_id": file_id,
            "file_name": file_name,
            "reason": reason,
        }
    
    if stop_event.is_set():
        return {
            "status": "failed",
            "employee": emp_name,
            "employee_id": emp_id,
            "file_id": file_id,
            "file_name": file_name,
            "reason": "cancelled",
        }

    if not file_id:
        return {
            "status": "failed",
            "employee": emp_name,
            "employee_id": emp_id,
            "file_id": file_id,
            "file_name": file_name,
            "reason": "missing file_id",
        }

    safe_emp = safe_name(emp_name)
    base_name = safe_name(file_name)
    if not base_name.lower().endswith(".pdf"):
        base_name = f"{base_name}.pdf"
    file_tag = f"{os.path.splitext(base_name)[0]}__{file_id[:8]}"

    emp_dir = os.path.join(out_dir, safe_emp)
    file_dir = os.path.join(emp_dir, file_tag)
    days_path = os.path.join(file_dir, "days.csv")
    pairs_path = os.path.join(file_dir, "pairs.csv")
    totals_path = os.path.join(file_dir, "totals.json")
    report_path = os.path.join(file_dir, "report.json")

    rel_days = os.path.join(safe_emp, file_tag, "days.csv")
    rel_pairs = os.path.join(safe_emp, file_tag, "pairs.csv")
    rel_totals = os.path.join(safe_emp, file_tag, "totals.json")
    rel_report = os.path.join(safe_emp, file_tag, "report.json")

    if skip_existing_outputs and all(
        os.path.exists(path) for path in (days_path, pairs_path, totals_path, report_path)
    ):
        return {
            "status": "success",
            "employee": emp_name,
            "employee_id": emp_id,
            "file_id": file_id,
            "file_name": file_name,
            "outputs": {
                "days_csv": rel_days,
                "pairs_csv": rel_pairs,
                "totals_json": rel_totals,
                "report_json": rel_report,
            },
            "skipped": True,
        }

    try:
        if stop_event.is_set():
            raise RuntimeError("cancelled")
        stream = download_pdf_stream(drive, file_id, logger=logger)
        if stop_event.is_set():
            raise RuntimeError("cancelled")
        parsed = parse_pdf(stream)

        ensure_dir(file_dir)
        parsed.days_df.to_csv(days_path, index=False)
        parsed.pairs_df.to_csv(pairs_path, index=False, date_format="%Y-%m-%d %H:%M:%S")
        write_json(totals_path, parsed.totals)
        write_json(
            report_path,
            {
                "meta": parsed.meta,
                "totals": parsed.totals,
                "validation": parsed.validation,
            },
        )
        return {
            "status": "success",
            "employee": emp_name,
            "employee_id": emp_id,
            "file_id": file_id,
            "file_name": file_name,
            "outputs": {
                "days_csv": rel_days,
                "pairs_csv": rel_pairs,
                "totals_json": rel_totals,
                "report_json": rel_report,
            },
        }
    except Exception as exc:
        if isinstance(exc, RuntimeError) and str(exc) == "cancelled":
            reason = "cancelled"
        else:
            reason = f"{type(exc).__name__}: {exc}"
        return {
            "status": "failed",
            "employee": emp_name,
            "employee_id": emp_id,
            "file_id": file_id,
            "file_name": file_name,
            "reason": reason,
        }
    finally:
        try:
            if "stream" in locals():
                stream.close()
        except Exception:
            pass


def _resolve_report_path(out_dir: str, report: str) -> str:
    if os.path.isabs(report):
        return report
    return os.path.join(out_dir, report)


 


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="downloads", help="Output directory (default: downloads)")
    parser.add_argument(
        "--index",
        default="index.scan.json",
        help="Input index filename (resolved under --out when relative)",
    )
    parser.add_argument(
        "--allow-legacy-index",
        action="store_true",
        help="Allow legacy index format with files as array (default: false)",
    )
    parser.add_argument(
        "--excluded",
        default="excluded.index.json",
        help="Excluded index output filename (default: excluded.index.json)",
    )
    parser.add_argument(
        "--included",
        default="included.index.json",
        help="Included index output filename (default: included.index.json)",
    )
    # Reason filtering disabled for now.
    # parser.add_argument("--skip-reason", action="append", default=[], help="...")
    # parser.add_argument("--skip-reason-contains", action="append", default=[], help="...")
    # parser.add_argument("--skip-reason-regex", action="append", default=[], help="...")
    # parser.add_argument("--only-reason", action="append", default=[], help="...")
    # parser.add_argument("--only-reason-contains", action="append", default=[], help="...")
    # parser.add_argument("--only-reason-regex", action="append", default=[], help="...")
 
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument(
        "--skip-existing-outputs",
        action="store_true",
        help="Skip processing when all output files already exist (default: false)",
    )
    parser.add_argument(
        "--skip-included",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip processing when file is already in included index (default: true)",
    )
    parser.add_argument(
        "--reprocess-included",
        action="store_true",
        help="Reprocess files already in included index (overrides --skip-included)",
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    setup_logging(args.verbose)
    global logger
    logger = get_logger()
    logger.info("Starting fetch_index at out=%s", args.out )
    config.validate_env()
    ensure_dir(args.out)

    logger.info("Loading credentials...")
    creds = load_creds()
    logger.info("Credentials loaded")

 
    index_path = _resolve_report_path(args.out, args.index)
    excluded_path = _resolve_report_path(args.out, args.excluded)
    included_path = _resolve_report_path(args.out, args.included)

    source = MapIndex.load_index(
        index_path,
        strict=False,
        allow_legacy=args.allow_legacy_index,
    )
    existing_excluded = MapIndex.load_index(excluded_path, strict=False, allow_legacy=True)
    existing_included = MapIndex.load_index(included_path, strict=False, allow_legacy=True)
    excluded_map: dict[str, IndexFile] = dict(existing_excluded.files)
    included_map: dict[str, IndexFile] = dict(existing_included.files)
    skip_included = args.skip_included and not args.reprocess_included
    total_docs = len(source.files)
    docs = [
        doc
        for doc in source.files.values()
        if _doc_attr(doc, "file_id") not in excluded_map
        and (
            not skip_included
            or _doc_attr(doc, "file_id") not in included_map
        )
    ]
    # Reason filtering disabled for now.
    skip_exact = set()
    skip_contains: list[str] = []
    skip_regexes: list[re.Pattern] = []
    if args.skip_existing_outputs:
        logger.info("Skip-existing-outputs enabled")
    if skip_included:
        logger.info("Skip-included enabled")

    filtered_docs = docs

    skipped_docs = total_docs - len(filtered_docs)
    logger.info("Found %d documents to process", len(filtered_docs))
    if skipped_docs:
        logger.info("Skipped %d documents already present in excluded index", skipped_docs)
    if not filtered_docs:
        excluded_index = MapIndex.generate_index(source.root_id, source.employee_count, excluded_map)
        included_index = MapIndex.generate_index(source.root_id, source.employee_count, included_map)
        _update_index_meta(included_index)
        _update_index_meta(excluded_index)
        included_index.save_index(included_path)
        excluded_index.save_index(excluded_path)
        logger.info("No documents to process... exiting.")
        return
    


    t0 = time.time()
    flush_every = 25
    processed_since_flush = 0
    stop_event = threading.Event()

    interrupted = False


    excluded_index = MapIndex.generate_index(source.root_id, source.employee_count, excluded_map)
    included_index = MapIndex.generate_index(source.root_id, source.employee_count, included_map)
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        logger.info(
            "Submitting %d tasks to ThreadPoolExecutor (workers=%d)",
            len(filtered_docs),
            args.workers,
        )
        futures = [
            pool.submit(process_document, creds, doc, args.out, stop_event, args.skip_existing_outputs)
            for doc in filtered_docs
        ]
        try:
            for i, f in enumerate(as_completed(futures), 1):
                result = f.result()

                file_id = result.get("file_id")
                if result["status"] == "success":
                    if file_id:
                        included_map[file_id] = IndexFile(
                            employee=result.get("employee"),
                            employee_id=result.get("employee_id"),
                            file_id=file_id,
                            file_name=result.get("file_name"),
                            outputs=Outputs(
                                days_csv=result["outputs"]["days_csv"],
                                pairs_csv=result["outputs"]["pairs_csv"],
                                totals_json=result["outputs"]["totals_json"],
                                report_json=result["outputs"]["report_json"],
                            ),
                        )

                    
                else:    
                    if file_id:
                        excluded_map[file_id] = IndexFile(
                            employee=result.get("employee") or "unknown",
                            employee_id=result.get("employee_id"),
                            file_id=file_id,
                            file_name=result.get("file_name"),
                            reason=result.get("reason"),
                            type="file",
                        )
                    
                processed_since_flush += 1
                if processed_since_flush >= flush_every:
                    excluded_index.files = dict(excluded_map)
                    included_index.files = dict(included_map)
                    _update_index_meta(included_index)
                    _update_index_meta(excluded_index)
                    logger.info("Updating indexes, excluded: %s included: %s", excluded_path, included_path)
                    included_index.save_index(included_path)
                    excluded_index.save_index(excluded_path)
                    processed_since_flush = 0
                if i % 25 == 0 or i == len(futures):
                    logger.info("Progress %s/%s files", i, len(futures))
        except KeyboardInterrupt:
            stop_event.set()
            for future in futures:
                future.cancel()
            logger.warning("Interrupted by user, flushing report...")
            interrupted = True

    if interrupted:
        logger.info("Stopped after %.1fs", time.time() - t0)
    else:
        logger.info("Done in %.1fs", time.time() - t0)

    excluded_index.files = dict(excluded_map)
    included_index.files = dict(included_map)
    _update_index_meta(included_index)
    _update_index_meta(excluded_index)
    included_index.save_index(included_path)
    excluded_index.save_index(excluded_path)

      


if __name__ == "__main__":
    main()
