import os
import io
import json
import time
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from googleapiclient.http import MediaIoBaseDownload

from . import config
from .auth_service import load_creds
from .drive_client import get_drive_service
from .fs_utils import ensure_dir
from .logging_utils import setup_logging, get_logger
from cartellino_parser.parser import parse_pdf

logger = get_logger()


def load_manifest(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_report(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def safe_name(name: str, max_len: int = 120) -> str:
    name = name.strip()
    name = name.replace("\\", "_").replace("/", "_")
    name = name.replace(":", "_").replace("*", "_")
    name = name.replace("?", "_").replace('"', "_")
    name = name.replace("<", "_").replace(">", "_").replace("|", "_")
    if len(name) > max_len:
        name = name[:max_len]
    return name or "unnamed"


def download_pdf_stream(drive, file_id: str) -> io.BytesIO:
    request = drive.files().get_media(fileId=file_id, supportsAllDrives=True)
    stream = io.BytesIO()
    downloader = MediaIoBaseDownload(stream, request, chunksize=4 * 1024 * 1024)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    stream.seek(0)
    return stream


def _write_json(path: str, payload: dict):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def _write_report(path: str, root_id: str | None, employees: list[dict]):
    _write_json(
        path,
        {
            "root_id": root_id,
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "employee_count": len(employees),
            "employees": employees,
        },
    )


def _normalize_name(value: str | None) -> str:
    if not value:
        return "unknown"
    return " ".join(value.strip().lower().split())


def _employee_key(emp: dict) -> str:
    emp_id = emp.get("employee_id") or emp.get("id")
    if emp_id:
        return f"id:{emp_id}"
    return f"name:{_normalize_name(emp.get('employee') or emp.get('name'))}"


def _employee_name(emp: dict) -> str:
    return emp.get("employee") or emp.get("name") or "unknown"


def _upsert_item(items: list[dict], item: dict):
    file_id = item.get("file_id")
    if not file_id:
        items.append(item)
        return
    for i, existing in enumerate(items):
        if existing.get("file_id") == file_id:
            items[i] = item
            return
    items.append(item)


def _collect_cached_ids(employees: list[dict]) -> set[str]:
    cached: set[str] = set()
    for emp in employees:
        for section in ("included", "skipped"):
            for item in emp.get(section, []):
                file_id = item.get("file_id")
                if file_id:
                    cached.add(file_id)
    return cached


def _build_base_employees(employees: list[dict]) -> dict[str, dict]:
    base: dict[str, dict] = {}
    for emp in employees:
        key = _employee_key(emp)
        base[key] = {
            "employee": emp.get("employee") or emp.get("name") or "unknown",
            "employee_id": emp.get("employee_id") or emp.get("id"),
            "included": [],
            "skipped": list(emp.get("skipped", [])),
            "excluded_folders": list(emp.get("excluded_folders", [])),
        }
    return base


def _build_name_index(base: dict[str, dict]) -> dict[str, str]:
    index: dict[str, str] = {}
    for key, emp in base.items():
        index[_normalize_name(emp.get("employee"))] = key
    return index


def _merge_report_into_base(base: dict[str, dict], report: dict):
    if not report:
        return
    name_index = _build_name_index(base)
    if "employees" in report:
        for emp in report.get("employees", []):
            key = _employee_key(emp)
            if key not in base:
                name_key = name_index.get(_normalize_name(_employee_name(emp)))
                if name_key:
                    key = name_key
            if key not in base:
                base[key] = {
                    "employee": emp.get("employee") or emp.get("name") or "unknown",
                    "employee_id": emp.get("employee_id") or emp.get("id"),
                    "included": [],
                    "skipped": [],
                    "excluded_folders": list(emp.get("excluded_folders", [])),
                }
            for item in emp.get("included", []):
                _upsert_item(base[key]["included"], item)
            for item in emp.get("skipped", []):
                _upsert_item(base[key]["skipped"], item)
        return
    if "files" in report:
        for item in report.get("files", []):
            emp_name = item.get("employee") or "unknown"
            name_key = name_index.get(_normalize_name(emp_name))
            key = name_key or f"name:{_normalize_name(emp_name)}"
            if key not in base:
                base[key] = {
                    "employee": emp_name,
                    "employee_id": None,
                    "included": [],
                    "skipped": [],
                    "excluded_folders": [],
                }
            if item.get("status") == "success":
                _upsert_item(
                    base[key]["included"],
                    {
                        "file_id": item.get("file_id"),
                        "file_name": item.get("file_name"),
                        "outputs": item.get("outputs"),
                    },
                )
            else:
                _upsert_item(
                    base[key]["skipped"],
                    {
                        "file_id": item.get("file_id"),
                        "file_name": item.get("file_name"),
                        "reason": item.get("reason"),
                    },
                )


def _finalize_employees(base: dict[str, dict], order: list[dict]) -> list[dict]:
    output: list[dict] = []
    for emp in order:
        key = _employee_key(emp)
        data = base.get(key)
        if not data:
            continue
        data["counts"] = {
            "included": len(data.get("included", [])),
            "skipped_files": len(data.get("skipped", [])),
            "excluded_folders": len(data.get("excluded_folders", [])),
        }
        output.append(data)
    for key, data in base.items():
        if data in output:
            continue
        data["counts"] = {
            "included": len(data.get("included", [])),
            "skipped_files": len(data.get("skipped", [])),
            "excluded_folders": len(data.get("excluded_folders", [])),
        }
        output.append(data)
    return output


def process_document(creds, employee: dict, doc: dict, out_dir: str, stop_event: threading.Event):
    drive = get_drive_service(creds)
    emp_name = _employee_name(employee)
    file_id = doc.get("file_id")
    file_name = doc.get("file_name") or file_id or "unknown.pdf"

    if stop_event.is_set():
        return {
            "status": "failed",
            "employee": emp_name,
            "employee_id": employee.get("employee_id") or employee.get("id"),
            "file_id": file_id,
            "file_name": file_name,
            "reason": "cancelled",
        }

    if not file_id:
        return {
            "status": "failed",
            "employee": emp_name,
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
    ensure_dir(file_dir)

    try:
        if stop_event.is_set():
            raise RuntimeError("cancelled")
        stream = download_pdf_stream(drive, file_id)
        if stop_event.is_set():
            raise RuntimeError("cancelled")
        parsed = parse_pdf(stream)

        days_path = os.path.join(file_dir, "days.csv")
        pairs_path = os.path.join(file_dir, "pairs.csv")
        totals_path = os.path.join(file_dir, "totals.json")
        report_path = os.path.join(file_dir, "report.json")

        parsed.days_df.to_csv(days_path, index=False)
        parsed.pairs_df.to_csv(pairs_path, index=False)
        _write_json(totals_path, parsed.totals)
        _write_json(
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
            "employee_id": employee.get("employee_id") or employee.get("id"),
            "file_id": file_id,
            "file_name": file_name,
            "outputs": {
                "days_csv": days_path,
                "pairs_csv": pairs_path,
                "totals_json": totals_path,
                "report_json": report_path,
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
            "employee_id": employee.get("employee_id") or employee.get("id"),
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--out", default="downloads")
    parser.add_argument("--report", default="report.json")
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    setup_logging(args.verbose)
    config.validate_env()
    ensure_dir(args.out)

    creds = load_creds()
    manifest = load_manifest(args.manifest)

    employees = manifest.get("employees") or []

    report_path = args.report
    if not os.path.isabs(report_path):
        report_path = os.path.join(args.out, report_path)
    report = load_report(report_path)
    base_employees = _build_base_employees(employees)
    _merge_report_into_base(base_employees, report)
    cached = _collect_cached_ids(list(base_employees.values()))

    docs = []
    for emp in employees:
        for doc in emp.get("included", []):
            file_id = doc.get("file_id")
            if file_id and file_id in cached:
                continue
            docs.append((emp, doc))

    t0 = time.time()
    flush_every = 25
    processed_since_flush = 0
    stop_event = threading.Event()

    interrupted = False
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [
            pool.submit(process_document, creds, emp, doc, args.out, stop_event)
            for emp, doc in docs
        ]
        try:
            for i, f in enumerate(as_completed(futures), 1):
                result = f.result()
                if result["status"] == "failed":
                    logger.debug("Failed %s (%s)", result["file_name"], result["reason"])
                if result.get("employee_id"):
                    emp_key = f"id:{result.get('employee_id')}"
                else:
                    emp_key = f"name:{_normalize_name(result.get('employee'))}"
                if emp_key not in base_employees:
                    base_employees[emp_key] = {
                        "employee": result.get("employee") or "unknown",
                        "employee_id": result.get("employee_id"),
                        "included": [],
                        "skipped": [],
                        "excluded_folders": [],
                    }
                if result["status"] == "success":
                    _upsert_item(
                        base_employees[emp_key]["included"],
                        {
                            "file_id": result.get("file_id"),
                            "file_name": result.get("file_name"),
                            "outputs": result.get("outputs"),
                        },
                    )
                else:
                    _upsert_item(
                        base_employees[emp_key]["skipped"],
                        {
                            "file_id": result.get("file_id"),
                            "file_name": result.get("file_name"),
                            "reason": result.get("reason"),
                        },
                    )
                processed_since_flush += 1
                if processed_since_flush >= flush_every:
                    _write_report(
                        report_path,
                        manifest.get("root_id"),
                        _finalize_employees(base_employees, employees),
                    )
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

    _write_report(
        report_path,
        manifest.get("root_id"),
        _finalize_employees(base_employees, employees),
    )

    logger.info("Report saved to %s", report_path)


if __name__ == "__main__":
    main()
