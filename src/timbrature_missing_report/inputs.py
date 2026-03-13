from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

from src.drive_service.text_extraction_csv import load_text_extraction_doc
from src.raw_text_parsing import infer_year_month_from_filename, resolve_year_month

YearMonth = tuple[int, int]


@dataclass(slots=True)
class ResolvedAuditInputs:
    pipeline_dir: Path
    scan_dir: Path
    documents_dir: Path
    events_dir: Path
    shifts_dir: Path
    scan_report_path: Path
    excluded_index_path: Path
    found_events_csv_path: Path
    pages_csv_path: Path
    pair_report_path: Path


def clean_str(value: object) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.lower() in {"nan", "none", "<na>", "null"}:
        return None
    return text


def parse_int(value: object) -> int | None:
    text = clean_str(value)
    if text is None:
        return None
    try:
        return int(text)
    except Exception:
        try:
            return int(float(text))
        except Exception:
            return None


def parse_year_month(year_value: object, month_value: object) -> YearMonth | None:
    year = parse_int(year_value)
    month = parse_int(month_value)
    if year is None or month is None:
        return None
    if not (1900 <= year <= 2100 and 1 <= month <= 12):
        return None
    return year, month


def format_year_month(value: YearMonth) -> str:
    return f"{value[0]:04d}-{value[1]:02d}"


def resolve_audit_inputs(pipeline_dir: str | Path) -> ResolvedAuditInputs:
    base = Path(pipeline_dir).resolve()
    documents_dir = base / "documents"
    shifts_dir = base / "shifts"
    legacy_documents_dir = base / "text_extracted"
    legacy_shifts_dir = base / "employee_shifts_from_raw"

    if not documents_dir.exists() and legacy_documents_dir.exists():
        raise ValueError(
            "Legacy pipeline layout is no longer supported for the audit step; "
            "expected 'documents/' instead of 'text_extracted/'."
        )
    if not shifts_dir.exists() and legacy_shifts_dir.exists():
        raise ValueError(
            "Legacy pipeline layout is no longer supported for the audit step; "
            "expected 'shifts/' instead of 'employee_shifts_from_raw/'."
        )

    scan_dir = base / "scan"
    events_dir = base / "events"
    found_events_csv_path = events_dir / "events.cleaned.csv"
    if not found_events_csv_path.exists():
        found_events_csv_path = events_dir / "events.csv"

    return ResolvedAuditInputs(
        pipeline_dir=base,
        scan_dir=scan_dir,
        documents_dir=documents_dir,
        events_dir=events_dir,
        shifts_dir=shifts_dir,
        scan_report_path=scan_dir / "scan_directory.report.json",
        excluded_index_path=documents_dir / "excluded_documents.index.json",
        found_events_csv_path=found_events_csv_path,
        pages_csv_path=events_dir / "pages.csv",
        pair_report_path=shifts_dir / "pair_employee_events.report.json",
    )


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def iter_csv_rows(path: Path) -> Iterator[dict[str, str]]:
    if not path.exists():
        return
    with path.open("r", encoding="utf-8", newline="") as handle:
        yield from csv.DictReader(handle)


def source_path_hint(
    *,
    file_name: str | None,
    source_text_ref: str | None,
    drive_path: str | None,
    fallback: str,
) -> Path:
    return Path(file_name or source_text_ref or drive_path or fallback)


def load_document_full_text(documents_dir: Path, row: dict[str, Any]) -> str:
    payload = load_text_extraction_doc(documents_dir, clean_str(row.get("doc_json")))
    if not isinstance(payload, dict):
        return ""
    document = payload.get("document")
    if not isinstance(document, dict):
        return ""
    return str(document.get("full_text") or "")


def derive_manifest_expected_months(
    *,
    documents_dir: Path,
    row: dict[str, Any],
    page_rows: list[dict[str, str]],
) -> tuple[set[YearMonth], str | None]:
    page_months = {
        ym
        for ym in (
            parse_year_month(item.get("page_year"), item.get("page_month")) for item in page_rows
        )
        if ym is not None
    }
    if page_months:
        return page_months, "pages"

    path_hint = source_path_hint(
        file_name=clean_str(row.get("file_name")),
        source_text_ref=clean_str(row.get("source_text_ref")),
        drive_path=clean_str(row.get("drive_path")),
        fallback=clean_str(row.get("doc_json")) or "unknown.pdf",
    )
    full_text = load_document_full_text(documents_dir, row)
    year, month = resolve_year_month(full_text, path_hint)
    if year is not None and month is not None:
        return {(int(year), int(month))}, "document"
    return set(), None


def infer_expected_month_from_file(
    *,
    file_name: str | None,
    drive_path: str | None,
    source_text_ref: str | None = None,
) -> YearMonth | None:
    for candidate in (file_name, source_text_ref, drive_path):
        text = clean_str(candidate)
        if text is None:
            continue
        year, month = infer_year_month_from_filename(Path(text))
        if year is not None and month is not None:
            return int(year), int(month)
    return None


def read_pair_months(path: Path) -> tuple[set[YearMonth], int]:
    rows = read_csv_rows(path)
    months = {
        ym
        for ym in (parse_year_month(row.get("year"), row.get("month")) for row in rows)
        if ym is not None
    }
    return months, len(rows)


def parse_event_year_month(row: dict[str, str]) -> YearMonth | None:
    event_ts = clean_str(row.get("event_ts"))
    if event_ts is None:
        return None
    event_day = event_ts.split(" ", 1)[0]
    parts = event_day.split("-")
    if len(parts) < 2:
        return None
    try:
        year = int(parts[0])
        month = int(parts[1])
    except ValueError:
        return None
    return parse_year_month(year, month)


__all__ = [
    "ResolvedAuditInputs",
    "YearMonth",
    "clean_str",
    "derive_manifest_expected_months",
    "format_year_month",
    "infer_expected_month_from_file",
    "iter_csv_rows",
    "parse_event_year_month",
    "parse_int",
    "parse_year_month",
    "read_csv_rows",
    "read_pair_months",
    "resolve_audit_inputs",
]
