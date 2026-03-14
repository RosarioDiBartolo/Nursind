from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

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


def parse_bool(value: object) -> bool | None:
    text = clean_str(value)
    if text is None:
        return None
    lowered = text.lower()
    if lowered in {"1", "true", "yes", "y"}:
        return True
    if lowered in {"0", "false", "no", "n"}:
        return False
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

    return ResolvedAuditInputs(
        pipeline_dir=base,
        scan_dir=scan_dir,
        documents_dir=documents_dir,
        events_dir=events_dir,
        shifts_dir=shifts_dir,
        scan_report_path=scan_dir / "scan_directory.report.json",
        excluded_index_path=documents_dir / "excluded_documents.index.json",
        pages_csv_path=events_dir / "pages.csv",
        pair_report_path=shifts_dir / "pair_employee_events.report.json",
    )


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


__all__ = [
    "ResolvedAuditInputs",
    "YearMonth",
    "clean_str",
    "format_year_month",
    "parse_bool",
    "parse_int",
    "parse_year_month",
    "read_csv_rows",
    "resolve_audit_inputs",
]
