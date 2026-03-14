from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.drive_service.fs_utils import ensure_parent_dir
from src.extract_events_from_documents.writers import write_rows_csv

from .options import (
    DEFAULT_REPORT_JSON,
    DEFAULT_ROOT_DIR,
    DEFAULT_SUSPICIOUS_CSV,
    ParserRecallAuditOptions,
)
from .service import SUSPICIOUS_PAGE_COLUMNS, audit_parser_recall_root


def _resolve_output_path(root_dir: str | Path, path: str) -> Path:
    candidate = Path(path)
    if candidate.is_absolute():
        return candidate
    return Path(root_dir) / candidate


def _build_json_payload(report: dict[str, Any]) -> dict[str, Any]:
    return {
        "stats": report["stats"],
        "artifacts": report["artifacts"],
        "outputs": report["outputs"],
        "counts_by_bucket": report["counts_by_bucket"],
        "counts_by_pipeline": report["counts_by_pipeline"],
        "counts_by_parser": report["counts_by_parser"],
        "row_totals": {
            "suspicious_rows": len(report["suspicious_rows"]),
        },
    }


def build_parser_recall_report(
    *,
    root_dir: str = DEFAULT_ROOT_DIR,
    report_json: str = DEFAULT_REPORT_JSON,
    suspicious_csv: str = DEFAULT_SUSPICIOUS_CSV,
    max_tiny_rows: int,
    min_large_rows: int,
    low_coverage_threshold: float,
) -> dict[str, Any]:
    report = audit_parser_recall_root(
        root_dir,
        max_tiny_rows=max_tiny_rows,
        min_large_rows=min_large_rows,
        low_coverage_threshold=low_coverage_threshold,
    )

    report_path = _resolve_output_path(root_dir, report_json)
    suspicious_csv_path = _resolve_output_path(root_dir, suspicious_csv)

    outputs = report.setdefault("outputs", {})
    outputs["report_json"] = str(report_path.resolve())
    outputs["suspicious_csv"] = str(suspicious_csv_path.resolve())

    write_rows_csv(
        rows=report["suspicious_rows"],
        out_csv=suspicious_csv_path,
        columns=SUSPICIOUS_PAGE_COLUMNS,
    )

    json_payload = _build_json_payload(report)
    ensure_parent_dir(str(report_path))
    with report_path.open("w", encoding="utf-8") as handle:
        json.dump(json_payload, handle, ensure_ascii=False, indent=2)
    return report


def run_from_options(options: ParserRecallAuditOptions) -> dict[str, Any]:
    return build_parser_recall_report(
        root_dir=options.root_dir,
        report_json=options.report_json,
        suspicious_csv=options.suspicious_csv,
        max_tiny_rows=options.max_tiny_rows,
        min_large_rows=options.min_large_rows,
        low_coverage_threshold=options.low_coverage_threshold,
    )


__all__ = [
    "build_parser_recall_report",
    "run_from_options",
]
