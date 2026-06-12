from __future__ import annotations

from pathlib import Path
from typing import Any

from core.drive.fs_utils import ensure_parent_dir
from core.drive.io_json import write_json


def resolve_output_path(base_dir: str | Path, path: str | Path) -> Path:
    candidate = Path(path)
    if candidate.is_absolute():
        return candidate
    return Path(base_dir) / candidate


def write_json_report(path: str | Path, payload: dict[str, Any]) -> None:
    report_path = Path(path)
    ensure_parent_dir(str(report_path))
    write_json(str(report_path), payload)


def build_stage_report(
    *,
    stage: str,
    inputs: dict[str, Any],
    outputs: dict[str, Any] | None = None,
    stats: dict[str, Any] | None = None,
    row_totals: dict[str, Any] | None = None,
    items: list[dict[str, Any]] | None = None,
    issues: list[dict[str, Any]] | None = None,
    status: str = "ok",
) -> dict[str, Any]:
    report = {
        "stage": stage,
        "status": status,
        "inputs": inputs,
        "outputs": outputs or {},
        "stats": stats or {},
        "row_totals": row_totals or {},
        "issues": issues or [],
    }
    if items is not None:
        report["items"] = items
    return report


def compact_stage_report(
    report: dict[str, Any],
    *,
    include_items: bool = False,
) -> dict[str, Any]:
    payload = {
        key: report[key]
        for key in ("stage", "status", "inputs", "outputs", "stats", "row_totals", "issues")
        if key in report
    }
    if include_items and "items" in report:
        payload["items"] = report["items"]
    return payload


__all__ = [
    "build_stage_report",
    "compact_stage_report",
    "resolve_output_path",
    "write_json_report",
]

