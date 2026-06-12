from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pandas as pd

from notebooks.shared_config import NotebookContext


def pipeline_overview(ctx: NotebookContext, step_name: str) -> pd.DataFrame:
    """Return the small set of values an operator needs before running a step."""
    return pd.DataFrame(
        [
            ("config", str(ctx.config_path)),
            ("pipeline", ctx.root_prefix),
            ("output root", str(ctx.paths.pipeline_root)),
            ("step", step_name),
        ],
        columns=["setting", "value"],
    )


def artifact_table(artifacts: Mapping[str, str | Path]) -> pd.DataFrame:
    """Describe expected inputs and outputs without opening large artifacts."""
    rows: list[dict[str, Any]] = []
    for name, raw_path in artifacts.items():
        path = Path(raw_path)
        exists = path.exists()
        rows.append(
            {
                "artifact": name,
                "exists": exists,
                "size_kb": round(path.stat().st_size / 1024, 1) if exists and path.is_file() else None,
                "path": str(path),
            }
        )
    return pd.DataFrame(rows)


def preview_csv(path: str | Path, rows: int = 8) -> pd.DataFrame:
    """Read a small CSV preview and return an empty table when it is unavailable."""
    source = Path(path)
    if not source.exists() or source.stat().st_size == 0:
        return pd.DataFrame()
    return pd.read_csv(source, nrows=rows)


def preview_json(path: str | Path) -> dict[str, Any]:
    """Load a JSON artifact for notebook display."""
    source = Path(path)
    if not source.exists():
        return {"status": "not_created", "path": str(source)}
    with source.open("r", encoding="utf-8") as stream:
        payload = json.load(stream)
    return payload if isinstance(payload, dict) else {"value": payload}


def report_summary(report: Mapping[str, Any] | None) -> dict[str, Any]:
    """Keep the report preview focused on status, totals, outputs, and issues."""
    if not report:
        return {"status": "no_report"}
    summary: dict[str, Any] = {
        "stage": report.get("stage"),
        "status": report.get("status"),
    }
    for key in ("stats", "row_totals", "outputs"):
        if key in report:
            summary[key] = report[key]
    issues = report.get("issues")
    if isinstance(issues, list):
        summary["issue_count"] = len(issues)
        summary["issue_examples"] = issues[:3]
    return summary


def file_table(directory: str | Path, pattern: str, limit: int = 10) -> pd.DataFrame:
    """List a bounded sample of generated files."""
    root = Path(directory)
    if not root.exists():
        return pd.DataFrame(columns=["name", "size_kb", "path"])
    files = sorted(root.glob(pattern))
    return pd.DataFrame(
        [
            {
                "name": path.name,
                "size_kb": round(path.stat().st_size / 1024, 1),
                "path": str(path),
            }
            for path in files[:limit]
            if path.is_file()
        ]
    )


__all__ = [
    "artifact_table",
    "file_table",
    "pipeline_overview",
    "preview_csv",
    "preview_json",
    "report_summary",
]
