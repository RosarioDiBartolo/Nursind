from __future__ import annotations

from typing import Any


def assert_process_one_contract(
    result: dict[str, Any],
    *,
    source_key: str,
) -> None:
    assert isinstance(result, dict)
    assert "status" in result
    assert result["status"] in {"ok", "error"}
    assert "error" in result
    assert "error_code" in result
    assert source_key in result
    if result["status"] == "ok":
        assert result["error"] is None
        assert result["error_code"] is None


def assert_process_many_contract(report: dict[str, Any]) -> None:
    assert isinstance(report, dict)
    assert "stage" in report
    assert "status" in report
    assert report["status"] == "ok"
    assert "inputs" in report
    assert "outputs" in report
    assert "stats" in report
    assert "row_totals" in report
    assert "items" in report
    assert "issues" in report

    inputs = report["inputs"]
    outputs = report["outputs"]
    stats = report["stats"]
    row_totals = report["row_totals"]
    items = report["items"]
    issues = report["issues"]

    assert isinstance(inputs, dict)
    assert isinstance(outputs, dict)
    assert isinstance(stats, dict)
    assert isinstance(row_totals, dict)
    assert isinstance(items, list)
    assert isinstance(issues, list)

    for required_stat in ("files_total", "files_processed", "files_error"):
        assert required_stat in stats

    assert "items" in row_totals
    assert "issues" in row_totals
    assert int(stats["files_total"]) == len(items)
    assert int(row_totals["items"]) == len(items)
    assert int(stats["files_error"]) == len(issues)
    assert int(row_totals["issues"]) == len(issues)
    assert int(stats["files_processed"]) + int(stats["files_error"]) == int(stats["files_total"])
