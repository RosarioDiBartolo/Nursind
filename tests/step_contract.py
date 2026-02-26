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
    assert "stats" in report
    assert "items" in report
    assert "errors" in report

    stats = report["stats"]
    items = report["items"]
    errors = report["errors"]

    assert isinstance(stats, dict)
    assert isinstance(items, list)
    assert isinstance(errors, list)

    for required_stat in ("files_total", "files_processed", "files_error"):
        assert required_stat in stats

    assert int(stats["files_total"]) == len(items)
    assert int(stats["files_error"]) == len(errors)
    assert int(stats["files_processed"]) + int(stats["files_error"]) == int(stats["files_total"])
