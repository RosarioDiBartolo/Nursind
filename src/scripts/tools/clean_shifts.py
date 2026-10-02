from __future__ import annotations

from pathlib import Path

scripts_dir = Path(__file__).resolve().parents[1]
import sys

sys.path.insert(0, str(scripts_dir))

from _bootstrap import bootstrap

bootstrap()

from _common import run_script
from core.tools.clean_shifts_export.options import (
    DEFAULT_REPORT_JSON,
    CleanShiftsExportOptions,
)
from core.tools.clean_shifts_export.service import run_from_options


def run(config, verbose: bool) -> None:
    output_dir = config.paths.pipeline_root / "clean_shifts"
    settings = config.step("clean_shifts_export")
    report = run_from_options(
        CleanShiftsExportOptions(
            enriched_dir=str(config.paths.enrichment_dir),
            output_dir=str(output_dir),
            report_json=str(settings.get("report_json", DEFAULT_REPORT_JSON)),
            verbose=verbose,
        )
    )
    if report["status"] != "ok":
        issue = report["issues"][0]
        raise RuntimeError(str(issue["message"]))


if __name__ == "__main__":
    raise SystemExit(
        run_script(
            "Export cleaned per-employee shift CSV files.",
            run,
        )
    )
