from __future__ import annotations

from pathlib import Path

scripts_dir = Path(__file__).resolve().parents[1]
import sys

sys.path.insert(0, str(scripts_dir))

from _bootstrap import bootstrap

bootstrap()

from _common import run_script
from core.tools.afternoon_export.options import TurniAfternoonLongExportOptions
from core.tools.afternoon_export.service import run_from_options


def run(config, verbose: bool) -> None:
    output_dir = config.base_output_dir / config.name / "afternoon_long_export"
    report = run_from_options(
        TurniAfternoonLongExportOptions(
            enriched_dir=str(config.paths.enrichment_dir),
            pairs_dir=str(config.paths.shifts_dir),
            output_dir=str(output_dir),
            report_json=str(output_dir / "turni_afternoon_long_export.report.json"),
            verbose=verbose,
        )
    )
    if report["status"] != "ok":
        issue = report["issues"][0]
        raise RuntimeError(str(issue["message"]))


if __name__ == "__main__":
    raise SystemExit(
        run_script(
            "Export long afternoon shifts to per-employee CSV and PDF files.",
            run,
        )
    )
