from __future__ import annotations

from pathlib import Path

scripts_dir = Path(__file__).resolve().parents[1]
import sys

sys.path.insert(0, str(scripts_dir))

from _bootstrap import bootstrap

bootstrap()

from _common import run_script
from core.config_values import optional_int
from core.tools.turni_custom_counts.options import (
    DEFAULT_REPORT_JSON,
    DEFAULT_SUMMARY_CSV,
    DEFAULT_YEAR_END,
    DEFAULT_YEAR_START,
    TurniCustomCountsOptions,
)
from core.tools.turni_custom_counts.service import run_from_options


def run(config, verbose: bool) -> None:
    output_dir = config.paths.pipeline_root / "turni_custom_counts"
    settings = config.step("turni_custom_counts")
    report = run_from_options(
        TurniCustomCountsOptions(
            enriched_dir=str(config.paths.enrichment_dir),
            output_dir=str(output_dir),
            summary_csv=str(settings.get("summary_csv", DEFAULT_SUMMARY_CSV)),
            report_json=str(settings.get("report_json", DEFAULT_REPORT_JSON)),
            year_start=optional_int(settings.get("year_start", DEFAULT_YEAR_START)),
            year_end=optional_int(settings.get("year_end", DEFAULT_YEAR_END)),
            verbose=verbose,
        )
    )
    if report["status"] != "ok":
        issue = report["issues"][0]
        raise RuntimeError(str(issue["message"]))


if __name__ == "__main__":
    raise SystemExit(
        run_script(
            "Count custom shift categories from enriched shifts.",
            run,
        )
    )
