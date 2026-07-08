from __future__ import annotations

from _bootstrap import bootstrap

bootstrap()

from _common import run_script
from core.config_values import optional_int
from core.shifts.summary.options import TurniEmployeeSummaryOptions
from core.shifts.summary.service import run_from_options


def run(config, verbose: bool) -> None:
    paths = config.paths
    settings = config.step("summarize_shifts")
    output_format = str(settings.get("format", "csv"))
    output_path = paths.summary_csv
    if output_format == "json":
        output_path = output_path.with_suffix(".json")
    run_from_options(
        TurniEmployeeSummaryOptions(
            enriched_dir=str(paths.enrichment_dir),
            out=str(output_path),
            report_json=str(paths.summary_report),
            year_start=optional_int(settings.get("year_start")),
            year_end=optional_int(settings.get("year_end")),
            output_format=output_format,
            min_hours=settings.get("min_hours"),
            verbose=verbose,
        )
    )


if __name__ == "__main__":
    raise SystemExit(run_script("Aggregate enriched shifts into yearly summaries.", run))
