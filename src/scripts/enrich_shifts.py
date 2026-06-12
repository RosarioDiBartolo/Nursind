from __future__ import annotations

from _bootstrap import bootstrap

bootstrap()

from _common import run_script
from core.shifts.enrichment.options import TurniEnrichmentOptions
from core.shifts.enrichment.service import run_from_options


def run(config, verbose: bool) -> None:
    paths = config.paths
    settings = config.step("enrich_shifts")
    run_from_options(
        TurniEnrichmentOptions(
            input_dir=str(paths.shifts_dir),
            output_dir=str(paths.enrichment_dir),
            min_hours=float(settings.get("min_hours", 6.0)),
            include_holidays=bool(settings.get("include_holidays", True)),
            report_json=str(paths.enrichment_report),
            verbose=verbose,
        )
    )


if __name__ == "__main__":
    raise SystemExit(run_script("Enrich paired shifts with reporting classifications.", run))
