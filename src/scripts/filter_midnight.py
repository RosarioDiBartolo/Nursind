from __future__ import annotations

from _bootstrap import bootstrap

bootstrap()

from _common import run_script
from core.events.filtering.options import FilterMidnightEventsOptions
from core.events.filtering.service import run_from_options


def run(config, verbose: bool) -> None:
    paths = config.paths
    settings = config.step("filter_midnight")
    run_from_options(
        FilterMidnightEventsOptions(
            input_dir=str(paths.events_dir),
            report_json=str(paths.filter_report),
            removed_csv=str(paths.removed_midnight_csv),
            max_removed_examples_per_file=int(
                settings.get("max_removed_examples_per_file", 10)
            ),
            verbose=verbose,
        )
    )


if __name__ == "__main__":
    raise SystemExit(run_script("Remove synthetic midnight events.", run))
