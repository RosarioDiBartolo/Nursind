from __future__ import annotations

from _bootstrap import bootstrap

bootstrap()

from _common import run_script
from core.shifts.pairing.options import PairEmployeeEventsOptions
from core.shifts.pairing.runtime import run_from_options


def run(config, verbose: bool) -> None:
    paths = config.paths
    settings = config.step("pair_events")
    run_from_options(
        PairEmployeeEventsOptions(
            input_dir=str(paths.events_dir),
            output_dir=str(paths.shifts_dir),
            report_json=str(paths.pairing_report),
            max_gap_hours=float(settings.get("max_gap_hours", 16.0)),
            keep_inferred_column=bool(settings.get("keep_inferred_column", False)),
            verbose=verbose,
        )
    )


if __name__ == "__main__":
    raise SystemExit(run_script("Pair cleaned employee events into shifts.", run))
