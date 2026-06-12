from __future__ import annotations

from _bootstrap import bootstrap

bootstrap()

from _common import run_script
from core.events.extraction.options import ExtractEventsFromTextOptions
from core.events.extraction.service import run_from_options


def run(config, verbose: bool) -> None:
    paths = config.paths
    settings = config.step("extract_events")
    run_from_options(
        ExtractEventsFromTextOptions(
            input_dir=str(paths.documents_dir),
            output_dir=str(paths.events_dir),
            report_json=str(paths.events_report),
            max_pattern_examples=int(settings.get("max_pattern_examples", 12)),
            max_unmatched_examples_per_file=int(
                settings.get("max_unmatched_examples_per_file", 5)
            ),
            verbose=verbose,
        )
    )


if __name__ == "__main__":
    raise SystemExit(run_script("Parse canonical documents into events and page diagnostics.", run))
