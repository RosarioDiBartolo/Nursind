from __future__ import annotations

from _bootstrap import bootstrap

bootstrap()

from _common import run_script
from core.pipeline.steps import run_extract_events_step


def run(config, verbose: bool) -> None:
    run_extract_events_step(config, verbose)


if __name__ == "__main__":
    raise SystemExit(run_script("Parse canonical documents into events and page diagnostics.", run))
