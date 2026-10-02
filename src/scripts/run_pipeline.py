 

import argparse
import logging

from _bootstrap import bootstrap

bootstrap()

from core.config import load_pipeline_config
from core.drive.logging_utils import setup_logging
from core.pipeline.runner import STEP_ORDER, run_pipeline_steps


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run selected pipeline steps in canonical dependency order."
    )
    parser.add_argument("steps", nargs="*", help="Step names or aliases to run")
    parser.add_argument("--steps", dest="steps_csv", help="Comma-separated step names to run")
    parser.add_argument("--from", dest="start", help="Start from this step")
    parser.add_argument("--through", dest="end", help="Stop after this step")
    parser.add_argument("--skip", action="append", default=[], help="Step name or comma list to skip")
    parser.add_argument("--config", default="pipeline.json", help="Pipeline JSON configuration")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.epilog = "Canonical order: " + ", ".join(STEP_ORDER)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    setup_logging(args.verbose)
    selected = list(args.steps)
    if args.steps_csv:
        selected.append(args.steps_csv)

    try:
        ran = run_pipeline_steps(
            load_pipeline_config(args.config),
            steps=selected,
            start=args.start,
            end=args.end,
            skip=args.skip,
            verbose=bool(args.verbose),
        )
    except Exception as exc:
        logging.getLogger(__name__).error("%s", exc)
        return 1

    logging.getLogger(__name__).info("Pipeline runner complete: %s", ", ".join(ran) or "no steps")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
