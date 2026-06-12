from __future__ import annotations

import argparse
import logging
from collections.abc import Callable

from core.config import PipelineConfig, load_pipeline_config
from core.drive.logging_utils import setup_logging


def parse_script_args(description: str) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--config", default="pipeline.json", help="Pipeline JSON configuration")
    parser.add_argument("--verbose", "-v", action="store_true")
    return parser.parse_args()


def run_script(description: str, action: Callable[[PipelineConfig, bool], None]) -> int:
    args = parse_script_args(description)
    setup_logging(args.verbose)
    try:
        action(load_pipeline_config(args.config), bool(args.verbose))
    except Exception as exc:
        logging.getLogger(__name__).error("%s", exc)
        return 1
    return 0
