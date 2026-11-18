from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _bootstrap import bootstrap

bootstrap()

from core.config import load_pipeline_config
from core.drive.logging_utils import setup_logging
from core.tools.deviation_export.service import export_deviations


def main() -> int:
    parser = argparse.ArgumentParser(description="Export post-filter shift deviations to CSV and monthly PDF reports.")
    parser.add_argument("--config", default="pipeline.json")
    parser.add_argument("--threshold-minutes", type=float, default=10)
    parser.add_argument("--schedules", type=Path, help="Explicit theoretical schedule JSON")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()
    setup_logging(args.verbose)
    try:
        config = load_pipeline_config(args.config)
        report = export_deviations(config.paths.pipeline_root, config.paths.repository_root,
                                   threshold=args.threshold_minutes, schedules_path=args.schedules)
        logging.getLogger(__name__).info("Export %s: %s", report["status"], report["stats"])
        return 0
    except Exception:
        logging.getLogger(__name__).exception("Deviation export failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
