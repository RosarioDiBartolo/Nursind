from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from core.config import load_pipeline_config
from core.drive.logging_utils import setup_logging
from core.tools.missing_report.options import TimbratureMissingReportOptions
from core.tools.missing_report.service import run_from_options


def main() -> int:
    config = load_pipeline_config()
    setup_logging(False)
    run_from_options(TimbratureMissingReportOptions(pipeline_dir=str(config.paths.pipeline_root)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
