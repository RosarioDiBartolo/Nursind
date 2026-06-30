from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from core.config import load_pipeline_config
from core.drive.logging_utils import setup_logging
from core.tools.parser_recall.options import ParserRecallAuditOptions
from core.tools.parser_recall.service import run_from_options


def main() -> int:
    config = load_pipeline_config()
    setup_logging(False)
    root = config.paths.pipeline_root
    run_from_options(
        ParserRecallAuditOptions(
            root_dir=str(root),
            report_json="parser_recall_audit.report.json",
            suspicious_csv="suspicious_pages.csv",
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
