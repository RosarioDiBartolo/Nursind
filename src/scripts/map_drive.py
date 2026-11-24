from __future__ import annotations

from _bootstrap import bootstrap

bootstrap()

import argparse
import logging

from core.config import load_pipeline_config
from core.drive.logging_utils import setup_logging
from core.folder_mapping.runner import STEPS, run_mapping


def main() -> int:
    parser = argparse.ArgumentParser(description="Map Drive PDFs using existing text and event extraction logic.")
    parser.add_argument("--config", default="pipeline.json")
    parser.add_argument("--from", dest="start", choices=STEPS, default="scan")
    parser.add_argument("--through", choices=STEPS, default="extract_events")
    parser.add_argument("--input-csv", help="Drive file_id or file_url CSV")
    parser.add_argument("--identities", help="Reviewed file_id,employee,employee_id CSV for grouping")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()
    setup_logging(args.verbose)
    try:
        run_mapping(load_pipeline_config(args.config), start=args.start, through=args.through, verbose=args.verbose,
                    input_csv=args.input_csv, identities=args.identities)
    except Exception as exc:
        logging.getLogger(__name__).error("%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
