from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from cartellino_parser.parser import parse_pdf


def _configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")


def _iter_pdfs(input_path: Path) -> list[Path]:
    if input_path.is_file():
        return [input_path]
    return sorted(input_path.glob("*.pdf"))


def main() -> int:
    parser = argparse.ArgumentParser(description="Parse Cartellino mensile PDFs.")
    subparsers = parser.add_subparsers(dest="command", required=True)
    parse_parser = subparsers.add_parser("parse", help="Parse PDF files")
    parse_parser.add_argument("--input", required=True, help="PDF file or folder")
    parse_parser.add_argument("--out", required=True, help="Output folder")
    args = parser.parse_args()

    _configure_logging()
    input_path = Path(args.input)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    for pdf_path in _iter_pdfs(input_path):
        parsed = parse_pdf(pdf_path)
        print(parsed.totals)

        stem = pdf_path.stem
        days_path = out_dir / f"{stem}.days.csv"
        pairs_path = out_dir / f"{stem}.pairs.csv"
        totals_path = out_dir / f"{stem}.totals.json"
        report_path = out_dir / f"{stem}.report.json"

        parsed.days_df.to_csv(days_path, index=False)
        parsed.pairs_df.to_csv(pairs_path, index=False)
        totals_path.write_text(json.dumps(parsed.totals, indent=2, ensure_ascii=False))
        report = {
            "meta": parsed.meta,
            "totals": parsed.totals,
            "validation": parsed.validation,
        }
        report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
