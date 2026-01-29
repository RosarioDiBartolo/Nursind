from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from parser_service.router import analyze_detection, parse_pdf, parse_text
from parser_shared.extract import extract_text, extract_text_vertical
from parser_shared.models import CartellinoParseError, ParserDetectionError


LOGGER = logging.getLogger(__name__)
def _configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(levelname)s %(name)s: %(message)s")


def _iter_pdfs(input_path: Path, recursive: bool) -> list[Path]:
    if input_path.is_file():
        return [input_path]
    pattern = "**/*.pdf" if recursive else "*.pdf"
    return sorted(input_path.glob(pattern))


def _output_subdir(input_root: Path, pdf_path: Path) -> Path:
    if input_root.is_dir():
        return pdf_path.relative_to(input_root).with_suffix("")
    return Path(pdf_path.stem)


def _write_outputs(out_dir: Path, output_path: Path, parsed) -> None:
    file_out = out_dir / output_path
    file_out.mkdir(parents=True, exist_ok=True)
    days_path = file_out / "days.csv"
    pairs_path = file_out / "pairs.csv"
    totals_path = file_out / "totals.json"
    report_path = file_out / "report.json"

    parsed.days_df.to_csv(days_path, index=False)
    parsed.pairs_df.to_csv(pairs_path, index=False)
    totals_path.write_text(json.dumps(parsed.totals, indent=2, ensure_ascii=False))
    report = {
        "meta": parsed.meta,
        "totals": parsed.totals,
        "validation": parsed.validation,
    }
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False))


def _write_debug_files(
    out_dir: Path,
    output_path: Path,
    text_label: str,
    text: str,
    detect_info: dict,
    dump_text: bool,
    debug_detect: bool,
) -> None:
    file_out = out_dir / output_path
    file_out.mkdir(parents=True, exist_ok=True)
    if dump_text:
        text_path = file_out / f"extracted.{text_label}.txt"
        text_path.write_text(text, encoding="utf-8")
    if debug_detect:
        detect_path = file_out / f"detect.{text_label}.json"
        detect_path.write_text(json.dumps(detect_info, indent=2, ensure_ascii=False), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Auto-detect and parse cartellino/timbrature PDFs.")
    subparsers = parser.add_subparsers(dest="command", required=True)
    parse_parser = subparsers.add_parser("parse", help="Parse PDF files")
    parse_parser.add_argument("--input", required=True, help="PDF file or folder")
    parse_parser.add_argument("--out", help="Output folder (writes CSV/JSON when provided)")
    parse_parser.add_argument("--recursive", action="store_true", help="Scan subfolders for PDFs")
    parse_parser.add_argument("--strict", action="store_true", help="Fail fast on parse errors")
    parse_parser.add_argument(
        "--debug-detect",
        action="store_true",
        help="Log detection scores; writes detect.*.json when --out is set",
    )
    parse_parser.add_argument(
        "--dump-text",
        action="store_true",
        help="Write extracted text (normal/vertical) when --out is set",
    )
    parse_parser.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    _configure_logging(args.verbose)
    input_path = Path(args.input)
    out_dir = Path(args.out) if args.out else None
    if out_dir is not None:
        out_dir.mkdir(parents=True, exist_ok=True)

    pdf_paths = _iter_pdfs(input_path, args.recursive)
    if not pdf_paths:
        LOGGER.warning("No PDF files found under %s", input_path)
        return 1

    if (args.debug_detect or args.dump_text) and out_dir is None:
        LOGGER.warning("--debug-detect/--dump-text enabled without --out; debug files will not be written.")

    failures = 0
    for pdf_path in pdf_paths:
        output_path = _output_subdir(input_path, pdf_path)
        if args.debug_detect or args.dump_text:
            text = extract_text(pdf_path)
            detect_info = analyze_detection(text)
            if args.debug_detect:
                LOGGER.info("Detect(normal) %s: %s", pdf_path, json.dumps(detect_info, ensure_ascii=False))
            if out_dir is not None:
                _write_debug_files(
                    out_dir,
                    output_path,
                    "normal",
                    text,
                    detect_info,
                    args.dump_text,
                    args.debug_detect,
                )

            text_vertical = extract_text_vertical(pdf_path)
            detect_info_vertical = analyze_detection(text_vertical)
            if args.debug_detect:
                LOGGER.info(
                    "Detect(vertical) %s: %s",
                    pdf_path,
                    json.dumps(detect_info_vertical, ensure_ascii=False),
                )
            if out_dir is not None:
                _write_debug_files(
                    out_dir,
                    output_path,
                    "vertical",
                    text_vertical,
                    detect_info_vertical,
                    args.dump_text,
                    args.debug_detect,
                )
        try:
            if args.debug_detect or args.dump_text:
                text_for_parse = text
                if detect_info["score_cart"] == 0 and detect_info["score_timb"] == 0:
                    text_for_parse = text_vertical
                try:
                    parsed = parse_text(text_for_parse, pdf_path)
                except ParserDetectionError as exc:
                    message = str(exc).lower()
                    if "no cartellino or timbrature markers found" not in message:
                        raise
                    parsed = parse_text(text_vertical, pdf_path)
                except CartellinoParseError as exc:
                    if "No day lines found" not in str(exc):
                        raise
                    parsed = parse_text(text_vertical, pdf_path)
            else:
                parsed = parse_pdf(pdf_path)
        except (ParserDetectionError, CartellinoParseError) as exc:
            failures += 1
            if args.verbose:
                LOGGER.exception("Failed to parse %s", pdf_path)
            else:
                LOGGER.error("Failed to parse %s: %s", pdf_path, exc)
            if args.strict:
                return 1
            continue
        print(f"{pdf_path}: {json.dumps(parsed.totals, ensure_ascii=False)}")
        if out_dir is not None:
            _write_outputs(out_dir, output_path, parsed)

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
