from __future__ import annotations

import argparse
import os
from typing import Sequence

from ..logging_utils import get_logger, setup_logging
from .converters import convert_index_file


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Convert Drive index schema between map and flat list formats."
    )
    parser.add_argument("--input", required=True, help="Input index JSON path")
    parser.add_argument("--output", required=True, help="Output index JSON path")
    parser.add_argument(
        "--to",
        choices=("map", "list"),
        default=None,
        help="Target format. If omitted, convert to the opposite of input schema.",
    )
    parser.add_argument(
        "--on-duplicate",
        choices=("error", "first", "last"),
        default="error",
        help="Duplicate file_id policy for list->map conversion.",
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    setup_logging(args.verbose)
    logger = get_logger()

    input_path = args.input if os.path.isabs(args.input) else os.path.abspath(args.input)
    output_path = args.output if os.path.isabs(args.output) else os.path.abspath(args.output)

    source_kind, target_kind, total_files = convert_index_file(
        input_path=input_path,
        output_path=output_path,
        target_kind=args.to,
        duplicate_policy=args.on_duplicate,
    )
    logger.info(
        "Converted index %s -> %s (%s files): %s",
        source_kind,
        target_kind,
        total_files,
        output_path,
    )
    return 0
