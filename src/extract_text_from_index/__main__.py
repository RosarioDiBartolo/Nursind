 
from __future__ import annotations

from typing import Sequence

from .options import ExtractTextFromIndexOptions, build_parser, parse_options
from .runtime import run_extraction


def main(argv: Sequence[str] | None = None) -> int:
    return run_extraction(parse_options(argv))


__all__ = [
    "ExtractTextFromIndexOptions",
    "build_parser",
    "parse_options",
    "run_extraction",
    "main",
]

if __name__ == "__main__":
    raise SystemExit(main())
