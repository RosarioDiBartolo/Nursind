"""CLI entrypoint for the parsing service."""

from __future__ import annotations

from .parser_service.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
