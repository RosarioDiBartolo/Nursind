"""Generate the offline reference for Nursind's attendance parsers."""

import argparse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from core.tools.parser_catalog.render import OUTPUT, render_catalog


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Fail if the checked-in HTML is stale.")
    args = parser.parse_args()
    rendered = render_catalog()
    if args.check:
        if not OUTPUT.exists() or OUTPUT.read_text(encoding="utf-8") != rendered:
            parser.exit(1, "Regenerate docs/parsers/index.html.\n")
    else:
        OUTPUT.write_text(rendered, encoding="utf-8", newline="\n")


if __name__ == "__main__":
    main()

