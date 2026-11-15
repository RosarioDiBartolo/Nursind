"""Prepare or verify the isolated synthetic demo; no remote scan."""

from __future__ import annotations

import argparse
from pathlib import Path

from _bootstrap import bootstrap

bootstrap()

from core.config import load_pipeline_config
from core.tools.portfolio_demo.fixtures import prepare_demo
from core.tools.portfolio_demo.verification import verify_demo


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "verify", "visuals"))
    parser.add_argument("--name", default="portfolio-demo")
    args = parser.parse_args()
    if (
        not args.name
        or any(c not in "abcdefghijklmnopqrstuvwxyz0123456789-" for c in args.name)
        or not args.name.startswith("portfolio-demo")
    ):
        parser.error(
            "--name must start with portfolio-demo and contain lowercase letters, digits or hyphens"
        )
    repository = Path(__file__).resolve().parents[2]
    if args.action == "prepare":
        print(prepare_demo(repository, args.name))
    else:
        config = load_pipeline_config(repository / f"{args.name}.out_dir" / "demo-config.json")
        if args.action == "verify":
            result = verify_demo(config)
            print(
                f"PASS: {len(result['checks'])} checks; evidence: {config.paths.pipeline_root / 'verification.json'}"
            )
        else:
            from core.tools.portfolio_demo.visuals import create_visuals

            create_visuals(config)


if __name__ == "__main__":
    main()
