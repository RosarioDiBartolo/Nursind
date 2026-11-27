"""Unisce i lotti di origine e ricrea una cartella di consegna per gruppo."""

import argparse
from pathlib import Path
from _bootstrap import bootstrap

bootstrap()
from core.tools.consegna.service import build

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--blocco", type=Path, default=Path(__file__).resolve().parents[2] / "runs/Blocco 1"
    )
    parser.add_argument(
        "--destinazione", default="Consegna", help="Nome della nuova cartella nel blocco"
    )
    parser.add_argument("--workers", type=int, default=3)
    args = parser.parse_args()
    if args.workers < 1:
        parser.error("--workers deve essere positivo")
    base = args.blocco.resolve()
    build(base, (base / args.destinazione).resolve(), args.workers)
