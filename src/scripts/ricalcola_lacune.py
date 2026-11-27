"""Ricalcola i conteggi dai lotti filtrati; --recupera tenta anche le lacune residue."""

import argparse
from pathlib import Path
from _bootstrap import bootstrap

bootstrap()
from core.tools.recupero_lacune.service import run

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--blocco", type=Path, default=Path("runs/Blocco 1"))
    parser.add_argument("--gruppo", action="append", default=[])
    parser.add_argument(
        "--recupera", action="store_true", help="Riprova parser e OCR sui candidati ancora a zero"
    )
    parser.add_argument("--offline", action="store_true", help="Usa solo originali locali")
    parser.add_argument("--workers", type=int, default=3, help="Processi paralleli per i conteggi")
    args = parser.parse_args()
    if args.workers < 1:
        parser.error("--workers deve essere positivo")
    residual = run(args.blocco.resolve(), args.gruppo, args.recupera, args.offline, args.workers)
    if residual:
        print(
            f"{residual} anomalie: vedere lacune_residue.json o errore_gruppo.json nel gruppo interessato"
        )
        raise SystemExit(2)
