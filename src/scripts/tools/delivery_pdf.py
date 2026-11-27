"""Export a delivery folder containing conteggi.csv and QUESITO.md."""
import argparse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _bootstrap import bootstrap

bootstrap()
from core.tools.turni_custom_counts.delivery_pdf import export_counts_pdf


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("folder", type=Path)
    parser.add_argument("--status", required=True, help="Stato e perimetro dei conteggi.")
    args = parser.parse_args()
    print(export_counts_pdf(args.folder, args.status))
