from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable, Iterable

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from cartellino_parser import parse_pdf as parse_cartellino_pdf  # noqa: E402
from parser_service.router import parse_pdf as parse_auto_pdf  # noqa: E402
from timbrature_elenco_compact_parser import parse_pdf as parse_compact_pdf  # noqa: E402
from timbrature_elenco_parser import parse_pdf as parse_elenco_pdf  # noqa: E402


@dataclass
class FileResult:
    path: str
    parser: str
    ok: bool
    error: str | None = None
    days: int | None = None
    totals_ok: bool | None = None
    ore_lavorate_diff: float | None = None


PARSERS: dict[str, Callable[[Path], object]] = {
    "cartellino": parse_cartellino_pdf,
    "timbrature_compact": parse_compact_pdf,
    "timbrature_elenco": parse_elenco_pdf,
    "auto": parse_auto_pdf,
}

FOLDER_PARSERS: dict[str, str] = {
    "cartellino": "cartellino",
    "timbrature_compact": "timbrature_compact",
    "timbrature_elenco": "timbrature_elenco",
}


def _iter_pdfs(folder: Path) -> Iterable[Path]:
    if not folder.exists():
        return []
    return sorted(folder.glob("*.pdf"))


def _validate_result(parsed) -> tuple[int | None, bool | None, float | None]:
    days_df = parsed.days_df
    if not isinstance(days_df, pd.DataFrame):
        return None, None, None
    days = len(days_df)
    validation = parsed.validation if hasattr(parsed, "validation") else {}
    is_ok = validation.get("is_ok") if isinstance(validation, dict) else None
    diff = validation.get("ore_lavorate_diff") if isinstance(validation, dict) else None
    if isinstance(is_ok, bool):
        return days, is_ok, diff
    return days, None, diff


def run_samples(samples_root: Path, mode: str, strict: bool) -> list[FileResult]:
    results: list[FileResult] = []
    for folder, parser_key in FOLDER_PARSERS.items():
        parser_name = mode if mode != "folder" else parser_key
        parser = PARSERS[parser_name]
        for pdf_path in _iter_pdfs(samples_root / folder):
            try:
                parsed = parser(pdf_path)
                days, totals_ok, diff = _validate_result(parsed)
                ok = True
                if strict and days is not None:
                    ok = 28 <= days <= 31
                    if totals_ok is False:
                        ok = False
                results.append(
                    FileResult(
                        path=str(pdf_path),
                        parser=parser_name,
                        ok=ok,
                        days=days,
                        totals_ok=totals_ok,
                        ore_lavorate_diff=diff,
                    )
                )
            except Exception as exc:  # noqa: BLE001 - top-level test harness
                results.append(
                    FileResult(
                        path=str(pdf_path),
                        parser=parser_name,
                        ok=False,
                        error=f"{exc.__class__.__name__}: {exc}",
                    )
                )
    return results


def _print_summary(results: list[FileResult]) -> int:
    total = len(results)
    failed = [r for r in results if not r.ok]
    print(f"Total files: {total}")
    print(f"Failed: {len(failed)}")
    if failed:
        print("Failures:")
        for item in failed:
            print(f"- {item.path} [{item.parser}] {item.error or 'validation failed'}")
        return 1
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run cartellino/timbrature parsers against samples/."
    )
    parser.add_argument("--root", default="samples", help="Samples folder root")
    parser.add_argument(
        "--mode",
        choices=["folder", "auto", "cartellino", "timbrature_compact", "timbrature_elenco"],
        default="folder",
        help="Parser selection: folder uses parser-named samples folders.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Fail when days are out of 28-31 or ore_lavorate diff exceeds 0.05.",
    )
    parser.add_argument("--json-out", help="Write results to JSON file.")
    args = parser.parse_args()

    results = run_samples(Path(args.root), args.mode, args.strict)
    if args.json_out:
        payload = {
            "total": len(results),
            "failed": len([r for r in results if not r.ok]),
            "results": [asdict(item) for item in results],
        }
        Path(args.json_out).write_text(json.dumps(payload, indent=2), encoding="utf-8")

    return _print_summary(results)


if __name__ == "__main__":
    raise SystemExit(main())
