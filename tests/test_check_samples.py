from __future__ import annotations

from pathlib import Path
import json
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from cartellino_parser import parse_pdf as parse_cartellino_pdf  # noqa: E402
from parser_service import check_samples  # noqa: E402
from timbrature_elenco_compact_parser import parse_pdf as parse_compact_pdf  # noqa: E402
from timbrature_elenco_parser import parse_pdf as parse_elenco_pdf  # noqa: E402


PARSERS = {
    "cartellino": parse_cartellino_pdf,
    "timbrature_compact": parse_compact_pdf,
    "timbrature_elenco": parse_elenco_pdf,
}


def _iter_pdfs(folder: Path) -> list[Path]:
    return sorted(folder.glob("*.pdf"))


def _write_outputs(out_dir: Path, stem: str, parsed) -> None:
    file_out = out_dir / stem
    file_out.mkdir(parents=True, exist_ok=True)
    parsed.days_df.to_csv(file_out / "days.csv", index=False)
    parsed.pairs_df.to_csv(file_out / "pairs.csv", index=False)
    (file_out / "totals.json").write_text(
        json.dumps(parsed.totals, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    report = {"meta": parsed.meta, "totals": parsed.totals, "validation": parsed.validation}
    (file_out / "report.json").write_text(
        json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8"
    )


def _generate_outputs(samples_root: Path, output_root: Path) -> None:
    for folder, parser in PARSERS.items():
        pdfs = _iter_pdfs(samples_root / folder)
        if not pdfs:
            raise AssertionError(f"No sample PDFs found under {samples_root / folder}")
        for pdf_path in pdfs:
            parsed = parser(pdf_path)
            _write_outputs(output_root / folder, pdf_path.stem, parsed)


def test_check_samples_full_outputs(tmp_path: Path) -> None:
    samples_root = ROOT / "samples"
    assert samples_root.exists(), "Samples folder missing."

    output_root = tmp_path / "output"
    _generate_outputs(samples_root, output_root)

    issues = 0
    for name in ("cartellino", "timbrature_compact", "timbrature_elenco"):
        issues += check_samples._check_group(  # noqa: SLF001 - intentional test hook
            name=name,
            samples_root=samples_root,
            output_root=output_root,
            sample_ratio=1.0,
            limit=None,
            allow_extract=True,
            diagnose_overcount=False,
            max_lines=8,
        )

    assert issues == 0
