from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def test_primary_scripts_start_and_expose_config_flag() -> None:
    root = Path(__file__).resolve().parents[1]
    scripts = (
        "scan.py",
        "extract_documents.py",
        "extract_events.py",
        "filter_midnight.py",
        "pair_events.py",
        "enrich_shifts.py",
        "summarize_shifts.py",
    )

    for script in scripts:
        result = subprocess.run(
            [sys.executable, str(root / "src" / "scripts" / script), "--help"],
            cwd=root,
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr
        assert "--config" in result.stdout
