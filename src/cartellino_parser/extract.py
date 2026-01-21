from __future__ import annotations

from pathlib import Path

import pdfplumber


def extract_text(pdf_path: str | Path) -> str:
    path = Path(pdf_path)
    with pdfplumber.open(path) as pdf:
        return "\n".join(page.extract_text() or "" for page in pdf.pages)
