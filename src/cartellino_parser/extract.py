from __future__ import annotations

from pathlib import Path
from typing import BinaryIO, Union

import pdfplumber


def extract_text(source: Union[str, Path, BinaryIO]) -> str:
    if isinstance(source, (str, Path)):
        path = Path(source)
        with pdfplumber.open(path) as pdf:
            return "\n".join(page.extract_text() or "" for page in pdf.pages)
    source.seek(0)
    with pdfplumber.open(source) as pdf:
        return "\n".join(page.extract_text() or "" for page in pdf.pages)
