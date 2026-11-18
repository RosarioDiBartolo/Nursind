"""Resolve schedule codes using the original lines of paired event anchors."""
from __future__ import annotations

import json
import re
from pathlib import Path


class SourceCodes:
    def __init__(self, repository_root: Path):
        self.root = repository_root
        self.documents: dict[Path, dict] = {}

    def line(self, reference: str) -> str:
        filename, anchor = reference.split("#", 1)
        path = Path(filename)
        if not path.is_absolute():
            path = self.root / path
        if path not in self.documents:
            document = json.loads(path.read_text(encoding="utf-8"))
            lines = {}
            for page in document.get("layout", {}).get("pages", []):
                for line in page.get("lines", []):
                    lines[(str(page["page_no"]), line.get("line_id"))] = line["text"]
            self.documents[path] = lines
        page = re.search(r"^p(\d+)", anchor)
        line = re.search(r"(?:^|:)line=([^:]+)", anchor)
        if page is None or line is None:
            raise ValueError("Source anchor lacks a page or line identity")
        return self.documents[path][(page[1], line[1])]

    def code(self, reference: str) -> str:
        text = self.line(reference).upper()
        # Only narrowly defined OCR aliases in code tokens, never infer from hours.
        text = re.sub(r"\bT[O0][I1L]([MPNA])\b", r"T01\1", text)
        candidates = set(re.findall(
            r"(?<![A-Z])(?:T\d{2}[A-Z]+\d*|V[MP]-\d{3}|VN[EU]\d{3})(?![A-Z0-9])",
            text,
        ))
        if len(candidates) != 1:
            raise ValueError("Schedule code missing or ambiguous in source line")
        return candidates.pop()
