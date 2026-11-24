"""Read synthetic attendance documents and analyse them with the live pipeline."""

from dataclasses import dataclass
import json
from pathlib import Path

from core.events.extraction.identity import employee_identity
from core.events.extraction.page_analysis import parse_document_pages
from core.events.extraction.parsers import resolve_parser
from core.events.extraction.parsers.common import iter_preferred_lines

ROOT = Path(__file__).resolve().parents[4]
FIXTURES = ROOT / "tests" / "fixtures" / "parsers"


@dataclass(frozen=True)
class Case:
    path: Path
    title: str
    document: dict
    notes: dict[str, str]
    period_lines: tuple[int, ...]
    expected: dict

    @property
    def parser_id(self) -> str:
        return self.path.parent.name

    @property
    def slug(self) -> str:
        return f"{self.parser_id}-{self.path.name.removesuffix('.case.json')}"


def load_case(path: Path) -> Case:
    payload = json.loads(path.read_text(encoding="utf-8"))
    lines, words = [], []
    for number, line in enumerate(payload["lines"], start=1):
        indices = []
        if not isinstance(line, str):
            y0 = 28 + 18 * number
            for token, x0, width in line:
                indices.append(len(words))
                words.append(dict(text=token, word_index=len(words), x0=x0,
                                  x1=x0 + width, y0=y0, y1=y0 + 10))
            line = " ".join(words[index]["text"] for index in indices)
        lines.append(dict(text=line, line_id=f"p1_l{number}", word_indices=indices))

    document = {"document": {"full_text": "\n".join(line["text"] for line in lines)}}
    if words:
        document["layout"] = {"pages": [
            dict(page_no=1, width=595, height=842, words=words, lines=lines),
        ]}
    return Case(path, payload["title"], document, payload["notes"],
                tuple(payload.get("period_lines", ())), payload["expected"])


def inspect_case(case: Case) -> dict:
    parser = resolve_parser(case.document)
    lines = [line for line in iter_preferred_lines(case.document) if line.text]
    identity = employee_identity(case.document["document"]["full_text"])
    _, events, pages, _, _ = parse_document_pages(
        document=case.document,
        source_context={"source_doc_json": case.path.name},
        source_path=Path("campione.pdf"),
        max_unmatched_examples_per_file=10,
    )
    return dict(parser=parser, lines=lines, identity=identity, events=events,
                page=pages[0], rows=parser.parse_document(case.document))
