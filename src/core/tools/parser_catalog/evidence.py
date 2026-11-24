"""Format source references and highlights for the synthetic document viewer."""

from html import escape
from importlib import import_module
import inspect
import re
from textwrap import dedent

from core.tools.parser_catalog.cases import ROOT

EXTRACTION = "core.events.extraction"
PARSERS = {"cartellino_classic", "cartellino_ocr", "cartellino_unico", "situazione_mensile",
           "timbrature_web", "workflow_monthly", "common", "router"}


class Sources:
    def __init__(self):
        self.excerpts: dict[str, str] = {}

    def link(self, reference: str, label: str | None = None) -> str:
        module_name, _, qualname = reference.partition(":")
        namespace = f"{EXTRACTION}.parsers" if module_name in PARSERS else EXTRACTION
        module = import_module(f"{namespace}.{module_name}" if module_name != "parsing" else "core.parsing")
        anchor = "code-" + reference.replace(":", "-").replace(".", "-")
        if anchor not in self.excerpts:
            symbol = module
            for part in qualname.split("."):
                symbol = getattr(symbol, part)
            raw, first_line = inspect.getsourcelines(symbol)
            path = ROOT.joinpath(inspect.getfile(symbol)).relative_to(ROOT).as_posix()
            snippet = "\n".join(f"{n:>4}  {line}" for n, line in
                                enumerate(dedent("".join(raw)).splitlines(), start=first_line))
            self.excerpts[anchor] = (
                f'<details class="source" id="{anchor}"><summary>{escape(qualname)} '
                f'<span>{escape(path)} · riga {first_line}</span></summary>'
                f'<a href="../../{escape(path)}">Apri il file sorgente</a>'
                f'<pre>{escape(snippet)}</pre></details>'
            )
        return f'<a href="#{anchor}">{escape(label or qualname.split(".")[-1])}</a>'


def identity_marks(line, identity: dict) -> list[tuple[int, int, str, str]]:
    marks = []
    for candidate in identity["identity_candidates"]:
        name = candidate["name"]
        label = f"Candidato {name} · {identity['identity_status']}"
        match = re.search(re.escape(name), line.text, re.IGNORECASE)
        if match:
            marks.append((match.start(), match.end(), "identity", label))
        elif candidate["evidence"] == line.text.strip():
            marks.append((0, len(line.text), "identity", label))
    return marks


def event_mark(event, start: int, end: int) -> tuple[int, int, str, str]:
    return start, end, "punch", f"{event.event_kind} {event.event_time_hhmm}"


def highlighted(text: str, marks: list[tuple[int, int, str, str]]) -> str:
    fragments = []
    cursor = 0
    for start, end, kind, label in sorted(marks):
        if not cursor <= start < end <= len(text):
            continue
        fragments.extend((escape(text[cursor:start]),
                          f'<mark class="{kind}" title="{escape(label)}">{escape(text[start:end])}</mark>'))
        cursor = end
    fragments.append(escape(text[cursor:]))
    return "".join(fragments)


def document_line(line, row, identity: dict, *, period_evidence: bool) -> str:
    events = row.events if row else ()
    marks = identity_marks(line, identity)
    if period_evidence:
        marks.append((0, len(line.text), "period", "Evidenza del periodo"))
    if not line.words:
        marks.extend(event_mark(event, event.match_start, event.match_end) for event in events
                     if event.match_start is not None and event.match_end is not None)
        return highlighted(line.text, marks)

    words = []
    for index, word in enumerate(line.words):
        event = next((event for event in events
                      if event.source_word_start is not None and event.source_word_end is not None
                      and event.source_word_start <= index <= event.source_word_end), None)
        token = str(word["text"])
        token_html = highlighted(token, [event_mark(event, 0, len(token))] if event else [])

        left = float(word["x0"]) * 1.55
        words.append(f'<span style="left:{left:g}px">{token_html}</span>')
    width = max(float(word["x1"]) for word in line.words) * 1.55 + 24
    return f'<span class="coords" style="width:{width:g}px">{"".join(words)}</span>'
