"""Generate the standalone, offline parser catalogue from checked-in examples."""

from html import escape
import json

from core.tools.parser_catalog.cases import FIXTURES, ROOT, Case, inspect_case, load_case
from core.tools.parser_catalog.evidence import Sources, document_line, identity_marks

OUTPUT = ROOT / "docs" / "parsers" / "index.html"
TEMPLATE = ROOT / "docs" / "parsers" / "template.html"


def render_case(case: Case, sources: Sources, opened: bool) -> str:
    view = inspect_case(case)
    identity, page = view["identity"], view["page"]
    rows = {row.line.line_no: row for row in view["rows"]}
    emitted: dict[int, list[dict]] = {}
    for event in view["events"]:
        emitted.setdefault(event["source_line_no"], []).append(event)

    parser_code = sources.link(f"{case.parser_id}:{type(view['parser']).__name__}.parse_document", "Parser")
    format_code = sources.link(f"{case.parser_id}:{type(view['parser']).__name__}.score_document", "Riconoscimento formato")
    explicit_code = sources.link("common:explicit_events_for_line", "Prefissi e riferimenti")
    date_code = sources.link("page_analysis:resolve_page_year_month", "Periodo")
    identity_code = sources.link("identity:employee_identity", "Identità")
    page_code = sources.link("page_analysis:parse_document_pages", "Decisione pagina")
    line_html = []
    for line in view["lines"]:
        number = line.line_no
        anchor = f"{case.slug}-r{number}"
        row = rows.get(number)
        note = case.notes.get(str(number))
        name_found = identity_marks(line, identity)
        refs = [identity_code] if name_found else []
        if row:
            refs.append(parser_code)
        if row and any(event.source_origin == "text_regex" for event in row.events):
            refs.append(explicit_code)
        if number == 1:
            refs.append(format_code)
        period_evidence = number in case.period_lines
        if period_evidence or (note and not row and not name_found):
            refs.append(date_code)
        candidates = ", ".join(f"{event.event_kind} {event.event_time_hhmm}"
                               + (f" ({event.source_slot})" if event.source_slot else "")
                               for event in row.events) if row else ""
        if row:
            row_caption = f"Giorno {row.day} · {row.dow} · "
            row_caption += (f"Candidati {candidates} · {len(emitted.get(number, []))} emessi"
                            if candidates else "Nessun candidato")
        else:
            row_caption = ""
        caption = " ".join(filter(None, [escape(row_caption), escape(note or ""), " · ".join(refs)]))
        source = document_line(line, row, identity, period_evidence=period_evidence)
        line_html.append(
            f'<div id="{anchor}" class="doc-line{" parsed" if row else ""}">'
            f'<a class="line-no" href="#{anchor}" aria-label="Riga {number}">{number}</a>'
            f'<div><div class="raw">{source}</div>'
            f'{f"<div class=note>{caption}</div>" if caption else ""}</div></div>'
        )

    year, month = page["page_year"], page["page_month"]
    period = f"{year:04d}-{month:02d}" if year and month else "Assente"
    name = identity["employee"] or ", ".join(item["name"] for item in identity["identity_candidates"]) or "Assente"
    name += f" · {identity['identity_status']}"
    if identity["employee_identifier"]:
        name += f" · {identity['employee_identifier']}"
    fixture = case.path.relative_to(ROOT).as_posix()
    metrics = (
        f'<dl class="metrics"><div><dt>{identity_code}</dt><dd>{escape(name)}</dd></div>'
        f'<div><dt>{date_code}</dt><dd>{period} <small>{escape(page["year_month_source"])}</small></dd></div>'
        f'<div><dt>{page_code}</dt><dd>{escape(page["decision"])}</dd></div></dl>'
    )

    output_rows = []
    for event in view["events"]:
        number = event["source_line_no"]
        output_rows.append(
            f'<tr><td>{escape(event["event_ts"])}</td><td>{escape(event["event_kind"])}</td>'
            f'<td>{escape(event["event_raw"])}</td><td>{escape(event["source_origin"])}</td>'
            f'<td>{escape(event["source_slot"])}</td><td><a href="#{case.slug}-r{number}">'
            f'Riga {number}</a><small>{escape(event["source_event_ref"])}</small></td></tr>'
        )
    output = (
        '<div class="scroll"><table><thead><tr><th>Data e ora</th><th>Tipo</th><th>Valore sorgente</th>'
        '<th>Origine</th><th>Slot</th><th>Riferimento</th></tr></thead>'
        f'<tbody>{"".join(output_rows)}</tbody></table></div>'
        if output_rows else "<p>Nessun evento emesso per questa pagina.</p>"
    )
    return (
        f'<details class="case" id="{case.slug}"{" open" if opened else ""}>'
        f'<summary>{escape(case.title)} <small>{len(view["events"])} eventi emessi</small></summary>'
        f'{metrics}<div class="paper">{"".join(line_html)}</div>'
        f'<h4>Eventi emessi</h4>{output}'
        f'<a class="file" href="../../{escape(fixture)}">Apri il documento di test e le aspettative</a></details>'
    )


def render_catalog() -> str:
    sources = Sources()
    menu, sections = [], []
    for directory in sorted(FIXTURES.iterdir()):
        if not directory.is_dir():
            continue
        guide = json.loads((directory / "guide.json").read_text(encoding="utf-8"))
        parser_id = directory.name
        title = escape(guide["title"])
        menu.append(f'<a href="#{parser_id}">{title}</a>')
        steps = []
        for label, explanation, *refs in guide["steps"]:
            links = " · ".join(sources.link(ref) for ref in refs)
            steps.append(f'<li><strong>{escape(label)}</strong><p>{escape(explanation)}</p>{links}</li>')
        source_docs = [
            f'<a href="../../{path.relative_to(ROOT).as_posix()}">{escape(path.name)}</a>'
            for path in sorted(directory.glob("*.txt"))
        ]
        legacy = (f'<p class="legacy">Documenti testuali di routing · {" · ".join(source_docs)}</p>'
                  if source_docs else "")
        cases = "".join(render_case(load_case(path), sources, index == 0)
                        for index, path in enumerate(sorted(directory.glob("*.case.json"))))
        sections.append(
            f'<section id="{parser_id}"><h2>{title}</h2><details class="rules"><summary>Regole di estrazione e limiti'
            f'</summary><ul>{"".join(steps)}</ul></details>{cases}{legacy}</section>'
        )

    template = TEMPLATE.read_text(encoding="utf-8")
    return (template.replace("<!-- menu -->", "\n".join(menu))
            .replace("<!-- sections -->", "\n".join(sections))
            .replace("<!-- source -->", "\n".join(sources.excerpts.values())))

