"""Read printed punch columns using their PDF coordinates."""

import re

from ..models import DocumentLine, ParsedEvent

_TIME_RE = re.compile(r"^(?P<h>[0-2]?\d)[:.,](?P<m>[0-5]\d)$")

def ocr_columns(line: DocumentLine, *, presenze: bool = False) -> tuple[float, ...]:
    # Require the complete, ordered series of printed entry/exit labels.
    labels = []
    for word in line.words:
        token = re.sub(r"[^a-z0-9]", "", word["text"].lower())
        if presenze:
            token = {"eont1": "ent1", "entwl": "ent1", "entel": "ent1", "entt": "ent1",
                     "user": "usc1", "userl": "usc1", "use2": "usc2", "usce2": "usc2",
                     "use72": "usc2"}.get(token, token)
        match = re.fullmatch(r"(ent|usc)([i1-6]?)([abcd]?)", token)
        if match:
            number = int(match[2].replace("i", "1")) if match[2] else None
            center = (float(word["x0"]) + float(word["x1"])) / 2
            labels.append((match[1], number, center))
    if len(labels) not in (4, 12):
        return ()
    if sum(number is None for _, number, _ in labels) > 2:
        return ()
    if any(kind != ("usc" if i % 2 else "ent") or number not in (None, i // 2 + 1)
           for i, (kind, number, _) in enumerate(labels)):
        return ()
    positions = tuple(x for _, _, x in labels)
    return positions if all(a < b for a, b in zip(positions, positions[1:])) else ()

def parse_time(token: str) -> tuple[str, int] | None:
    match = _TIME_RE.match(token)
    if not match:
        return None
    hour = int(match.group("h"))
    minute = int(match.group("m"))
    if not ((0 <= hour <= 23) or (hour == 24 and minute == 0)):
        return None
    hhmm = f"{hour:02d}:{minute:02d}"
    minutes = hour * 60 + minute
    return hhmm, minutes

def column_events(
    *, parser_id: str, day: int, dow: str, line: DocumentLine, columns: tuple[float, ...],
    centered: bool = False, roll_overnight: bool = True,
) -> tuple[ParsedEvent, ...]:
    tolerance = min(right - left for left, right in zip(columns, columns[1:])) / 4
    events = []
    occupied = set()
    entry_times: dict[int, int] = {}
    for index, word in enumerate(line.words):
        token = str(word["text"])
        parsed = parse_time(token)
        if parsed is None:
            continue
        hhmm, minutes = parsed
        x0 = float(word["x0"])
        position = (x0 + float(word["x1"])) / 2 if centered else x0
        slot = min(range(len(columns)), key=lambda candidate: abs(columns[candidate] - position))
        if centered:
            gap = min(abs(other - columns[slot]) for other in columns if other != columns[slot])
            tolerance = gap * 0.45
        if abs(columns[slot] - position) > tolerance or slot in occupied:
            continue
        occupied.add(slot)
        pair = slot // 2
        kind = "U" if slot % 2 else "E"
        if kind == "E":
            entry_times[pair] = minutes
        events.append(
            ParsedEvent(
                line=line,
                day=day,
                dow=dow,
                event_kind=kind,
                event_time_hhmm=hhmm,
                event_raw=token,
                event_pattern=f"{parser_id}:column_{slot + 1}",
                source_origin="layout_column",
                day_offset=int(roll_overnight and kind == "U" and minutes < entry_times.get(pair, minutes)),
                source_slot=f"{'ent' if kind == 'E' else 'usc'}_{pair + 1}",
                source_word_start=index,
                source_word_end=index,
                source_bbox_x0=x0,
                source_bbox_y0=float(word["y0"]),
                source_bbox_x1=float(word["x1"]),
                source_bbox_y1=float(word["y1"]),
            )
        )
    return tuple(events)
