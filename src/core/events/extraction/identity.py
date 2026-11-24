"""Conservative employee identity extraction from labelled document content."""
from __future__ import annotations

import re
import unicodedata

from core.events.extraction.workflow_header import decode_workflow_header

_LABEL = re.compile(
    r"\b(?:nome\s+e\s+cognome|cognome\s+e\s+nome|cognome\s*/\s*nome|nominativo|dipendente)\b\s*[:=\-]?\s*",
    re.IGNORECASE,
)
_END = re.compile(
    r"\b(?:matricola|codice\s+fiscale|nato|nata|qualifica|reparto|ufficio|periodo|badge)\b|[;|\t]|\d",
    re.IGNORECASE,
)
_SEPARATE = re.compile(
    r"\bcognome\s*:\s*(?P<surname>.+?)\s+nome\s*:\s*(?P<name>.+)", re.IGNORECASE,
)


_NAME = r"[^\W\d_]+(?:[ '\u2019-]+[^\W\d_]+){1,6}"
_CLASSIC = re.compile(rf"^({_NAME})\s+-\s+(\d+)\s+Un\.\s*Org\.", re.I)
_MONTHLY = re.compile(rf"^({_NAME})\s+(\d{{4,8}})$")
_EMPLOYER = re.compile(r"^([A-Z0-9]+)\s+-\s+PERSONALE DIPENDENTE", re.M)


def employee_identity(text: str) -> dict:
    """Return explicit names and evidence; never infer from a path or filename.

    Unknown templates remain unresolved. Multiple different labelled names are
    ambiguous, rather than silently selecting the first person mentioned.
    """
    candidates: dict[str, dict] = {}
    lines = decode_workflow_header(text).splitlines()
    header = "\n".join(lines[:12])
    employer = _EMPLOYER.search(header)
    monthly = "umbriadigitale" in header.casefold() and "presenze" in header.casefold()
    identifiers = set()
    for index, line in enumerate(lines):
        evidence = line.strip()
        if index < 12:
            structured = _CLASSIC.match(evidence)
            if structured is None and monthly:
                structured = _MONTHLY.fullmatch(evidence)
            if structured:
                line = f"Nominativo: {structured[1]}"
                if employer:
                    identifiers.add(f"{employer[1]}:{structured[2]}")
        separate = _SEPARATE.search(line)
        if separate:
            line = f"Nominativo: {separate['surname']} {separate['name']}"
        for match in _LABEL.finditer(line):
            tail = line[match.end():].strip()
            if not tail and index + 1 < len(lines):
                tail = lines[index + 1].strip()
            name = _END.split(tail, maxsplit=1)[0].strip(" :-=,.")
            words = name.split()
            if not 2 <= len(words) <= 7:
                continue
            if any(word.casefold() in {"dipendente", "nominativo", "cognome", "nome", "del", "della"} for word in words):
                continue
            if not all(all(ch.isalpha() or ch in "'-’" for ch in word) for word in words):
                continue
            name = " ".join(words)
            key = unicodedata.normalize("NFC", name).casefold()
            candidates[key] = {"name": name, "evidence": evidence}
    codes = set(re.findall(r"(?i)\bcodice\s+fiscale\s*[:=-]?\s*([a-z]{6}[a-z0-9]{2}[a-z][a-z0-9]{2}[a-z][a-z0-9]{3}[a-z])\b", text.upper()))
    identifiers = codes or identifiers
    values = list(candidates.values())
    return {
        "employee": values[0]["name"] if len(values) == 1 else None,
        "identity_status": "recognized" if len(values) == 1 else "ambiguous" if values else "missing",
        "employee_identifier": next(iter(identifiers)) if len(identifiers) == len(values) == 1 else None,
        "identity_candidates": values,
    }
