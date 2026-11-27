"""Original source lots and the annual question for each Blocco 1 group."""

import re

GROUPS = {
    "Bologna": ("M P N", ("",)),
    "Grosseto": ("P N MF", ("artifacts",)),
    "La Spezia": ("P N MF", ("artifacts",)),
    "Mantova": ("M P N", ("artifacts",)),
    "Meyer": ("N MF", ("artifacts",)),
    "Pisa AOUP": ("P N MF", ("2024/artifacts", "2026/artifacts")),
    "Pisa AUSL": ("M P N", ("2024/artifacts", "2026/artifacts")),
    "Pisa FTGM": ("M P N", ("2024/artifacts", "2026/artifacts")),
    "Ragusa CGS": ("M P N", ("artifacts",)),
    "Ragusa Nursind": (
        "M P N",
        tuple(
            f"{lot}/artifacts"
            for lot in (
                "Aprile 2026",
                "Maggio 2026",
                "Nuovi ricorrenti 2025",
                "Nuovi ricorrenti 2026",
            )
        ),
    ),
}

QUESTIONS = {
    "M P N": "Quanti turni di mattina, pomeriggio e notte risultano per dipendente e anno?",
    "P N MF": "Quanti pomeriggi, notti e mattine domenicali o festive risultano per dipendente e anno?",
    "N MF": "Quante notti e mattine domenicali o festive risultano per dipendente e anno?",
}


def employee(group, raw, folder_id=None):
    if group == "Ragusa Nursind" and folder_id == "16Uz7U5dmwAsvhb8cWFOfvjsRPmKxaWf4":
        return "CABIBBO ROSA MARIA"
    if not raw or not raw.strip():
        raise ValueError(f"Dipendente mancante in {group}")
    if not group.startswith(("Ragusa", "Pisa")):
        return raw.strip()
    name = re.split(r"\s*-\s*revoca\b", raw.replace("_", " "), flags=re.I)[0]
    name = re.sub(r"(?:\s*-?\s+(?:AOUP|AUSL|FTGM|CGS|OK))\s*$", "", name, flags=re.I)
    return " ".join(name.strip(" -").upper().split())
