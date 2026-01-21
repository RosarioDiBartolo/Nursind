from __future__ import annotations

import re
from typing import Dict

from cartellino_parser.utils import hhmm_to_decimal, parse_number

TOTAL_LABELS = {
    "ore_lavorate": "ORE LAVORATE",
    "ore_dovute_programmate": "ORE DOVUTE PROGRAMMATE",
    "ore_dovute_contrattuali": "ORE DOVUTE CONTRATTUALI",
    "dbcr_lordo_confermato": "DB/CR LORDO CONFERMATO",
    "dbcr_netto": "DB/CR NETTO",
    "saldo_al_mese_precedente": "SALDO AL MESE PRECEDENTE",
    "saldo_al_mese_corrente": "SALDO AL MESE CORRENTE",
}


def parse_totals(text: str) -> Dict[str, float]:
    totals: Dict[str, float] = {}
    for key, label in TOTAL_LABELS.items():
        pattern = rf"{re.escape(label)}\s+(?P<val>[+-]?\d+(?:\.\d+)?)"
        match = re.search(pattern, text)
        if match:
            totals[key] = hhmm_to_decimal(parse_number(match.group("val")))
    return totals
