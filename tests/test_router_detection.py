from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from parser_service.router import (  # noqa: E402
    detect_document_family,
    detect_timbrature_variant,
)
from parser_shared.models import ParserDetectionError  # noqa: E402


def test_detect_document_family_cartellino():
    text = "\n".join(
        [
            "Cartellino mensile",
            "Ore dovute programmate",
            "01 LU 08:00 12:00",
        ]
    )
    assert detect_document_family(text) == "cartellino"


def test_detect_document_family_timbrature():
    text = "\n".join(
        [
            "Elenco timbrature",
            "Utente Mario Rossi",
            "1 lun E 08:00 U 12:00 2,00 3,00",
        ]
    )
    assert detect_document_family(text) == "timbrature"


def test_detect_document_family_ambiguous_raises():
    with pytest.raises(ParserDetectionError):
        detect_document_family("Documento privo di marker")


def test_detect_timbrature_variant_compact():
    text = "\n".join(
        [
            "1 lun E 08:00 U 12:00 0,50 2,00",
            "2 mar E 08:00 U 12:00 0,50 2,00",
            "3 mer E 08:00 U 12:00 0,50 2,00",
        ]
    )
    assert detect_timbrature_variant(text, strict=True) == "timbrature_compact"


def test_detect_timbrature_variant_elenco():
    text = "\n".join(
        [
            "1 lun E 08:00 U 12:00 2,00 3,00",
            "2 mar E 08:00 U 12:00 2,00 3,00",
            "3 mer E 08:00 U 12:00 2,00 3,00",
            "4 gio E 08:00 U 12:00 2,00 3,00",
            "5 ven E 08:00 U 12:00 2,00 3,00",
        ]
    )
    assert detect_timbrature_variant(text, strict=True) == "timbrature_elenco"
