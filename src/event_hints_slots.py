from __future__ import annotations

from typing import Any, Iterable, Protocol, Sequence

HINT_SLOT_COUNT = 12


class HintLike(Protocol):
    kind: str
    time_hhmm: str
    source: str
    confidence: float


def format_hint_confidence(value: float | None) -> str:
    if value is None:
        return ""
    text = f"{value:.6f}".rstrip("0").rstrip(".")
    return text or "0"


def hint_columns(*, slot_count: int = HINT_SLOT_COUNT) -> list[str]:
    columns = ["parser_id", "hint_count", "hint_overflow"]
    for index in range(1, slot_count + 1):
        columns.extend(
            [
                f"hint_{index}_kind",
                f"hint_{index}_time_hhmm",
                f"hint_{index}_source",
                f"hint_{index}_confidence",
            ]
        )
    return columns


def serialize_hint_slots(
    *,
    parser_id: str,
    event_hints: Sequence[HintLike],
    slot_count: int = HINT_SLOT_COUNT,
) -> dict[str, Any]:
    out: dict[str, Any] = {
        "parser_id": parser_id,
        "hint_count": len(event_hints),
        "hint_overflow": 1 if len(event_hints) > slot_count else 0,
    }
    selected = list(event_hints[:slot_count])
    for index in range(1, slot_count + 1):
        if index <= len(selected):
            hint = selected[index - 1]
            out[f"hint_{index}_kind"] = hint.kind
            out[f"hint_{index}_time_hhmm"] = hint.time_hhmm
            out[f"hint_{index}_source"] = hint.source
            out[f"hint_{index}_confidence"] = format_hint_confidence(hint.confidence)
        else:
            out[f"hint_{index}_kind"] = ""
            out[f"hint_{index}_time_hhmm"] = ""
            out[f"hint_{index}_source"] = ""
            out[f"hint_{index}_confidence"] = ""
    return out


def iter_hint_slot_values(
    row: Any,
    *,
    slot_count: int = HINT_SLOT_COUNT,
) -> Iterable[dict[str, Any]]:
    for slot_index in range(1, slot_count + 1):
        yield {
            "slot_index": slot_index,
            "kind": row.get(f"hint_{slot_index}_kind"),
            "time_hhmm": row.get(f"hint_{slot_index}_time_hhmm"),
            "source": row.get(f"hint_{slot_index}_source"),
            "confidence": row.get(f"hint_{slot_index}_confidence"),
        }
