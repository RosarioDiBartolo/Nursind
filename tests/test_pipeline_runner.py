from __future__ import annotations

import pytest

from core.errors import ConfigurationError
from core.pipeline.runner import run_pipeline_steps, select_pipeline_steps


def test_select_pipeline_steps_preserves_canonical_order() -> None:
    assert select_pipeline_steps(steps=["summarize_shifts", "scan", "pair_events"]) == [
        "scan",
        "pair_events",
        "summarize_shifts",
    ]


def test_select_pipeline_steps_supports_aliases_ranges_and_skips() -> None:
    assert select_pipeline_steps(
        steps=["all"],
        start="events",
        end="summary",
        skip=["filter,pairing"],
    ) == ["extract_events", "enrich_shifts", "summarize_shifts"]


def test_select_pipeline_steps_rejects_unknown_names() -> None:
    with pytest.raises(ConfigurationError, match="Unknown pipeline step"):
        select_pipeline_steps(steps=["made_up"])


def test_run_pipeline_steps_calls_selected_actions_in_order() -> None:
    calls: list[tuple[str, bool]] = []
    actions = {
        name: (lambda _config, verbose, step=name: calls.append((step, verbose)))
        for name in ("scan", "pair_events", "summarize_shifts")
    }

    ran = run_pipeline_steps(
        object(),  # type: ignore[arg-type]
        steps=["summary", "scan", "pairing"],
        verbose=True,
        actions=actions,
    )

    assert ran == ["scan", "pair_events", "summarize_shifts"]
    assert calls == [
        ("scan", True),
        ("pair_events", True),
        ("summarize_shifts", True),
    ]

