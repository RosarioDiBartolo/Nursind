from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping

from core.config import PipelineConfig
from core.errors import ConfigurationError
from core.pipeline.steps import STEP_ACTIONS, PipelineStepAction

log = logging.getLogger(__name__)

STEP_ORDER: tuple[str, ...] = (
    "scan",
    "extract_documents",
    "extract_events",
    "filter_midnight",
    "pair_events",
    "enrich_shifts",
    "summarize_shifts",
)

STEP_ALIASES: dict[str, str] = {
    "all": "all",
    "documents": "extract_documents",
    "events": "extract_events",
    "filter": "filter_midnight",
    "pairing": "pair_events",
    "enrichment": "enrich_shifts",
    "summary": "summarize_shifts",
}


def normalize_step_name(name: str) -> str:
    normalized = name.strip().lower().replace("-", "_")
    normalized = STEP_ALIASES.get(normalized, normalized)
    if normalized == "all" or normalized in STEP_ORDER:
        return normalized
    valid = ", ".join(STEP_ORDER)
    raise ConfigurationError(f"Unknown pipeline step '{name}'. Valid steps: {valid}")


def _normalized_steps(values: Iterable[str] | None) -> list[str]:
    if values is None:
        return []
    steps: list[str] = []
    for value in values:
        for item in value.split(","):
            item = item.strip()
            if item:
                steps.append(normalize_step_name(item))
    return steps


def select_pipeline_steps(
    *,
    steps: Iterable[str] | None = None,
    start: str | None = None,
    end: str | None = None,
    skip: Iterable[str] | None = None,
) -> list[str]:
    selected = _normalized_steps(steps)
    skipped = set(_normalized_steps(skip))

    if not selected or "all" in selected:
        selected = list(STEP_ORDER)
    else:
        wanted = set(selected)
        selected = [name for name in STEP_ORDER if name in wanted]

    if start:
        start_name = normalize_step_name(start)
        if start_name == "all":
            raise ConfigurationError("--from cannot be 'all'")
        selected = [name for name in selected if STEP_ORDER.index(name) >= STEP_ORDER.index(start_name)]

    if end:
        end_name = normalize_step_name(end)
        if end_name == "all":
            raise ConfigurationError("--through cannot be 'all'")
        selected = [name for name in selected if STEP_ORDER.index(name) <= STEP_ORDER.index(end_name)]

    return [name for name in selected if name not in skipped]


def run_pipeline_steps(
    config: PipelineConfig,
    *,
    steps: Iterable[str] | None = None,
    start: str | None = None,
    end: str | None = None,
    skip: Iterable[str] | None = None,
    verbose: bool = False,
    actions: Mapping[str, PipelineStepAction] | None = None,
) -> list[str]:
    selected = select_pipeline_steps(steps=steps, start=start, end=end, skip=skip)
    step_actions = actions or STEP_ACTIONS

    for name in selected:
        action = step_actions.get(name)
        if action is None:
            raise ConfigurationError(f"No runner registered for pipeline step '{name}'")
        log.info("Running pipeline step: %s", name)
        action(config, verbose)

    return selected

