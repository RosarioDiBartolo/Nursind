from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Literal


PipelineStage = Literal[
    "scan",
    "documents",
    "events",
    "shifts",
    "enrichment",
    "aggregation",
]


class ArtifactScope(StrEnum):
    STAGE_DIR = "stage_dir"
    PIPELINE_ROOT = "pipeline_root"
    OUTPUT_ROOT = "output_root"


@dataclass(frozen=True, slots=True)
class ArtifactRef:
    step: str
    artifact: str

