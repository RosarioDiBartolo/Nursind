"""Independent mapping orchestration, reusing the primary document step."""
from __future__ import annotations

from dataclasses import replace
import logging
import time

from core.config import PipelineConfig
from core.drive.auth_service import load_creds
from core.drive.drive_client import get_drive_service
from core.drive.index.map_index import MapIndex
from core.pipeline.steps import run_extract_documents_step

from .scan import scan_folder
from .service import map_extracted_folder

STEPS = ("scan", "extract_documents", "extract_events")
logger = logging.getLogger(__name__)


def mapping_config(config: PipelineConfig) -> PipelineConfig:
    # Keep mapping artifacts separate from primary pipeline artifacts.
    settings = dict(config.step("extract_documents"))
    # The inventory covers the full folder; a partial extraction cannot map it.
    settings.update(limit=0, reprocess_included=True, reprocess_excluded=True)
    return replace(config, output_dir_stem=config.output_dir_stem / "mapping",
                   steps={**config.steps, "extract_documents": settings})


def run_mapping(config: PipelineConfig, *, start: str = "scan", through: str = "extract_events",
                verbose: bool = False) -> dict | None:
    first, last = STEPS.index(start), STEPS.index(through)
    if first > last:
        raise ValueError("--from must precede --through")
    config = mapping_config(config)
    started = time.monotonic()
    logger.info("Mapping started: %s | steps=%s | output=%s", config.name,
                ", ".join(STEPS[first:last + 1]), config.paths.pipeline_root)
    report = None
    for step in STEPS[first:last + 1]:
        step_started = time.monotonic()
        logger.info("Mapping step started: %s", step)
        if step == "scan":
            scan_folder(get_drive_service(load_creds()), config)
        elif step == "extract_documents":
            index = MapIndex.load_index(str(config.paths.scan_included_index), strict=True)
            if index.root_id != config.drive_root_id:
                raise ValueError("Scan index belongs to another Drive folder; run scan again")
            run_extract_documents_step(config, verbose)
        else:
            report = map_extracted_folder(config)
        logger.info("Mapping step completed: %s | %.1fs", step, time.monotonic() - step_started)
    logger.info("Mapping completed: %s | %.1fs", config.name, time.monotonic() - started)
    return report
