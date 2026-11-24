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

from core.folder_mapping.csv_input import scan_csv
from core.folder_mapping.grouping import group_employees, run_employee_step
from .scan import scan_folder
from .service import map_extracted_folder

STEPS = ("scan", "extract_documents", "extract_events", "group_by_employee", "filter_midnight", "pair_events", "enrich_shifts", "summarize_shifts")
logger = logging.getLogger(__name__)


def mapping_config(config: PipelineConfig) -> PipelineConfig:
    # Keep mapping artifacts separate from primary pipeline artifacts.
    settings = dict(config.step("extract_documents"))
    # The inventory covers the full folder; a partial extraction cannot map it.
    settings.update(limit=0, reprocess_included=True, reprocess_excluded=True)
    return replace(config, output_dir_stem=config.output_dir_stem / "mapping",
                   steps={**config.steps, "extract_documents": settings})


def run_mapping(config: PipelineConfig, *, start: str = "scan", through: str = "extract_events",
                verbose: bool = False, input_csv=None, identities=None) -> dict | None:
    first, last = STEPS.index(start), STEPS.index(through)
    if first > last:
        raise ValueError("--from must precede --through")
    if input_csv and start != "scan":
        raise ValueError("--input-csv requires --from scan")
    config = mapping_config(config)
    started = time.monotonic()
    logger.info("Mapping started: %s | steps=%s | output=%s", config.name,
                ", ".join(STEPS[first:last + 1]), config.paths.pipeline_root)
    report = None
    for step in STEPS[first:last + 1]:
        step_started = time.monotonic()
        logger.info("Mapping step started: %s", step)
        if step == "scan":
            drive = get_drive_service(load_creds())
            if input_csv:
                scan_csv(drive, config, input_csv)
            else:
                scan_folder(drive, config)
        elif step == "extract_documents":
            index = MapIndex.load_index(str(config.paths.scan_included_index), strict=True)
            if index.root_id != config.drive_root_id:
                raise ValueError("Scan index belongs to another Drive folder; run scan again")
            run_extract_documents_step(config, verbose)
        elif step == "extract_events":
            report = map_extracted_folder(config)
        elif step == "group_by_employee":
            group_employees(config, identities)
        else:
            run_employee_step(config, step, verbose)
        logger.info("Mapping step completed: %s | %.1fs", step, time.monotonic() - step_started)
    logger.info("Mapping completed: %s | %.1fs", config.name, time.monotonic() - started)
    return report
