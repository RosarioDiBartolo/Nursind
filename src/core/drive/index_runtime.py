from __future__ import annotations

import os
import time
from typing import Any


def resolve_output_path(out_dir: str, name: str) -> str:
    if os.path.isabs(name):
        return name
    return os.path.join(out_dir, name)


def doc_attr(doc: Any, name: str) -> Any:
    if hasattr(doc, name):
        return getattr(doc, name)
    if isinstance(doc, dict):
        return doc.get(name)
    return None


def update_index_meta(index: Any) -> None:
    index.generated_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    employees = {
        item.employee for item in index.files.values() if getattr(item, "employee", None)
    }
    index.employee_count = len(employees)
    index.total_files = len(index.files)


def maybe_flush_indexes(
    *,
    processed: int,
    flush_every: int,
    log_every: int,
    start_ts: float,
    included_index: Any,
    excluded_index: Any,
    included_map: dict,
    excluded_map: dict,
    included_path: str,
    excluded_path: str,
    logger: Any,
) -> None:
    if processed % flush_every == 0:
        included_index.files = dict(included_map)
        excluded_index.files = dict(excluded_map)
        update_index_meta(included_index)
        update_index_meta(excluded_index)
        included_index.save_index(included_path)
        excluded_index.save_index(excluded_path)

    if processed % log_every == 0:
        elapsed = max(0.01, time.time() - start_ts)
        rate = processed / elapsed * 60.0
        logger.info("Progress %s files (%.1f files/min)", processed, rate)
