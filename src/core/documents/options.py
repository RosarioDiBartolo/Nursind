from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(slots=True)
class ExtractDocumentsFromIndexOptions:
    out: str
    index: str
    excluded: str
    included: str
    skip_included: bool = True
    reprocess_included: bool = False
    reprocess_excluded: bool = False
    workers: int = 8
    download_workers: int | None = None
    extract_workers: int = max(1, os.cpu_count() or 1)
    max_in_flight: int = 128
    flush_every: int = 100
    limit: int = 0
    log_every: int = 50
    min_normal_score: float = 0.72
    min_score_delta: float = 0.08
    report: str = "extract_documents_from_index.report.json"
    verbose: bool = False
