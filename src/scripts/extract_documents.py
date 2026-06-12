from __future__ import annotations

from _bootstrap import bootstrap

bootstrap()

from _common import run_script
from core.documents.options import ExtractDocumentsFromIndexOptions
from core.documents.runtime import run_extraction


def run(config, verbose: bool) -> None:
    paths = config.paths
    settings = config.step("extract_documents")
    options = ExtractDocumentsFromIndexOptions(
        out=str(paths.documents_dir),
        index=str(paths.scan_included_index),
        included=str(paths.documents_included_index),
        excluded=str(paths.documents_excluded_index),
        report=str(paths.documents_report),
        workers=int(settings.get("workers", 8)),
        download_workers=settings.get("download_workers"),
        extract_workers=int(settings.get("extract_workers", 1)),
        max_in_flight=int(settings.get("max_in_flight", 128)),
        flush_every=int(settings.get("flush_every", 100)),
        limit=int(settings.get("limit", 0)),
        log_every=int(settings.get("log_every", 50)),
        min_normal_score=float(settings.get("min_normal_score", 0.72)),
        min_score_delta=float(settings.get("min_score_delta", 0.08)),
        verbose=verbose,
    )
    run_extraction(options, configure_logging=False)


if __name__ == "__main__":
    raise SystemExit(run_script("Extract canonical document artifacts from the scan index.", run))
