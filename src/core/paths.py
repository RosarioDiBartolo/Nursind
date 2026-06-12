from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class PipelinePaths:
    repository_root: Path
    pipeline_root: Path
    scan_dir: Path
    documents_dir: Path
    events_dir: Path
    shifts_dir: Path
    enrichment_dir: Path
    aggregation_dir: Path
    scan_included_index: Path
    scan_filtered_index: Path
    scan_report: Path
    documents_included_index: Path
    documents_excluded_index: Path
    documents_report: Path
    events_csv: Path
    pages_csv: Path
    events_report: Path
    cleaned_events_csv: Path
    removed_midnight_csv: Path
    filter_report: Path
    pairing_report: Path
    enrichment_report: Path
    summary_csv: Path
    summary_report: Path

    @property
    def root_output(self) -> Path:
        return self.pipeline_root

    @property
    def scan_output(self) -> Path:
        return self.scan_dir

    @property
    def documents_output(self) -> Path:
        return self.documents_dir

    @property
    def events_output(self) -> Path:
        return self.events_dir

    @property
    def shifts_output(self) -> Path:
        return self.shifts_dir

    @property
    def enrichment_output(self) -> Path:
        return self.enrichment_dir

    @property
    def aggregation_output(self) -> Path:
        return self.aggregation_dir

    def ensure_stage_dirs(self) -> None:
        for path in (
            self.scan_dir,
            self.documents_dir,
            self.events_dir,
            self.shifts_dir,
            self.enrichment_dir,
            self.aggregation_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)


def build_pipeline_paths(
    *,
    repository_root: str | Path,
    base_output_dir: str | Path,
    pipeline_name: str,
) -> PipelinePaths:
    repo = Path(repository_root).resolve()
    base = Path(f"{base_output_dir}.out_dir")
    if not base.is_absolute():
        base = repo / base
    root = base.resolve() / pipeline_name
    scan = root / "scan"
    documents = root / "documents"
    events = root / "events"
    shifts = root / "shifts"
    enrichment = root / "enrichment"
    aggregation = root / "aggregation"
    return PipelinePaths(
        repository_root=repo,
        pipeline_root=root,
        scan_dir=scan,
        documents_dir=documents,
        events_dir=events,
        shifts_dir=shifts,
        enrichment_dir=enrichment,
        aggregation_dir=aggregation,
        scan_included_index=scan / "included.index.json",
        scan_filtered_index=scan / "filtered.index.json",
        scan_report=scan / "scan_directory.report.json",
        documents_included_index=documents / "included_documents.index.json",
        documents_excluded_index=documents / "excluded_documents.index.json",
        documents_report=documents / "extract_documents_from_index.report.json",
        events_csv=events / "events.csv",
        pages_csv=events / "pages.csv",
        events_report=events / "extract_events.report.json",
        cleaned_events_csv=events / "events.cleaned.csv",
        removed_midnight_csv=events / "events.midnight_removed.csv",
        filter_report=events / "events.clean_midnight.report.json",
        pairing_report=shifts / "pair_employee_events.report.json",
        enrichment_report=enrichment / "turni_enrichment.stats.json",
        summary_csv=aggregation / "turni_employee_summary.csv",
        summary_report=aggregation / "turni_employee_summary.report.json",
    )
