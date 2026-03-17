from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from cartellino_parser import PipelineClient  # noqa: E402
from cartellino_parser.models import ExtractEventsRequest  # noqa: E402


def test_root_package_exports_pipeline_client():
    assert PipelineClient is not None


def test_pipeline_client_extract_events_via_service(monkeypatch, tmp_path: Path):
    import cartellino_parser.extract_events_from_documents.service as service

    captured = {}

    def fake_run_from_options(options):
        captured["options"] = options
        return {
            "stage": "extract_events_from_documents",
            "status": "ok",
            "inputs": {"input_dir": options.input_dir},
            "outputs": {"output_dir": options.output_dir},
            "stats": {"files_total": 0, "files_processed": 0, "files_error": 0},
            "row_totals": {"items": 0, "issues": 0},
            "items": [],
            "issues": [],
        }

    monkeypatch.setattr(service, "run_from_options", fake_run_from_options)

    client = PipelineClient()
    report = client.extract_events(
        ExtractEventsRequest(
            input_dir=tmp_path / "events",
            output_dir=tmp_path / "parsed",
            max_unmatched_examples_per_file=3,
        )
    )

    assert report.stage == "extract_events_from_documents"
    assert report.outputs["output_dir"] == str(tmp_path / "parsed")
    assert captured["options"].input_dir == str(tmp_path / "events")
    assert captured["options"].max_unmatched_examples_per_file == 3
