from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from cartellino_parser.models import ScanRequest, StageReport  # noqa: E402
from cartellino_parser.scan_directory import cli  # noqa: E402
from cartellino_parser.scan_directory.artifacts import SCAN_ARTIFACTS  # noqa: E402


def test_scan_cli_calls_pipeline_client(monkeypatch, tmp_path):
    captured = {}

    class FakeClient:
        def scan(self, request):
            captured["request"] = request
            return StageReport(
                stage="scan_directory",
                stats={"employee_total": 1, "included_total": 2, "filtered_total": 3},
            )

    monkeypatch.setattr(cli, "PipelineClient", FakeClient)
    monkeypatch.setattr(cli, "setup_logging", lambda _verbose: None)

    out_dir = tmp_path / "scan"
    rc = cli.main(
        [
            "--root",
            "root-123",
            "--out",
            str(out_dir),
            "--included",
            "inc.index.json",
            "--filtered",
            "fil.index.json",
            "--report",
            "rep.json",
            "--workers",
            "3",
        ]
    )

    assert rc == 0
    request = captured["request"]
    assert isinstance(request, ScanRequest)
    assert request.root_id == "root-123"
    assert request.out == str(out_dir)
    assert request.included == "inc.index.json"
    assert request.filtered == "fil.index.json"
    assert request.report == "rep.json"
    assert request.workers == 3


def test_scan_cli_uses_default_report_name(monkeypatch, tmp_path):
    captured = {}

    class FakeClient:
        def scan(self, request):
            captured["request"] = request
            return StageReport(
                stage="scan_directory",
                stats={"employee_total": 0, "included_total": 0, "filtered_total": 0},
            )

    monkeypatch.setattr(cli, "PipelineClient", FakeClient)
    monkeypatch.setattr(cli, "setup_logging", lambda _verbose: None)

    out_dir = tmp_path / "scan"
    rc = cli.main(
        [
            "--root",
            "root-123",
            "--out",
            str(out_dir),
        ]
    )

    assert rc == 0
    request = captured["request"]
    assert request.out == str(out_dir)
    assert request.report == SCAN_ARTIFACTS.report_json
