from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.scan_directory import cli  # noqa: E402


def test_scan_cli_calls_runtime(monkeypatch, tmp_path):
    monkeypatch.setattr(cli.config, "validate_env", lambda: None)
    monkeypatch.setattr(cli, "load_creds", lambda: object())
    monkeypatch.setattr(cli, "get_drive_service", lambda _creds: object())

    called = {}

    def fake_run_scan(**kwargs):
        called.update(kwargs)
        return {"included_total": 0, "filtered_total": 0}

    monkeypatch.setattr(cli, "run_scan", fake_run_scan)

    out_dir = tmp_path / "scan"
    argv = [
        "scan",
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
    monkeypatch.setattr(sys, "argv", argv)

    rc = cli.main()

    assert rc == 0
    assert called["root_id"] == "root-123"
    assert called["workers"] == 3
    assert called["included_path"].endswith("inc.index.json")
    assert called["filtered_path"].endswith("fil.index.json")
    assert called["report_path"].endswith("rep.json")


def test_scan_cli_uses_default_report_name(monkeypatch, tmp_path):
    monkeypatch.setattr(cli.config, "validate_env", lambda: None)
    monkeypatch.setattr(cli, "load_creds", lambda: object())
    monkeypatch.setattr(cli, "get_drive_service", lambda _creds: object())

    called = {}

    def fake_run_scan(**kwargs):
        called.update(kwargs)
        return {"included_total": 0, "filtered_total": 0}

    monkeypatch.setattr(cli, "run_scan", fake_run_scan)

    out_dir = tmp_path / "scan"
    argv = [
        "scan",
        "--root",
        "root-123",
        "--out",
        str(out_dir),
    ]
    monkeypatch.setattr(sys, "argv", argv)

    rc = cli.main()

    assert rc == 0
    assert called["report_path"].endswith("scan_directory.report.json")
