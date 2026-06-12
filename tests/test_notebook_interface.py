from __future__ import annotations

import json

import pandas as pd

from notebooks.interface import artifact_table, preview_csv, preview_json, report_summary


def test_notebook_interface_previews_artifacts_without_mutating_them(tmp_path) -> None:
    csv_path = tmp_path / "events.csv"
    json_path = tmp_path / "report.json"
    pd.DataFrame([{"event": "E", "hour": "08:00"}]).to_csv(csv_path, index=False)
    json_path.write_text(
        json.dumps({"stage": "example", "status": "ok", "stats": {"rows": 1}}),
        encoding="utf-8",
    )

    status = artifact_table({"events": csv_path, "missing": tmp_path / "missing.csv"})
    preview = preview_csv(csv_path)
    report = report_summary(preview_json(json_path))

    assert status.set_index("artifact").loc["events", "exists"]
    assert not status.set_index("artifact").loc["missing", "exists"]
    assert preview.to_dict("records") == [{"event": "E", "hour": "08:00"}]
    assert report == {"stage": "example", "status": "ok", "stats": {"rows": 1}}
