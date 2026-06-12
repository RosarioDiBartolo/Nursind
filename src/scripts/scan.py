from __future__ import annotations

from _bootstrap import bootstrap

bootstrap()

from _common import run_script
from core.drive.auth_service import load_creds
from core.drive.drive_client import get_drive_service
from core.drive.scan.runtime import run_scan


def run(config, _verbose: bool) -> None:
    paths = config.paths
    settings = config.step("scan")
    creds = load_creds()
    run_scan(
        creds=creds,
        drive=get_drive_service(creds),
        root_id=config.drive_root_id,
        workers=int(settings.get("workers", 8)),
        included_path=str(paths.scan_included_index),
        filtered_path=str(paths.scan_filtered_index),
        report_path=str(paths.scan_report),
    )


if __name__ == "__main__":
    raise SystemExit(run_script("Scan Google Drive into canonical indexes.", run))
