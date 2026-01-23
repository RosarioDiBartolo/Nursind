import json
import time
import os


def write_manifest(out_dir: str, root_id: str, reports: list[dict]):
    report_path = os.path.join(out_dir, "manifest.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "root_id": root_id,
                "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "employee_count": len(reports),
                "employees": reports,
            },
            f,
            indent=2,
        )
    return report_path
