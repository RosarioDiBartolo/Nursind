import argparse
import os

from drive_scripts.map_index_service import MapIndex


def _default_out_path(path: str) -> str:
    base, ext = os.path.splitext(path)
    if not ext:
        ext = ".json"
    return f"{base}.map{ext}"


def main() -> int:
    parser = argparse.ArgumentParser(description="Convert legacy index (files array) to map index.")
    parser.add_argument("input", help="Input index JSON path (legacy array or map).")
    parser.add_argument(
        "--out",
        help="Output path for map index (default: <input>.map.json)",
    )
    args = parser.parse_args()

    out_path = args.out or _default_out_path(args.input)
    index = MapIndex.load_index(args.input, strict=True, allow_legacy=True)
    index.save_index(out_path)
    print(
        f"Saved map index to {out_path} (total_files={index.total_files}, employee_count={index.employee_count})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
