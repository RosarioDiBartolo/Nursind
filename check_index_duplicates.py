import argparse
from collections import defaultdict

from drive_scanner.index_service import Index


def _key_for(file_item, mode: str):
    if mode == "file_id":
        return file_item.file_id
    if mode == "file_name":
        return file_item.file_name
    if mode == "pair":
        return (file_item.file_id, file_item.file_name)
    raise ValueError(f"Unknown mode: {mode}")


def _format_key(key, mode: str) -> str:
    if mode == "pair":
        file_id, file_name = key
        return f"{file_id or '<missing_id>'} | {file_name or '<missing_name>'}"
    return key or "<missing>"


def find_duplicates(files, mode: str):
    locations = defaultdict(list)
    for idx, item in enumerate(files, 1):
        key = _key_for(item, mode)
        locations[key].append(idx)
    return {key: idxs for key, idxs in locations.items() if len(idxs) > 1}


def check_index(path: str) -> int:
    index = Index.load_index(path, strict=False)
    exit_code = 0
    for mode in ("file_id", "file_name", "pair"):
        dupes = find_duplicates(index.files, mode)
        if dupes:
            exit_code = 1
            print(f"{path} | duplicates by {mode}: {len(dupes)}")
            for key, idxs in sorted(dupes.items(), key=lambda kv: len(kv[1]), reverse=True):
                print(f"  { _format_key(key, mode) } -> {len(idxs)} hits (rows {', '.join(map(str, idxs))})")
        else:
            print(f"{path} | duplicates by {mode}: none")
    return exit_code


def main() -> int:
    parser = argparse.ArgumentParser(description="Check index files for duplicate entries.")
    parser.add_argument("indexes", nargs="+", help="Index JSON paths to check.")
    args = parser.parse_args()

    exit_code = 0
    for path in args.indexes:
        exit_code |= check_index(path)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
