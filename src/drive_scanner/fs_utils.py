import os
 


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def ensure_parent_dir(path: str) -> None:
    ensure_dir(os.path.dirname(path) or ".")

 