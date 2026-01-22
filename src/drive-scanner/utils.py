import re


def safe_filename(name: str, max_len: int = 140) -> str:
    name = name.strip().replace("\n", " ")
    name = re.sub(r'[<>:"/\\|?*\x00-\x1F]', "_", name)
    name = re.sub(r"\s+", " ", name)
    if len(name) > max_len:
        name = name[:max_len].rstrip()
    return name or "untitled"
