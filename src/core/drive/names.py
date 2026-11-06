def normalize_term(value: str) -> str:
    value = value.lower().strip().replace("_", " ").replace("-", " ")
    return " ".join(value.split())


def normalize_name(value: str | None) -> str:
    if not value:
        return "unknown"
    return " ".join(value.strip().lower().split())


def safe_name(name: str, max_len: int = 120) -> str:
    name = name.strip()
    name = name.replace("\\", "_").replace("/", "_")
    name = name.replace(":", "_").replace("*", "_")
    name = name.replace("?", "_").replace('"', "_")
    name = name.replace("<", "_").replace(">", "_").replace("|", "_")
    if len(name) > max_len:
        name = name[:max_len]
    return name or "unnamed"


def is_macos_metadata(path: str) -> bool:
    """Ignore AppleDouble sidecars and __MACOSX containers across input sources."""
    parts = path.replace("\\", "/").split("/")
    return any(part.startswith("._") or part == "__MACOSX" for part in parts)
