from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version
from pathlib import Path


def _namespace_root() -> str:
    return str(Path(__file__).resolve().parent.parent)


_ROOT = _namespace_root()
if _ROOT not in __path__:
    __path__.append(_ROOT)

__all__ = ["PipelineClient", "__version__"]


try:
    __version__ = version("cartellino-parser")
except PackageNotFoundError:
    __version__ = "0+unknown"


def __getattr__(name: str):
    if name == "PipelineClient":
        from .client import PipelineClient

        return PipelineClient
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
