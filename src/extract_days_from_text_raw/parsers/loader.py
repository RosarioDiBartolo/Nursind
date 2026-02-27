from __future__ import annotations

import importlib
import inspect
import pkgutil
from functools import lru_cache

from . import __path__ as parser_pkg_path
from .base import BaseFormatParser


@lru_cache(maxsize=1)
def load_parsers() -> tuple[BaseFormatParser, ...]:
    discovered: list[BaseFormatParser] = []
    for module_info in pkgutil.iter_modules(parser_pkg_path):
        if module_info.name in {"base", "common", "loader", "router"}:
            continue
        module = importlib.import_module(f"{__package__}.{module_info.name}")
        for _, candidate in inspect.getmembers(module, inspect.isclass):
            if not issubclass(candidate, BaseFormatParser):
                continue
            if candidate is BaseFormatParser:
                continue
            if candidate.__module__ != module.__name__:
                continue
            parser = candidate()
            if not parser.parser_id:
                continue
            discovered.append(parser)

    if not discovered:
        raise RuntimeError("No format parsers discovered in extract_days_from_text_raw.parsers")
    return tuple(sorted(discovered, key=lambda parser: parser.parser_id))
