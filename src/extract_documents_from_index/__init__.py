"""Document extraction pipeline package."""

from __future__ import annotations

from importlib import import_module

_EXPORTS = {
    "ExtractDocumentsFromIndexOptions": (".options", "ExtractDocumentsFromIndexOptions"),
    "build_parser": (".options", "build_parser"),
    "parse_options": (".options", "parse_options"),
    "process_one_index_document": (".service", "process_one_index_document"),
    "process_many_index_documents": (".service", "process_many_index_documents"),
    "run_extraction": (".runtime", "run_extraction"),
}

__all__ = list(_EXPORTS)


def __getattr__(name: str):
    if name not in _EXPORTS:
        raise AttributeError(name)
    module_name, attr_name = _EXPORTS[name]
    module = import_module(module_name, __name__)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value
