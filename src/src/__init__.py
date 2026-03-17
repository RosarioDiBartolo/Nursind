from __future__ import annotations

from cartellino_parser import _namespace_root


_ROOT = _namespace_root()
if _ROOT not in __path__:
    __path__.append(_ROOT)
    
