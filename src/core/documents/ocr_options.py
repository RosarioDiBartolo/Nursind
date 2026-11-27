from __future__ import annotations

from dataclasses import dataclass
import math


@dataclass(frozen=True, slots=True)
class OcrOptions:
    """Opt-in CPU OCR; one Tesseract thread per extraction worker."""

    enabled: bool = False
    executable: str = "tesseract"
    language: str = "ita+eng"
    dpi: int = 300
    psm: int = 6
    timeout_seconds: float = 120.0
    min_native_chars: int = 100
    min_native_lines: int = 5
    min_image_area_ratio: float = 0.25
    max_pixels: int = 25_000_000

    def __post_init__(self) -> None:
        if not isinstance(self.enabled, bool):
            raise ValueError("ocr.enabled must be a boolean")
        for name in ("executable", "language"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"ocr.{name} must be a non-empty string")
        for name, minimum, maximum in (
            ("dpi", 72, 600),
            ("psm", 1, 13),
            ("min_native_chars", 1, 10000),
            ("min_native_lines", 1, 1000),
            ("max_pixels", 1_000_000, 100_000_000),
        ):
            value = getattr(self, name)
            if type(value) is not int or not minimum <= value <= maximum:
                raise ValueError(f"ocr.{name} must be an integer in [{minimum}, {maximum}]")
        if self.psm == 2:
            raise ValueError("ocr.psm 2 does not perform text recognition")
        for name, lower_bound, upper_bound in (
            ("timeout_seconds", 1, 3600),
            ("min_image_area_ratio", 0.01, 1),
        ):
            value = getattr(self, name)
            if (
                type(value) not in (int, float)
                or not math.isfinite(value)
                or not lower_bound <= value <= upper_bound
            ):
                raise ValueError(f"ocr.{name} must be a number in [{lower_bound}, {upper_bound}]")
