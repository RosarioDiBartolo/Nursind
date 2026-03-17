from __future__ import annotations


class CartellinoParserError(Exception):
    """Base exception for the public package API."""


class PackagingError(CartellinoParserError):
    """Raised when the package layout or installation is invalid."""


class ConfigurationError(CartellinoParserError):
    """Raised when required configuration is missing or invalid."""


class CredentialsError(ConfigurationError):
    """Raised when credentials are required but unavailable."""


class ArtifactContractError(CartellinoParserError):
    """Raised when pipeline artifacts do not match the expected contract."""


class ProcessingError(CartellinoParserError):
    """Raised when a pipeline stage cannot complete successfully."""


class OptionalDependencyError(ConfigurationError):
    """Raised when an optional dependency is required but not installed."""

