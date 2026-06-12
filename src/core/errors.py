from __future__ import annotations


class PipelineError(Exception):
    """Base exception for pipeline configuration and processing failures."""


class ConfigurationError(PipelineError):
    """Raised when required configuration is missing or invalid."""


class CredentialsError(ConfigurationError):
    """Raised when credentials are required but unavailable."""


class ArtifactContractError(PipelineError):
    """Raised when pipeline artifacts do not match the expected contract."""


class ProcessingError(PipelineError):
    """Raised when a pipeline stage cannot complete successfully."""


class OptionalDependencyError(ConfigurationError):
    """Raised when an optional dependency is required but not installed."""

