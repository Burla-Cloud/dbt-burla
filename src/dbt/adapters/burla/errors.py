"""Typed errors raised by dbt-burla with actionable messages."""

from __future__ import annotations


class BurlaAdapterError(Exception):
    """Base class for dbt-burla errors."""


class BurlaConfigError(BurlaAdapterError):
    """Raised when profile or model config is invalid."""


class BurlaImportError(BurlaAdapterError):
    """Raised when an optional warehouse dependency isn't installed."""

    @classmethod
    def for_extra(cls, warehouse: str, missing: str) -> BurlaImportError:
        return cls(
            f"`{missing}` isn't installed, which is required for the `{warehouse}` warehouse.\n"
            f"Install it with:\n"
            f"    pip install 'dbt-burla[{warehouse}]'"
        )


class BurlaSubmissionError(BurlaAdapterError):
    """Raised when a model fails to run on the Burla cluster."""


class BurlaResultError(BurlaAdapterError):
    """Raised when a Python model returns something we can't materialize."""
