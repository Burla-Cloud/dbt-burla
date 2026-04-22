"""
Shared library code for every `dbt-burla-*` variant.

This package is not itself a dbt plugin - it is imported by the sibling
`dbt.adapters.burla_duckdb`, `dbt.adapters.burla_snowflake`, and
`dbt.adapters.burla_bigquery` packages, which register their own
:class:`AdapterPlugin` objects.
"""

from __future__ import annotations

from dbt.adapters.burla.__version__ import __version__

__all__ = ["__version__"]
