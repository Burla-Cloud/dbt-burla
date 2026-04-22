"""dbt plugin registration for the Snowflake-backed Burla adapter."""

from __future__ import annotations

from pathlib import Path

from dbt.adapters.base.plugin import AdapterPlugin

from dbt.adapters.burla.__version__ import __version__
from dbt.adapters.burla_snowflake.credentials import BurlaSnowflakeCredentials
from dbt.adapters.burla_snowflake.impl import BurlaSnowflakeAdapter

__all__ = ["Plugin", "__version__"]

Plugin = AdapterPlugin(
    adapter=BurlaSnowflakeAdapter,  # type: ignore[arg-type]
    credentials=BurlaSnowflakeCredentials,
    include_path=str(Path(__file__).resolve().parents[2] / "include" / "burla_snowflake"),
    dependencies=["snowflake"],
    project_name="dbt_burla_snowflake",
)
