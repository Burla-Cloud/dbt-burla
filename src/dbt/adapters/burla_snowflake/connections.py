"""Connection manager retagged as `burla_snowflake`."""

from __future__ import annotations

from dbt.adapters.snowflake.connections import SnowflakeConnectionManager

__all__ = ["BurlaSnowflakeConnectionManager"]


class BurlaSnowflakeConnectionManager(SnowflakeConnectionManager):
    TYPE = "burla_snowflake"
