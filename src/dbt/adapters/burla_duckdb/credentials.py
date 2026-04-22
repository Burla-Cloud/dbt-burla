"""`BurlaDuckDBCredentials` - DuckDB profile config plus `burla_*` knobs."""

from __future__ import annotations

from dataclasses import dataclass

from dbt.adapters.duckdb.credentials import DuckDBCredentials

__all__ = ["BurlaDuckDBCredentials"]


@dataclass
class BurlaDuckDBCredentials(DuckDBCredentials):
    burla_cluster_url: str | None = None
    burla_default_workers: int = 16
    burla_default_cpus_per_worker: int = 1
    burla_default_ram_per_worker: int | None = None
    burla_default_image: str | None = None
    burla_default_timeout_s: int = 3600
    burla_fake: bool = False

    @property
    def type(self) -> str:
        return "burla_duckdb"

    @property
    def unique_field(self) -> str:
        return f"burla_duckdb/{self.path}"

    def _connection_keys(self) -> tuple[str, ...]:
        parent_keys = super()._connection_keys()
        return (
            *parent_keys,
            "burla_cluster_url",
            "burla_default_workers",
            "burla_default_cpus_per_worker",
            "burla_default_ram_per_worker",
            "burla_default_image",
            "burla_default_timeout_s",
            "burla_fake",
        )
