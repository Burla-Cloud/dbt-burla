"""Shared helper for extracting Burla config from any warehouse-specific credentials."""

from __future__ import annotations

from typing import TYPE_CHECKING

from dbt.adapters.burla.config import BurlaConfig

if TYPE_CHECKING:
    from dbt.adapters.contracts.connection import Credentials

__all__ = ["build_burla_config"]


def build_burla_config(credentials: Credentials) -> BurlaConfig:
    """Extract Burla-specific config from any BurlaXxxCredentials instance."""
    return BurlaConfig(
        cluster_url=getattr(credentials, "burla_cluster_url", None),
        default_workers=getattr(credentials, "burla_default_workers", 16),
        default_cpus_per_worker=getattr(credentials, "burla_default_cpus_per_worker", 1),
        default_ram_per_worker=getattr(credentials, "burla_default_ram_per_worker", None),
        default_image=getattr(credentials, "burla_default_image", None),
        default_timeout_s=getattr(credentials, "burla_default_timeout_s", 3600),
        fake=getattr(credentials, "burla_fake", False),
    )
