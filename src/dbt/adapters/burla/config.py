"""Burla-specific configuration shared by all dbt-burla adapter variants."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class BurlaConfig:
    """
    Burla-specific fields merged into the warehouse `Credentials`.

    `cluster_url` is optional. When omitted, the client falls back to
    whatever `burla login` stored on disk.

    `default_*` values are applied to every Python model unless overridden by
    `dbt.config(burla_workers=..., burla_cpus_per_worker=..., burla_ram_per_worker=...)`
    inside the model itself.
    """

    cluster_url: str | None = None
    default_workers: int = 16
    default_cpus_per_worker: int = 1
    default_ram_per_worker: int | None = None
    default_image: str | None = None
    default_timeout_s: int = 3600
    fake: bool = False
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BurlaModelConfig:
    """Per-model Burla config, resolved by merging defaults + `dbt.config(...)`."""

    workers: int
    cpus_per_worker: int
    ram_per_worker: int | None
    image: str | None
    timeout_s: int
    fake: bool

    @classmethod
    def resolve(cls, defaults: BurlaConfig, parsed_model: dict[str, Any]) -> BurlaModelConfig:
        model_config = parsed_model.get("config", {}) or {}
        return cls(
            workers=int(model_config.get("burla_workers", defaults.default_workers)),
            cpus_per_worker=int(
                model_config.get("burla_cpus_per_worker", defaults.default_cpus_per_worker)
            ),
            ram_per_worker=model_config.get(
                "burla_ram_per_worker", defaults.default_ram_per_worker
            ),
            image=model_config.get("burla_image", defaults.default_image),
            timeout_s=int(model_config.get("burla_timeout_s", defaults.default_timeout_s)),
            fake=bool(model_config.get("burla_fake", defaults.fake)),
        )


BURLA_CREDENTIAL_FIELDS: tuple[str, ...] = (
    "burla_cluster_url",
    "burla_default_workers",
    "burla_default_cpus_per_worker",
    "burla_default_ram_per_worker",
    "burla_default_image",
    "burla_default_timeout_s",
    "burla_fake",
)
