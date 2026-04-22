"""Unit tests for `dbt.adapters.burla.credentials.build_burla_config`."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from dbt.adapters.burla.credentials import build_burla_config

pytestmark = pytest.mark.unit


@dataclass
class _StubCredentials:
    burla_cluster_url: str | None = None
    burla_default_workers: int = 16
    burla_default_cpus_per_worker: int = 1
    burla_default_ram_per_worker: int | None = None
    burla_default_image: str | None = None
    burla_default_timeout_s: int = 3600
    burla_fake: bool = False


def test_build_burla_config_extracts_all_fields() -> None:
    creds = _StubCredentials(
        burla_cluster_url="https://example.com",
        burla_default_workers=64,
        burla_default_cpus_per_worker=4,
        burla_default_ram_per_worker=16,
        burla_default_image="my-image:latest",
        burla_default_timeout_s=900,
        burla_fake=True,
    )
    cfg = build_burla_config(creds)
    assert cfg.cluster_url == "https://example.com"
    assert cfg.default_workers == 64
    assert cfg.default_cpus_per_worker == 4
    assert cfg.default_ram_per_worker == 16
    assert cfg.default_image == "my-image:latest"
    assert cfg.default_timeout_s == 900
    assert cfg.fake is True


def test_build_burla_config_defaults_when_fields_missing() -> None:
    class _Bare:
        pass

    cfg = build_burla_config(_Bare())
    assert cfg.cluster_url is None
    assert cfg.default_workers == 16
    assert cfg.fake is False
