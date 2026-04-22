"""Unit tests for `dbt.adapters.burla.config`."""

from __future__ import annotations

import pytest

from dbt.adapters.burla.config import BurlaConfig, BurlaModelConfig

pytestmark = pytest.mark.unit


class TestBurlaConfig:
    def test_defaults(self) -> None:
        cfg = BurlaConfig()
        assert cfg.cluster_url is None
        assert cfg.default_workers == 16
        assert cfg.default_cpus_per_worker == 1
        assert cfg.default_ram_per_worker is None
        assert cfg.default_image is None
        assert cfg.default_timeout_s == 3600
        assert cfg.fake is False
        assert cfg.extra == {}

    def test_from_kwargs(self) -> None:
        cfg = BurlaConfig(
            cluster_url="https://cluster.example",
            default_workers=64,
            default_cpus_per_worker=4,
            default_ram_per_worker=16,
            default_image="us-docker.pkg.dev/x/y:latest",
            default_timeout_s=600,
            fake=True,
        )
        assert cfg.cluster_url == "https://cluster.example"
        assert cfg.default_workers == 64
        assert cfg.fake is True


class TestBurlaModelConfig:
    def test_resolve_falls_back_to_defaults(self) -> None:
        burla_config = BurlaConfig(
            default_workers=32,
            default_cpus_per_worker=2,
            default_ram_per_worker=8,
            default_image="default:latest",
            default_timeout_s=120,
            fake=True,
        )
        parsed_model: dict = {"config": {}}
        resolved = BurlaModelConfig.resolve(burla_config, parsed_model)
        assert resolved.workers == 32
        assert resolved.cpus_per_worker == 2
        assert resolved.ram_per_worker == 8
        assert resolved.image == "default:latest"
        assert resolved.timeout_s == 120
        assert resolved.fake is True

    def test_resolve_overrides_from_model(self) -> None:
        burla_config = BurlaConfig(default_workers=16)
        parsed_model = {
            "config": {
                "burla_workers": 128,
                "burla_cpus_per_worker": 8,
                "burla_ram_per_worker": 32,
                "burla_image": "override:tag",
                "burla_timeout_s": 999,
                "burla_fake": True,
            }
        }
        resolved = BurlaModelConfig.resolve(burla_config, parsed_model)
        assert resolved.workers == 128
        assert resolved.cpus_per_worker == 8
        assert resolved.ram_per_worker == 32
        assert resolved.image == "override:tag"
        assert resolved.timeout_s == 999
        assert resolved.fake is True

    def test_resolve_handles_missing_config(self) -> None:
        resolved = BurlaModelConfig.resolve(BurlaConfig(), {})
        assert resolved.workers == 16

    def test_resolve_handles_none_config(self) -> None:
        resolved = BurlaModelConfig.resolve(BurlaConfig(), {"config": None})
        assert resolved.workers == 16

    def test_resolve_coerces_types(self) -> None:
        parsed_model = {
            "config": {
                "burla_workers": "42",
                "burla_cpus_per_worker": "4",
                "burla_timeout_s": "60",
                "burla_fake": 1,
            }
        }
        resolved = BurlaModelConfig.resolve(BurlaConfig(), parsed_model)
        assert resolved.workers == 42
        assert resolved.cpus_per_worker == 4
        assert resolved.timeout_s == 60
        assert resolved.fake is True
