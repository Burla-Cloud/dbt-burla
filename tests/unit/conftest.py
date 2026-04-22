"""Shared fixtures and helpers for unit tests."""

from __future__ import annotations

from typing import Any

import pandas as pd
import pytest


@pytest.fixture
def sample_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"id": 1, "name": "alice"},
            {"id": 2, "name": "bob"},
            {"id": 3, "name": "carol"},
        ]
    )


@pytest.fixture
def compiled_code_single_ref() -> str:
    return """

    def model(dbt, session):
        dbt.config(materialized="table")
        df = dbt.ref("stg_orders")
        return df


# This part is user provided model code
# COMMAND ----------
# this part is dbt logic for get ref work, do not modify

def ref(*args, **kwargs):
    refs = {"stg_orders": "\\"db\\".\\"main\\".\\"stg_orders\\""}
    key = '.'.join(args)
    dbt_load_df_function = kwargs.get("dbt_load_df_function")
    return dbt_load_df_function(refs[key])


def source(*args, dbt_load_df_function):
    sources = {}
    key = '.'.join(args)
    return dbt_load_df_function(sources[key])


config_dict = {}
meta_dict = {}


class config:
    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def get(key, default=None):
        return config_dict.get(key, default)

    @staticmethod
    def meta_get(key, default=None):
        return meta_dict.get(key, default)


class this:
    database = "db"
    schema = "main"
    identifier = "py_model"

    def __repr__(self):
        return '"db"."main"."py_model"'


class dbtObj:
    def __init__(self, load_df_function) -> None:
        self.source = lambda *args: source(*args, dbt_load_df_function=load_df_function)
        self.ref = lambda *args, **kwargs: ref(*args, **kwargs, dbt_load_df_function=load_df_function)
        self.config = config
        self.this = this()
        self.is_incremental = False
"""


@pytest.fixture
def compiled_code_with_source() -> str:
    return """

    def model(dbt, session):
        return dbt.source("raw", "orders")


def ref(*args, **kwargs):
    refs = {}
    key = '.'.join(args)
    dbt_load_df_function = kwargs.get("dbt_load_df_function")
    return dbt_load_df_function(refs[key])


def source(*args, dbt_load_df_function):
    sources = {"raw.orders": "\\"db\\".\\"raw\\".\\"orders\\""}
    key = '.'.join(args)
    return dbt_load_df_function(sources[key])


class dbtObj:
    def __init__(self, load_df_function) -> None:
        self.source = lambda *args: source(*args, dbt_load_df_function=load_df_function)
        self.ref = lambda *args, **kwargs: ref(*args, **kwargs, dbt_load_df_function=load_df_function)
"""


@pytest.fixture
def parsed_model_table() -> dict[str, Any]:
    return {
        "name": "py_model",
        "alias": "py_model",
        "schema": "main",
        "database": "db",
        "unique_id": "model.test.py_model",
        "config": {"materialized": "table"},
        "refs": [{"name": "stg_orders"}],
        "sources": [],
    }


@pytest.fixture
def parsed_model_incremental() -> dict[str, Any]:
    return {
        "name": "py_model",
        "alias": "py_model",
        "schema": "main",
        "database": "db",
        "unique_id": "model.test.py_model",
        "config": {
            "materialized": "incremental",
            "burla_workers": 100,
            "burla_cpus_per_worker": 8,
        },
        "refs": [{"name": "stg_orders"}],
        "sources": [],
    }
