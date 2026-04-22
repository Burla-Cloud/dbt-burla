"""`BurlaDuckDBAdapter` - DuckDB adapter with Python models routed to Burla."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

from dbt.adapters.base.impl import AdapterResponse, AdapterTrackingRelationInfo
from dbt.adapters.duckdb import DuckDBAdapter

from dbt.adapters.burla.python_submissions import BurlaPythonJobHelper
from dbt.adapters.burla_duckdb.connections import BurlaDuckDBConnectionManager

if TYPE_CHECKING:
    from dbt.adapters.contracts.relation import RelationConfig

__all__ = ["BurlaDuckDBAdapter"]


class BurlaDuckDBAdapter(DuckDBAdapter):
    """
    DuckDB adapter whose Python models run on a Burla cluster.

    SQL models run against DuckDB exactly as they would with `dbt-duckdb`.
    Python models are routed through :class:`BurlaPythonJobHelper` unless the
    user sets ``submission_method`` to something other than ``"burla"`` (in
    which case we fall back to the default DuckDB behaviour, running the
    Python locally).
    """

    ConnectionManager = BurlaDuckDBConnectionManager

    @classmethod
    def type(cls) -> str:
        return "burla_duckdb"

    def submit_python_job(
        self, parsed_model: dict[str, Any], compiled_code: str
    ) -> AdapterResponse:
        submission_method = (parsed_model.get("config") or {}).get("submission_method", "burla")
        if submission_method != "burla":
            return super().submit_python_job(parsed_model, compiled_code)

        helper = BurlaPythonJobHelper.for_adapter(self, parsed_model)
        result = helper.submit(compiled_code)
        return self.generate_python_submission_response(result)

    def generate_python_submission_response(self, submission_result: Any) -> AdapterResponse:
        message = "OK" if submission_result is None else str(submission_result)
        return AdapterResponse(_message=message, rows_affected=0)

    @classmethod
    def get_adapter_run_info(cls, config: RelationConfig) -> AdapterTrackingRelationInfo:
        # AdapterTrackingRelationInfo is a dataclass in dbt-core that mypy
        # occasionally misidentifies as abstract across versions.
        return AdapterTrackingRelationInfo(  # type: ignore[abstract]
            adapter_name="burla_duckdb",
            base_adapter_version=import_module("dbt.adapters.__about__").version,
            adapter_version=import_module("dbt.adapters.burla_duckdb.__version__").version,
            model_adapter_details=cls._get_adapter_specific_run_info(config),
        )
