"""`BurlaBigQueryAdapter` - BigQuery adapter with Python models routed to Burla."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

from dbt.adapters.base.impl import AdapterResponse, AdapterTrackingRelationInfo
from dbt.adapters.bigquery import BigQueryAdapter

from dbt.adapters.burla.python_submissions import BurlaPythonJobHelper
from dbt.adapters.burla_bigquery.connections import BurlaBigQueryConnectionManager

if TYPE_CHECKING:
    from dbt.adapters.contracts.relation import RelationConfig

__all__ = ["BurlaBigQueryAdapter"]


class BurlaBigQueryAdapter(BigQueryAdapter):
    """
    BigQuery adapter whose Python models run on a Burla cluster.

    Replaces the default Dataproc submission. Upstream tables are read via the
    BigQuery Storage API and results are written back with
    ``load_table_from_dataframe`` (Parquet upload).
    """

    ConnectionManager = BurlaBigQueryConnectionManager

    @classmethod
    def type(cls) -> str:
        return "burla_bigquery"

    def submit_python_job(
        self, parsed_model: dict[str, Any], compiled_code: str
    ) -> AdapterResponse:
        submission_method = (parsed_model.get("config") or {}).get("submission_method", "burla")
        if submission_method != "burla":
            return super().submit_python_job(parsed_model, compiled_code)

        helper = BurlaPythonJobHelper.for_adapter(self, parsed_model)
        result = helper.submit(compiled_code)
        return self.generate_python_submission_response(result)

    def generate_python_submission_response(self, submission_result: Any) -> AdapterResponse:  # type: ignore[override]
        message = "OK" if submission_result is None else str(submission_result)
        return AdapterResponse(_message=message, rows_affected=0)

    @classmethod
    def get_adapter_run_info(cls, config: RelationConfig) -> AdapterTrackingRelationInfo:
        return AdapterTrackingRelationInfo(  # type: ignore[abstract]
            adapter_name="burla_bigquery",
            base_adapter_version=import_module("dbt.adapters.__about__").version,
            adapter_version=import_module("dbt.adapters.burla_bigquery.__version__").version,
            model_adapter_details=cls._get_adapter_specific_run_info(config),
        )
