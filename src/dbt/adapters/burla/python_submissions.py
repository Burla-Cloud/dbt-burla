"""
BurlaPythonJobHelper - runs a dbt Python model on a Burla cluster.

End-to-end flow::

    compiled_code (from dbt) ──► extract embedded refs/sources map
                                 │
    ┌────────────────────────────┘
    │
    ▼                                (on the dbt-running machine)
    pull each upstream relation into a pandas DataFrame
    via the matching ``WarehouseBackend``
                                 │
                                 ▼
    ship ``compiled_code`` + upstream DataFrames to Burla
    via ``burla.remote_parallel_map(..., [None])``  (one-shot fan-in)
                                 │
                                 ▼                                (on the Burla worker)
    exec ``compiled_code``, build the dbt stub,
    call ``model(dbt, None)`` → pandas DataFrame
                                 │
                                 ▼                                (back on the dbt-running machine)
    write the returned DataFrame into the warehouse
    via ``WarehouseBackend.write_from_dataframe``

``burla_fake: true`` skips the trip to Burla entirely and runs the closure in
the local process, which keeps the quickstart + CI integration tests
self-contained and dependency-free.
"""

from __future__ import annotations

import ast
import contextlib
import logging
import os
import time
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from dbt.adapters.base import PythonJobHelper

from dbt.adapters.burla.config import BurlaConfig, BurlaModelConfig
from dbt.adapters.burla.credentials import build_burla_config
from dbt.adapters.burla.errors import (
    BurlaImportError,
    BurlaResultError,
    BurlaSubmissionError,
)
from dbt.adapters.burla.warehouses import (
    RelationRef,
    WarehouseBackend,
    get_backend_for_adapter,
)
from dbt.adapters.burla.warehouses.base import WriteMode

if TYPE_CHECKING:
    import pandas as pd
    from dbt.adapters.base.impl import BaseAdapter
    from dbt.adapters.contracts.connection import Credentials

__all__ = ["BurlaPythonJobHelper"]


logger = logging.getLogger("dbt.adapters.burla")


class BurlaPythonJobHelper(PythonJobHelper):
    """
    Submits a dbt Python model to a Burla cluster.

    dbt instantiates this with ``(parsed_model, credentials)`` but our adapter
    subclasses go through :meth:`for_adapter` instead so we can reach the
    already-open warehouse connection for DataFrame I/O.
    """

    def __init__(
        self,
        parsed_model: dict[str, Any],
        credentials: Credentials,
        *,
        adapter: BaseAdapter | None = None,
    ) -> None:
        self.parsed_model = parsed_model
        self.credentials = credentials
        self.adapter = adapter
        self.burla_config: BurlaConfig = build_burla_config(credentials)
        self.model_config: BurlaModelConfig = BurlaModelConfig.resolve(
            self.burla_config, parsed_model
        )
        self.target: RelationRef = RelationRef.from_parsed_model(parsed_model)

    @classmethod
    def for_adapter(
        cls, adapter: BaseAdapter, parsed_model: dict[str, Any]
    ) -> BurlaPythonJobHelper:
        return cls(parsed_model, adapter.config.credentials, adapter=adapter)

    def submit(self, compiled_code: str) -> Any:
        # dbt pads the compiled code with leading whitespace that Python's
        # parser treats as an indentation error - strip it before we either
        # exec locally or ship it to Burla. This mirrors what dbt-duckdb does.
        compiled_code = compiled_code.lstrip()

        backend = self._make_backend()
        refs_map, sources_map = self._extract_relation_maps(compiled_code)

        start = time.perf_counter()
        upstream_data = self._load_relations(backend, refs_map, sources_map)
        load_elapsed = time.perf_counter() - start

        logger.debug(
            "dbt-burla model=%s loaded %d upstream relations in %.2fs",
            self.target.identifier,
            len(upstream_data),
            load_elapsed,
        )

        is_incremental = self._resolve_is_incremental(backend)

        start = time.perf_counter()
        df = self._run(
            compiled_code=compiled_code,
            upstream_data=upstream_data,
            target=self.target,
            is_incremental=is_incremental,
        )
        run_elapsed = time.perf_counter() - start

        self._validate(df)
        self._materialize(backend, df)
        logger.info(
            "dbt-burla model=%s rows=%d runtime=%.2fs",
            self.target.identifier,
            len(df),
            run_elapsed,
        )
        return {"rows": len(df), "runtime_s": round(run_elapsed, 3)}

    def _make_backend(self) -> WarehouseBackend:
        if self.adapter is None:
            raise BurlaSubmissionError(
                "BurlaPythonJobHelper was constructed without an adapter; use "
                "BurlaPythonJobHelper.for_adapter(...) from inside an adapter's "
                "submit_python_job override."
            )
        backend_cls = get_backend_for_adapter(self.adapter.type())
        conn = self.adapter.connections.get_thread_connection()
        return backend_cls.from_connection_handle(conn.handle, self.credentials)

    @staticmethod
    def _extract_relation_maps(compiled_code: str) -> tuple[dict[str, str], dict[str, str]]:
        """
        Statically extract ``refs`` and ``sources`` dictionaries that dbt bakes
        into the compiled code.

        The compiled code defines two functions, ``ref(...)`` and
        ``source(...)``, each of which opens with a literal dict assignment:

            def ref(*args, **kwargs):
                refs = {"stg_orders": "\"db\".\"schema\".\"stg_orders\"", ...}
                ...

        We parse the code with AST, walk the two function bodies, and return
        those literal dicts.
        """
        tree = ast.parse(compiled_code.lstrip())
        refs: dict[str, str] = {}
        sources: dict[str, str] = {}
        for node in ast.walk(tree):
            if not isinstance(node, ast.FunctionDef):
                continue
            if node.name not in ("ref", "source"):
                continue
            target_name = "refs" if node.name == "ref" else "sources"
            for stmt in node.body:
                if (
                    isinstance(stmt, ast.Assign)
                    and len(stmt.targets) == 1
                    and isinstance(stmt.targets[0], ast.Name)
                    and stmt.targets[0].id == target_name
                    and isinstance(stmt.value, ast.Dict)
                ):
                    try:
                        value = ast.literal_eval(stmt.value)
                    except ValueError:
                        continue
                    if isinstance(value, dict):
                        if node.name == "ref":
                            refs = {str(k): str(v) for k, v in value.items()}
                        else:
                            sources = {str(k): str(v) for k, v in value.items()}
                    break
        return refs, sources

    def _load_relations(
        self,
        backend: WarehouseBackend,
        refs: dict[str, str],
        sources: dict[str, str],
    ) -> dict[str, pd.DataFrame]:
        """
        Fetch every upstream relation into a DataFrame, keyed by the exact
        quoted string dbt passes to ``dbt_load_df_function``.
        """
        unique_relations = {*refs.values(), *sources.values()}
        loaded: dict[str, pd.DataFrame] = {}
        for relation_str in unique_relations:
            ref = _parse_relation_string(relation_str)
            loaded[relation_str] = backend.read_as_dataframe(ref)
        return loaded

    def _resolve_is_incremental(self, backend: WarehouseBackend) -> bool:
        del backend
        materialized = (self.parsed_model.get("config") or {}).get("materialized", "table")
        return materialized == "incremental"

    def _run(
        self,
        *,
        compiled_code: str,
        upstream_data: dict[str, pd.DataFrame],
        target: RelationRef,
        is_incremental: bool,
    ) -> pd.DataFrame:
        if self.model_config.fake:
            with _fake_remote_parallel_map():
                return _execute_user_model(
                    compiled_code=compiled_code,
                    upstream_data=upstream_data,
                    target=target,
                    is_incremental=is_incremental,
                )
        return self._run_on_burla(
            compiled_code=compiled_code,
            upstream_data=upstream_data,
            target=target,
            is_incremental=is_incremental,
        )

    def _run_on_burla(
        self,
        *,
        compiled_code: str,
        upstream_data: dict[str, pd.DataFrame],
        target: RelationRef,
        is_incremental: bool,
    ) -> pd.DataFrame:
        try:
            from burla import remote_parallel_map
        except ImportError as exc:  # pragma: no cover - burla is a hard dep
            raise BurlaImportError(
                "The `burla` client isn't installed. Install with `pip install burla` "
                "or run with `burla_fake: true` to execute models in-process."
            ) from exc

        if self.burla_config.cluster_url:
            os.environ.setdefault("BURLA_CLUSTER_DASHBOARD_URL", self.burla_config.cluster_url)

        kwargs: dict[str, Any] = {}
        if self.model_config.cpus_per_worker:
            kwargs["func_cpu"] = self.model_config.cpus_per_worker
        if self.model_config.ram_per_worker:
            kwargs["func_ram"] = self.model_config.ram_per_worker
        if self.model_config.image:
            kwargs["image"] = self.model_config.image

        def _worker(_ignored: None) -> pd.DataFrame:
            return _execute_user_model(
                compiled_code=compiled_code,
                upstream_data=upstream_data,
                target=target,
                is_incremental=is_incremental,
            )

        try:
            results = list(remote_parallel_map(_worker, [None], **kwargs))
        except Exception as exc:
            raise BurlaSubmissionError(
                f"Model `{target.identifier}` failed on the Burla cluster: {exc}"
            ) from exc

        if not results:  # pragma: no cover - defensive
            raise BurlaSubmissionError(
                f"Model `{target.identifier}` returned no result from Burla."
            )
        return results[0]

    def _validate(self, df: Any) -> None:
        try:
            import pandas as pd
        except ImportError as exc:  # pragma: no cover
            raise BurlaImportError(
                "`pandas` is required to materialize dbt-burla Python models."
            ) from exc

        if not isinstance(df, pd.DataFrame):
            raise BurlaResultError(
                f"Python model `{self.target.identifier}` must return a pandas DataFrame; "
                f"got {type(df).__name__}"
            )

    def _materialize(self, backend: WarehouseBackend, df: pd.DataFrame) -> None:
        materialized = (self.parsed_model.get("config") or {}).get("materialized", "table")
        mode: WriteMode = "append" if materialized == "incremental" else "replace"
        backend.write_from_dataframe(df, self.target, mode=mode)


@contextlib.contextmanager
def _fake_remote_parallel_map() -> Iterator[None]:
    """
    Replace ``burla.remote_parallel_map`` with a plain in-process ``map``
    so models that call it inside ``model()`` work without a cluster.
    """
    try:
        import burla
    except ImportError:  # pragma: no cover - burla is a hard dep
        yield
        return

    original = getattr(burla, "remote_parallel_map", None)

    def _fake(func: Any, inputs: Any, **_ignored: Any) -> list[Any]:
        return [func(x) for x in inputs]

    burla.remote_parallel_map = _fake
    try:
        yield
    finally:
        if original is not None:
            burla.remote_parallel_map = original


def _parse_relation_string(relation_str: str) -> RelationRef:
    """Parse `"db"."schema"."name"` (or unquoted `db.schema.name`) to RelationRef."""
    parts = [p.strip().strip('"').strip("`") for p in relation_str.split(".")]
    parts = [p for p in parts if p]
    if len(parts) == 3:
        return RelationRef(database=parts[0], schema=parts[1], identifier=parts[2])
    if len(parts) == 2:
        return RelationRef(database=None, schema=parts[0], identifier=parts[1])
    if len(parts) == 1:
        return RelationRef(database=None, schema=None, identifier=parts[0])
    raise ValueError(f"Unexpected relation identifier {relation_str!r}")


def _execute_user_model(
    *,
    compiled_code: str,
    upstream_data: dict[str, pd.DataFrame],
    target: RelationRef,
    is_incremental: bool,
) -> pd.DataFrame:
    """
    Execute the dbt-compiled Python model in an isolated namespace.

    This runs in the same process when ``burla_fake=True``, and in the Burla
    worker process otherwise. It must be importable from the module path so
    cloudpickle can serialize references to it reliably.
    """
    namespace: dict[str, Any] = {"__name__": "__burla_model__"}
    try:
        exec(compiled_code, namespace)
    except Exception as exc:
        raise BurlaSubmissionError(f"Model `{target.identifier}` failed to compile: {exc}") from exc

    dbt_obj_cls = namespace.get("dbtObj")
    model_fn = namespace.get("model")
    if dbt_obj_cls is None or not callable(model_fn):
        raise BurlaSubmissionError(
            f"Compiled code for model `{target.identifier}` is missing required "
            "`dbtObj` or `model` definitions."
        )

    def _load_df(relation_str: str) -> pd.DataFrame:
        try:
            return upstream_data[relation_str]
        except KeyError as exc:
            raise BurlaSubmissionError(
                f"Model `{target.identifier}` referenced upstream relation "
                f"{relation_str!r}, which was not pre-loaded. This usually means "
                "dbt did not include it in the compiled `refs`/`sources` dicts."
            ) from exc

    dbt_obj = dbt_obj_cls(_load_df)
    dbt_obj.is_incremental = is_incremental
    try:
        return model_fn(dbt_obj, None)
    except Exception as exc:
        raise BurlaSubmissionError(
            f"Model `{target.identifier}` raised during execution: {exc}"
        ) from exc
