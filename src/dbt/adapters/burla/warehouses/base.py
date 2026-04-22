"""Abstract `WarehouseBackend` interface used for Python-model data transfer."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    import pandas as pd


WriteMode = Literal["replace", "append"]


@dataclass(frozen=True)
class RelationRef:
    """Fully-qualified pointer to a warehouse relation."""

    database: str | None
    schema: str | None
    identifier: str

    @classmethod
    def from_parsed_model(cls, parsed_model: dict[str, Any]) -> RelationRef:
        return cls(
            database=parsed_model.get("database"),
            schema=parsed_model.get("schema"),
            identifier=parsed_model.get("alias") or parsed_model["name"],
        )

    def render(self, quote: str = '"') -> str:
        parts = [p for p in (self.database, self.schema, self.identifier) if p]
        return ".".join(f"{quote}{p}{quote}" for p in parts)

    def render_unquoted(self) -> str:
        return ".".join(p for p in (self.database, self.schema, self.identifier) if p)


class WarehouseBackend(ABC):
    """
    Move pandas DataFrames between the warehouse and the Burla worker.

    Implementations are constructed per Python-model submission via
    :meth:`from_connection_handle`, given the already-open connection owned by
    the dbt adapter. Reusing the adapter's connection avoids re-authenticating
    and inherits dbt's connection pooling.
    """

    @classmethod
    @abstractmethod
    def from_connection_handle(cls, handle: Any, credentials: Any) -> WarehouseBackend:
        """Construct from an already-open warehouse connection."""

    @abstractmethod
    def read_as_dataframe(self, relation: RelationRef) -> pd.DataFrame:
        """Pull an entire relation into a pandas DataFrame."""

    @abstractmethod
    def write_from_dataframe(
        self,
        df: pd.DataFrame,
        relation: RelationRef,
        *,
        mode: WriteMode = "replace",
    ) -> None:
        """Write a pandas DataFrame to the warehouse, creating or replacing the relation."""

    @abstractmethod
    def drop_if_exists(self, relation: RelationRef) -> None:
        """Drop the relation if it exists."""
