"""Source adapter interfaces."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from ..config import IcebergSyncConfig
from ..schema import SnowflakeColumn


@dataclass(frozen=True)
class SourceExecutionContext:
    effective_mode: str
    destination_uri: str


@dataclass(frozen=True)
class SourceExportResult:
    schema_fields: list[dict[str, Any]]
    segments: list[dict[str, Any]]
    job_references: list[dict[str, Any]]
    staging_table_reference: str | None = None
    skipped: bool = False
    skip_reason: str | None = None


class SourceAdapter(Protocol):
    source_type: str

    def export_location(self, config: IcebergSyncConfig) -> str:
        ...

    def export(
        self,
        config: IcebergSyncConfig,
        context: SourceExecutionContext,
    ) -> SourceExportResult:
        ...

    def map_schema(self, export_result: SourceExportResult) -> list[SnowflakeColumn]:
        ...

    def start_export(
        self,
        config: IcebergSyncConfig,
        context: SourceExecutionContext,
    ) -> dict[str, Any]:
        ...

    def poll_export(
        self,
        config: IcebergSyncConfig,
        state: dict[str, Any],
    ) -> dict[str, Any]:
        ...
