"""Run log payload helpers."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from .config import IcebergSyncConfig
from .sql import relation_sql


def build_run_log_payload(
    *,
    config: IcebergSyncConfig,
    run_id: str,
    effective_mode: str,
    predicates: tuple[str, ...],
    export_segments: list[dict[str, Any]] | None,
    source_job_references: list[dict[str, Any]] | None,
    staging_table_reference: str | None,
    snowflake_query_ids: list[str],
    retry: dict[str, Any],
    cleanup: dict[str, Any],
    status: str,
    error_message: str | None,
    started_at: datetime,
    finished_at: datetime,
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "invocation_id": config.model.invocation_id,
        "model_unique_id": config.model.unique_id,
        "target_view": relation_sql(config.target_relation),
        "internal_iceberg_table": relation_sql(config.internal_relation),
        "source_type": config.source_type,
        "effective_mode": effective_mode,
        "predicate_json": list(predicates),
        "export_segments": export_segments or [],
        "source_job_references": source_job_references or [],
        "staging_table_reference": staging_table_reference,
        "snowflake_query_ids": snowflake_query_ids,
        "retry": retry,
        "cleanup": cleanup,
        "status": status,
        "error_message": error_message,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
    }
