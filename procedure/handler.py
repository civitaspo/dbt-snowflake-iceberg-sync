"""Snowflake procedure entrypoint."""

from __future__ import annotations

import re
from contextlib import suppress
from dataclasses import asdict
from typing import Any

from .config import IcebergSyncConfig, parse_config
from .errors import IcebergSyncError
from .run_log import build_run_log_payload
from .schema import (
    SnowflakeColumn,
    validate_schema_compatibility,
    view_columns,
)
from .snowflake import SnowflakeClient
from .sources import create_source_adapter
from .sources.base import SourceAdapter, SourceExecutionContext, SourceExportResult
from .utils import new_run_id, parse_json_maybe, utcnow


def main(session: Any, config: Any) -> dict[str, Any]:
    """Procedure entrypoint called by Snowflake."""

    payload = parse_json_maybe(config)
    runner = IcebergSyncRunner(session)
    return runner.run(payload)


class IcebergSyncRunner:
    def __init__(
        self,
        session: Any,
        *,
        snowflake_client: SnowflakeClient | None = None,
        source_adapters: dict[str, SourceAdapter] | None = None,
    ):
        self.session = session
        self.snowflake = snowflake_client or SnowflakeClient(session)
        self.source_adapters = source_adapters or {}

    def run(self, payload: dict[str, Any]) -> dict[str, Any]:
        config = parse_config(payload)
        run_id = new_run_id()
        started_at = utcnow()
        effective_mode = "unknown"
        predicates: tuple[str, ...] = ()
        export_result: SourceExportResult | None = None
        error_message: str | None = None

        try:
            self.snowflake.ensure_run_log(config.deployment.run_log_table)
            table_exists = self.snowflake.table_exists(config.internal_relation)
            effective_mode = effective_mode_for(config, table_exists)
            predicates = config.predicates_for_mode(effective_mode)
            source = self._source_adapter(config)
            stage = self.snowflake.resolve_stage_location(
                source.export_location(config),
                run_id,
            )
            export_result = source.export(
                config,
                SourceExecutionContext(
                    effective_mode=effective_mode,
                    destination_uri=stage.gcs_run_uri,
                ),
            )
            desired_columns = source.map_schema(export_result)

            self._create_or_validate_table(config, table_exists, desired_columns)
            self._load(config, effective_mode, stage.run_stage_location)

            result = _result_payload(
                config=config,
                run_id=run_id,
                effective_mode=effective_mode,
                columns=desired_columns,
                export_result=export_result,
                snowflake_query_ids=self.snowflake.query_ids,
                status="success",
                error_message=None,
            )
            self._write_log(
                config,
                run_id,
                effective_mode,
                predicates,
                export_result,
                "success",
                None,
                started_at,
            )
            return result
        except Exception as exc:
            error_message = _sanitize_error_message(exc)
            with suppress(Exception):
                self.snowflake.rollback()
            self._write_log(
                config,
                run_id,
                effective_mode,
                predicates,
                export_result,
                "failure",
                error_message,
                started_at,
            )
            if isinstance(exc, IcebergSyncError):
                raise
            raise

    def _source_adapter(self, config: IcebergSyncConfig) -> SourceAdapter:
        adapter = self.source_adapters.get(config.source_type)
        if adapter is not None:
            return adapter
        return create_source_adapter(config)

    def _create_or_validate_table(
        self,
        config: IcebergSyncConfig,
        table_exists: bool,
        desired_columns: list[SnowflakeColumn],
    ) -> None:
        self.snowflake.create_iceberg_table(config, desired_columns)
        if not table_exists:
            return
        existing_columns = self.snowflake.describe_table(config.internal_relation)
        validate_schema_compatibility(existing_columns, desired_columns)
        if len(desired_columns) > len(existing_columns):
            self.snowflake.add_columns(
                config.internal_relation,
                desired_columns[len(existing_columns) :],
            )

    def _load(
        self,
        config: IcebergSyncConfig,
        effective_mode: str,
        stage_run_location: str,
    ) -> None:
        self.snowflake.begin()
        try:
            predicate = None if effective_mode == "full_refresh" else config.incremental_predicate
            self.snowflake.delete_from_iceberg(config.internal_relation, predicate)
            self.snowflake.copy_into_iceberg(config.internal_relation, stage_run_location)
            self.snowflake.commit()
        except Exception:
            self.snowflake.rollback()
            raise

    def _write_log(
        self,
        config: IcebergSyncConfig,
        run_id: str,
        effective_mode: str,
        predicates: tuple[str, ...],
        export_result: SourceExportResult | None,
        status: str,
        error_message: str | None,
        started_at: Any,
    ) -> None:
        if config.deployment.run_log_table is None:
            return
        finished_at = utcnow()
        payload = build_run_log_payload(
            config=config,
            run_id=run_id,
            effective_mode=effective_mode,
            predicates=predicates,
            export_segments=export_result.segments if export_result else [],
            source_job_references=export_result.job_references if export_result else [],
            staging_table_reference=(
                export_result.staging_table_reference if export_result else None
            ),
            snowflake_query_ids=self.snowflake.query_ids,
            status=status,
            error_message=error_message,
            started_at=started_at,
            finished_at=finished_at,
        )
        self.snowflake.write_run_log(config.deployment.run_log_table, payload)


def effective_mode_for(config: IcebergSyncConfig, table_exists: bool) -> str:
    if config.dbt_full_refresh:
        return "full_refresh"
    if config.materialization_strategy == "full_refresh":
        return "full_refresh"
    if not table_exists:
        return "full_refresh"
    return "incremental"


def _sanitize_error_message(exc: Exception) -> str:
    message = " ".join(str(exc).split())
    replacements = (
        (r"(?i)bearer\s+[A-Za-z0-9._~+/=-]+", "Bearer <redacted>"),
        (r"(?i)(gs|gcs|s3)://\S+", "<redacted-uri>"),
        (r"(?i)https?://\S+", "<redacted-uri>"),
        (r"(?i)@[A-Za-z0-9_\".]+/\S+", "@<redacted-stage-path>"),
        (
            r"(?i)(password|token|secret|credential|private[_-]?key)(\s*[:=]\s*)"
            r"('[^']*'|\"[^\"]*\"|\S+)",
            r"\1\2<redacted>",
        ),
    )
    for pattern, replacement in replacements:
        message = re.sub(pattern, replacement, message)
    if len(message) > 500:
        message = message[:497].rstrip() + "..."
    return f"{type(exc).__name__}: {message}" if message else type(exc).__name__


def _result_payload(
    *,
    config: IcebergSyncConfig,
    run_id: str,
    effective_mode: str,
    columns: list[SnowflakeColumn],
    export_result: SourceExportResult,
    snowflake_query_ids: list[str],
    status: str,
    error_message: str | None,
) -> dict[str, Any]:
    return {
        "status": status,
        "error_message": error_message,
        "run_id": run_id,
        "effective_mode": effective_mode,
        "target_relation": asdict(config.target_relation),
        "internal_relation": asdict(config.internal_relation),
        "view_columns": [asdict(column) for column in view_columns(columns)],
        "export_segments": export_result.segments,
        "source_job_references": export_result.job_references,
        "staging_table_reference": export_result.staging_table_reference,
        "snowflake_query_ids": snowflake_query_ids,
    }
