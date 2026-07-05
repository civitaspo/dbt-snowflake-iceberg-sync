"""Snowflake procedure entrypoint."""

from __future__ import annotations

import random
import re
import time
from collections.abc import Callable
from dataclasses import asdict
from typing import Any

from .config import IcebergSyncConfig, RetryPolicyConfig, parse_config
from .errors import IcebergSyncError, SnowflakeExecutionError
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
    action = payload.get("action", "run") if isinstance(payload, dict) else "run"
    if action == "start_export":
        return runner.start_export(payload)
    if action == "poll_export":
        return runner.poll_export(payload)
    if action != "run":
        raise IcebergSyncError(f"unknown procedure action: {action}")
    return runner.run(payload)


class IcebergSyncRunner:
    def __init__(
        self,
        session: Any,
        *,
        snowflake_client: SnowflakeClient | None = None,
        source_adapters: dict[str, SourceAdapter] | None = None,
        sleep_func: Callable[[float], None] = time.sleep,
        jitter_func: Callable[[float, float], float] = random.uniform,
    ):
        self.session = session
        self.snowflake = snowflake_client or SnowflakeClient(session)
        self.source_adapters = source_adapters or {}
        self.sleep = sleep_func
        self.jitter = jitter_func

    def run(self, payload: dict[str, Any]) -> dict[str, Any]:
        config = parse_config(payload)
        run_id = new_run_id()
        started_at = utcnow()
        effective_mode = "unknown"
        predicates: tuple[str, ...] = ()
        export_result: SourceExportResult | None = None
        error_message: str | None = None
        retry = _initial_retry_payload(config.retry)
        cleanup = _initial_cleanup_payload()
        internal_table_existed_before = False
        target_view_existed_before = False
        load_committed = False
        view_created = False

        try:
            internal_table_existed_before = self.snowflake.table_exists(config.internal_relation)
            target_view_existed_before = self.snowflake.relation_exists(
                config.target_relation,
                expected_type="VIEW",
            )
            effective_mode = effective_mode_for(
                config,
                internal_table_existed_before,
                target_view_existed_before,
            )
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
            if export_result.skipped:
                result = _result_payload(
                    config=config,
                    run_id=run_id,
                    effective_mode=effective_mode,
                    columns=[],
                    export_result=export_result,
                    snowflake_query_ids=self.snowflake.query_ids,
                    retry=retry,
                    cleanup=cleanup,
                    status="skipped",
                    error_message=export_result.skip_reason,
                )
                run_log_error = self._write_log(
                    config,
                    run_id,
                    effective_mode,
                    predicates,
                    export_result,
                    retry,
                    cleanup,
                    "skipped",
                    export_result.skip_reason,
                    started_at,
                )
                if run_log_error is not None:
                    result["run_log_error"] = run_log_error
                return result
            desired_columns = source.map_schema(export_result)

            created_internal_table, altered_schema = self._create_or_validate_table(
                config,
                internal_table_existed_before,
                desired_columns,
            )
            cleanup["created_internal_table"] = created_internal_table
            cleanup["altered_internal_table_schema"] = altered_schema
            retry = self._load_with_retry(
                config,
                effective_mode,
                stage.run_stage_location,
                retry,
            )
            load_committed = True
            view_column_payload = view_columns(desired_columns)
            self.snowflake.create_or_replace_view(
                config.target_relation,
                config.internal_relation,
                view_column_payload,
            )
            view_created = True

            result = _result_payload(
                config=config,
                run_id=run_id,
                effective_mode=effective_mode,
                columns=desired_columns,
                export_result=export_result,
                snowflake_query_ids=self.snowflake.query_ids,
                retry=retry,
                cleanup=cleanup,
                status="success",
                error_message=None,
            )
            run_log_error = self._write_log(
                config,
                run_id,
                effective_mode,
                predicates,
                export_result,
                retry,
                cleanup,
                "success",
                None,
                started_at,
            )
            if run_log_error is not None:
                result["run_log_error"] = run_log_error
            return result
        except Exception as exc:
            error_message = _sanitize_error_message(exc)
            self._cleanup_created_table_on_failure(
                config=config,
                cleanup=cleanup,
                internal_table_existed_before=internal_table_existed_before,
                target_view_existed_before=target_view_existed_before,
                load_committed=load_committed,
                view_created=view_created,
            )
            try:
                self._write_log(
                    config,
                    run_id,
                    effective_mode,
                    predicates,
                    export_result,
                    retry,
                    cleanup,
                    "failure",
                    error_message,
                    started_at,
                )
            except Exception as log_exc:
                retry["run_log_error_message"] = _sanitize_error_message(log_exc)
            if isinstance(exc, IcebergSyncError):
                raise
            raise

    def start_export(self, payload: dict[str, Any]) -> dict[str, Any]:
        config = parse_config(payload.get("config", {}))
        effective_mode = str(payload.get("effective_mode") or "")
        destination_uri = str(payload.get("destination_uri") or "")
        if effective_mode not in {"full_refresh", "incremental"}:
            raise IcebergSyncError("effective_mode must be full_refresh or incremental")
        if not destination_uri.startswith("gs://"):
            raise IcebergSyncError("destination_uri must be a gs:// URI")

        source = self._source_adapter(config)
        state = source.start_export(
            config,
            SourceExecutionContext(
                effective_mode=effective_mode,
                destination_uri=destination_uri,
            ),
        )
        return self._export_action_result(source, state)

    def poll_export(self, payload: dict[str, Any]) -> dict[str, Any]:
        config = parse_config(payload.get("config", {}))
        state = payload.get("export_state") or {}
        if not isinstance(state, dict):
            raise IcebergSyncError("export_state must be an object")
        source = self._source_adapter(config)
        next_state = source.poll_export(config, state)
        return self._export_action_result(source, next_state)

    def _export_action_result(
        self,
        source: SourceAdapter,
        state: dict[str, Any],
    ) -> dict[str, Any]:
        if state.get("status") != "success":
            if state.get("status") == "skipped":
                skip_reason = state.get("skip_reason")
                return {
                    "status": "skipped",
                    "skip_reason": skip_reason,
                    "export_result": {
                        "schema_fields": [],
                        "segments": [],
                        "job_references": [],
                        "staging_table_reference": None,
                        "columns": [],
                        "view_columns": [],
                        "skipped": True,
                        "skip_reason": skip_reason,
                    },
                }
            return {
                "status": "running",
                "export_state": state,
            }
        export_result = SourceExportResult(
            schema_fields=state.get("schema_fields", []),
            segments=state.get("segments", []),
            job_references=state.get("job_references", []),
            staging_table_reference=state.get("staging_table_reference"),
        )
        columns = source.map_schema(export_result)
        return {
            "status": "success",
            "export_result": {
                "schema_fields": export_result.schema_fields,
                "segments": export_result.segments,
                "job_references": export_result.job_references,
                "staging_table_reference": export_result.staging_table_reference,
                "columns": [asdict(column) | {"ddl": column.ddl} for column in columns],
                "view_columns": [asdict(column) for column in view_columns(columns)],
            },
        }

    def _source_adapter(self, config: IcebergSyncConfig) -> SourceAdapter:
        adapter = self.source_adapters.get(config.source_type)
        if adapter is not None:
            return adapter
        return create_source_adapter(config, session=self.session)

    def _create_or_validate_table(
        self,
        config: IcebergSyncConfig,
        table_exists: bool,
        desired_columns: list[SnowflakeColumn],
    ) -> tuple[bool, bool]:
        self.snowflake.create_iceberg_table(config, desired_columns)
        if not table_exists:
            return True, False
        existing_columns = self.snowflake.describe_table(config.internal_relation)
        validate_schema_compatibility(existing_columns, desired_columns)
        if len(desired_columns) > len(existing_columns):
            self.snowflake.add_columns(
                config.internal_relation,
                desired_columns[len(existing_columns) :],
            )
            return False, True
        return False, False

    def _load_with_retry(
        self,
        config: IcebergSyncConfig,
        effective_mode: str,
        stage_run_location: str,
        retry: dict[str, Any],
    ) -> dict[str, Any]:
        for attempt in range(1, config.retry.max_attempts + 1):
            retry["attempts"] = attempt
            try:
                self._load_once(config, effective_mode, stage_run_location)
                return retry
            except Exception as exc:
                retryable = is_retryable_snowflake_error(exc)
                delay_seconds = (
                    compute_retry_delay(config.retry, attempt, self.jitter)
                    if retryable and attempt < config.retry.max_attempts
                    else 0.0
                )
                if retryable:
                    retry["retryable_errors"].append(
                        {
                            "attempt": attempt,
                            "phase": "load_transaction",
                            "error_message": _sanitize_error_message(exc),
                            "rolled_back": getattr(exc, "_iceberg_sync_rolled_back", False),
                            "rollback_error_message": getattr(
                                exc, "_iceberg_sync_rollback_error_message", None
                            ),
                            "delay_seconds": delay_seconds,
                        }
                    )
                if not retryable or attempt >= config.retry.max_attempts:
                    raise
                self.sleep(delay_seconds)
        return retry

    def _load_once(
        self,
        config: IcebergSyncConfig,
        effective_mode: str,
        stage_run_location: str,
    ) -> None:
        transaction_started = False
        transaction_committed = False
        try:
            self.snowflake.begin()
            transaction_started = True
            predicate = None if effective_mode == "full_refresh" else config.incremental_predicate
            self.snowflake.delete_from_iceberg(config.internal_relation, predicate)
            self.snowflake.copy_into_iceberg(config.internal_relation, stage_run_location)
            self.snowflake.commit()
            transaction_committed = True
        except Exception as exc:
            if transaction_started and not transaction_committed:
                try:
                    self.snowflake.rollback()
                    exc._iceberg_sync_rolled_back = True
                except Exception as rollback_exc:
                    rollback_error = _sanitize_error_message(rollback_exc)
                    exc._iceberg_sync_rolled_back = False
                    exc._iceberg_sync_rollback_error_message = rollback_error
                    if hasattr(exc, "add_note"):
                        exc.add_note(f"Rollback failed: {rollback_error}")
            raise

    def _cleanup_created_table_on_failure(
        self,
        *,
        config: IcebergSyncConfig,
        cleanup: dict[str, Any],
        internal_table_existed_before: bool,
        target_view_existed_before: bool,
        load_committed: bool,
        view_created: bool,
    ) -> None:
        should_drop_created_table = (
            config.cleanup.created_table_on_failure
            and not internal_table_existed_before
            and cleanup["created_internal_table"]
            and not target_view_existed_before
            and (not load_committed or not view_created)
        )
        if not should_drop_created_table:
            return
        try:
            self.snowflake.drop_iceberg_table(config.internal_relation)
            cleanup["dropped_created_internal_table"] = True
        except Exception as cleanup_exc:
            cleanup["cleanup_error_message"] = _sanitize_error_message(cleanup_exc)

    def _write_log(
        self,
        config: IcebergSyncConfig,
        run_id: str,
        effective_mode: str,
        predicates: tuple[str, ...],
        export_result: SourceExportResult | None,
        retry: dict[str, Any],
        cleanup: dict[str, Any],
        status: str,
        error_message: str | None,
        started_at: Any,
    ) -> str | None:
        if config.deployment.run_log_table is None:
            return None
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
            retry=retry,
            cleanup=cleanup,
            status=status,
            error_message=error_message,
            started_at=started_at,
            finished_at=finished_at,
        )
        for attempt in range(1, config.retry.max_attempts + 1):
            try:
                self.snowflake.write_run_log(config.deployment.run_log_table, payload)
                return None
            except Exception as exc:
                error_message = _sanitize_error_message(exc)
                retry.setdefault("run_log_errors", []).append(
                    {
                        "attempt": attempt,
                        "phase": "run_log",
                        "error_message": error_message,
                        "delay_seconds": (
                            compute_retry_delay(config.retry, attempt, self.jitter)
                            if is_retryable_run_log_error(exc)
                            and attempt < config.retry.max_attempts
                            else 0.0
                        ),
                    }
                )
                retryable = is_retryable_run_log_error(exc)
                if retryable and attempt < config.retry.max_attempts:
                    self.sleep(retry["run_log_errors"][-1]["delay_seconds"])
                    continue
                if config.run_log.fail_on_error:
                    raise
                return error_message
        return None


def effective_mode_for(
    config: IcebergSyncConfig,
    internal_table_exists: bool,
    target_view_exists: bool,
) -> str:
    if config.dbt_full_refresh:
        return "full_refresh"
    if config.materialization_strategy == "full_refresh":
        return "full_refresh"
    if not internal_table_exists:
        return "full_refresh"
    if not target_view_exists:
        return "full_refresh"
    return "incremental"


def is_retryable_snowflake_error(exc: Exception) -> bool:
    if not isinstance(exc, SnowflakeExecutionError):
        return False
    lowered = str(exc).lower()
    return (
        "sql execution internal error" in lowered
        or "incident" in lowered
        or "scoped transaction started in stored procedure is incomplete" in lowered
    )


def is_retryable_run_log_error(exc: Exception) -> bool:
    if not isinstance(exc, SnowflakeExecutionError):
        return False
    lowered = str(exc).lower()
    return "000625" in lowered or "locked table" in lowered or "number of waiters" in lowered


def compute_retry_delay(
    policy: RetryPolicyConfig,
    failed_attempt: int,
    jitter_func: Callable[[float, float], float] = random.uniform,
) -> float:
    base_delay = policy.initial_delay_seconds * (
        policy.backoff_multiplier ** max(failed_attempt - 1, 0)
    )
    jitter = jitter_func(0.0, policy.jitter_seconds) if policy.jitter_seconds else 0.0
    return min(policy.max_delay_seconds, base_delay + jitter)


def _initial_retry_payload(policy: RetryPolicyConfig) -> dict[str, Any]:
    return {
        "max_attempts": policy.max_attempts,
        "attempts": 0,
        "retryable_errors": [],
        "run_log_errors": [],
    }


def _initial_cleanup_payload() -> dict[str, Any]:
    return {
        "created_internal_table": False,
        "altered_internal_table_schema": False,
        "dropped_created_internal_table": False,
        "cleanup_error_message": None,
    }


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
    retry: dict[str, Any],
    cleanup: dict[str, Any],
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
        "retry": retry,
        "cleanup": cleanup,
    }
