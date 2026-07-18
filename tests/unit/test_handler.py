from __future__ import annotations

import pytest

from procedure.errors import ConfigError, SchemaError, SnowflakeExecutionError, SourceError
from procedure.handler import (
    IcebergSyncRunner,
    is_retryable_run_log_error,
    is_retryable_snowflake_error,
)
from procedure.schema import SnowflakeColumn
from procedure.sources.base import SourceExecutionContext, SourceExportResult


class FakeSnowflake:
    def __init__(
        self,
        *,
        table_exists=False,
        target_view_exists=False,
        fail_copy=False,
        fail_delete=False,
        fail_create_view=False,
        fail_drop=False,
        fail_rollback=False,
        copy_errors=None,
        run_log_errors=None,
        existing_columns=None,
    ):
        self.query_ids = []
        self.table_exists_value = table_exists
        self.target_view_exists_value = target_view_exists
        self.fail_copy = fail_copy
        self.fail_delete = fail_delete
        self.fail_create_view = fail_create_view
        self.fail_drop = fail_drop
        self.fail_rollback = fail_rollback
        self.copy_errors = list(copy_errors or [])
        self.run_log_errors = list(run_log_errors or [])
        self.existing_columns = existing_columns or [SnowflakeColumn("OrderID", "BIGINT")]
        self.calls = []
        self.run_logs = []

    def ensure_run_log(self, relation):
        self.calls.append(("ensure_run_log", relation))

    def table_exists(self, relation):
        self.calls.append(("table_exists", relation.identifier))
        return self.table_exists_value

    def relation_exists(self, relation, *, expected_type=None):
        self.calls.append(("relation_exists", relation.identifier, expected_type))
        if expected_type == "VIEW":
            return self.target_view_exists_value
        return self.table_exists_value

    def resolve_stage_location(
        self,
        export_location,
        run_id=None,
        *,
        allowed_schemes=None,
        field_name=None,
        cloud_label=None,
    ):
        self.calls.append(("resolve_stage_location", export_location, run_id))

        class Stage:
            run_stage_location = '@"ANALYTICS"."PUBLIC"."EXPORT_STAGE"/dbt/run'
            remote_run_uri = "gs://bucket/dbt/run"
            stage_url = "gcs://bucket/dbt"
            stage_fqn = '"ANALYTICS"."PUBLIC"."EXPORT_STAGE"'
            stage_path = "dbt"

        return Stage()

    def create_iceberg_table(self, config, columns):
        self.calls.append(("create_iceberg_table", [column.source_name for column in columns]))

    def drop_iceberg_table(self, relation):
        self.calls.append(("drop_iceberg_table", relation.identifier))
        if self.fail_drop:
            raise SnowflakeExecutionError("drop failed")

    def describe_table(self, relation):
        self.calls.append(("describe_table", relation.identifier))
        return self.existing_columns

    def add_columns(self, relation, columns):
        self.calls.append(("add_columns", [column.source_name for column in columns]))

    def begin(self):
        self.calls.append(("begin",))

    def delete_from_iceberg(self, relation, predicate):
        self.calls.append(("delete", predicate))
        if self.fail_delete:
            raise SnowflakeExecutionError("delete failed")

    def copy_into_iceberg(
        self,
        relation,
        stage_run_location,
        *,
        pattern=None,
        files=None,
        force=False,
        load_mode="add_files_copy",
        transform_columns=None,
    ):
        self.calls.append(
            (
                "copy",
                stage_run_location,
                pattern,
                files,
                force,
                load_mode,
                None
                if transform_columns is None
                else [column.source_name for column in transform_columns],
            )
        )
        if self.copy_errors:
            raise self.copy_errors.pop(0)
        if self.fail_copy:
            raise SnowflakeExecutionError("copy failed")

    def commit(self):
        self.calls.append(("commit",))

    def rollback(self):
        self.calls.append(("rollback",))
        if self.fail_rollback:
            raise SnowflakeExecutionError("rollback failed")

    def create_or_replace_view(self, target, internal, columns):
        self.calls.append(
            ("create_or_replace_view", target.identifier, [column.alias for column in columns])
        )
        if self.fail_create_view:
            raise SnowflakeExecutionError("view creation failed")

    def write_run_log(self, relation, payload):
        self.calls.append(("write_run_log", payload["status"]))
        if self.run_log_errors:
            raise self.run_log_errors.pop(0)
        self.run_logs.append(payload)


class FakeSource:
    source_type = "bigquery"

    def __init__(self, *, fail_export=False, skip_export=False, columns=None):
        self.calls = []
        self.fail_export = fail_export
        self.skip_export = skip_export
        self.columns = columns or [SnowflakeColumn("OrderID", "BIGINT")]

    def export_location(self, config):
        self.calls.append(("export_location", config.source_type))
        return config.bigquery.export_location

    def export(self, config, context: SourceExecutionContext):
        self.calls.append((context.effective_mode, context.destination_uri))
        if self.fail_export:
            raise SourceError("export failed")
        if self.skip_export:
            return SourceExportResult(
                schema_fields=[],
                segments=[],
                job_references=[],
                skipped=True,
                skip_reason="BigQuery extract source table was not found",
            )
        return SourceExportResult(
            schema_fields=[{"name": "OrderID", "type": "INT64"}],
            segments=[{"destination_uri": context.destination_uri + "/segment-*.parquet"}],
            job_references=[{"jobId": "job-1"}],
        )

    def map_schema(self, export_result):
        self.calls.append(("map_schema", len(export_result.schema_fields)))
        return self.columns


def test_handler_full_refresh_success(base_payload):
    snowflake = FakeSnowflake(table_exists=False, target_view_exists=False)
    source = FakeSource()

    result = IcebergSyncRunner(
        object(),
        snowflake_client=snowflake,
        source_adapters={"bigquery": source},
    ).run(base_payload)

    assert result["status"] == "success"
    assert result["effective_mode"] == "full_refresh"
    assert result["view_columns"] == [
        {"source_name": "OrderID", "alias": "order_id", "expression": None}
    ]
    assert ("delete", None) in snowflake.calls
    assert ("commit",) in snowflake.calls
    assert ("create_or_replace_view", "ORDERS", ["order_id"]) in snowflake.calls
    assert ("write_run_log", "success") in snowflake.calls
    assert result["retry"]["attempts"] == 1
    assert result["cleanup"]["created_internal_table"] is True


def test_handler_declared_columns_override_source_schema(payload_factory):
    payload = payload_factory(
        columns=[
            {
                "name": "OrderID",
                "type": "BIGINT",
                "alias": "order_id",
            },
            {
                "name": "AmountText",
                "type": "VARCHAR",
                "alias": "amount",
                "expression": 'TRY_TO_NUMBER("AmountText")',
            },
        ]
    )
    snowflake = FakeSnowflake(table_exists=False, target_view_exists=False)
    source = FakeSource()

    result = IcebergSyncRunner(
        object(),
        snowflake_client=snowflake,
        source_adapters={"bigquery": source},
    ).run(payload)

    assert result["status"] == "success"
    assert ("map_schema", 1) not in source.calls
    assert result["view_columns"] == [
        {"source_name": "OrderID", "alias": "order_id", "expression": None},
        {
            "source_name": "AmountText",
            "alias": "amount",
            "expression": 'TRY_TO_NUMBER("AmountText")',
        },
    ]
    assert ("create_or_replace_view", "ORDERS", ["order_id", "amount"]) in snowflake.calls


def test_handler_incremental_uses_incremental_predicate(payload_factory):
    payload = payload_factory(
        incremental_predicate="event_date >= '2026-01-01'",
        bigquery__incremental_predicates=["_PARTITIONDATE >= '2026-01-01'"],
    )
    snowflake = FakeSnowflake(table_exists=True, target_view_exists=True)

    IcebergSyncRunner(
        object(),
        snowflake_client=snowflake,
        source_adapters={"bigquery": FakeSource()},
    ).run(payload)

    assert ("delete", "event_date >= '2026-01-01'") in snowflake.calls


def test_handler_existing_table_allows_additive_columns(base_payload):
    snowflake = FakeSnowflake(table_exists=True, target_view_exists=True)
    source = FakeSource(
        columns=[
            SnowflakeColumn("OrderID", "BIGINT"),
            SnowflakeColumn("CustomerName", "VARCHAR"),
        ]
    )

    IcebergSyncRunner(
        object(),
        snowflake_client=snowflake,
        source_adapters={"bigquery": source},
    ).run(base_payload)

    assert ("describe_table", "__ORDERS") in snowflake.calls
    assert ("add_columns", ["CustomerName"]) in snowflake.calls
    assert ("commit",) in snowflake.calls


def test_handler_existing_table_rejects_schema_change(base_payload):
    snowflake = FakeSnowflake(
        table_exists=True,
        target_view_exists=True,
        existing_columns=[SnowflakeColumn("OrderID", "VARCHAR")],
    )

    with pytest.raises(SchemaError, match="incompatible type change"):
        IcebergSyncRunner(
            object(),
            snowflake_client=snowflake,
            source_adapters={"bigquery": FakeSource()},
        ).run(base_payload)

    assert ("write_run_log", "failure") in snowflake.calls
    assert not any(call[0] == "copy" for call in snowflake.calls)


def test_handler_writes_failure_log_when_source_export_fails(base_payload):
    snowflake = FakeSnowflake(table_exists=False)

    with pytest.raises(SourceError, match="export failed"):
        IcebergSyncRunner(
            object(),
            snowflake_client=snowflake,
            source_adapters={"bigquery": FakeSource(fail_export=True)},
        ).run(base_payload)

    assert ("begin",) not in snowflake.calls
    assert ("write_run_log", "failure") in snowflake.calls
    assert snowflake.run_logs[-1]["error_message"] == "SourceError: export failed"


def test_handler_skips_when_source_export_is_skipped(base_payload):
    snowflake = FakeSnowflake(table_exists=False, target_view_exists=False)
    source = FakeSource(skip_export=True)

    result = IcebergSyncRunner(
        object(),
        snowflake_client=snowflake,
        source_adapters={"bigquery": source},
    ).run(base_payload)

    assert result["status"] == "skipped"
    assert result["error_message"] == "BigQuery extract source table was not found"
    assert ("create_iceberg_table", []) not in snowflake.calls
    assert not any(
        call[0] in {"begin", "copy", "commit", "create_or_replace_view"} for call in snowflake.calls
    )
    assert ("write_run_log", "skipped") in snowflake.calls
    assert snowflake.run_logs[-1]["status"] == "skipped"


def test_handler_sanitizes_failure_log_error_message(base_payload):
    snowflake = FakeSnowflake(table_exists=False)
    source = FakeSource(fail_export=True)

    def fail_export(config, context):
        raise SourceError(
            "BigQuery API error 403: token=abc123 "
            "gs://private-bucket/path "
            "https://example.invalid/path"
        )

    source.export = fail_export

    with pytest.raises(SourceError):
        IcebergSyncRunner(
            object(),
            snowflake_client=snowflake,
            source_adapters={"bigquery": source},
        ).run(base_payload)

    error_message = snowflake.run_logs[-1]["error_message"]
    assert "abc123" not in error_message
    assert "private-bucket" not in error_message
    assert "example.invalid" not in error_message
    assert "SourceError:" in error_message


def test_handler_rolls_back_transaction_on_copy_failure(base_payload):
    snowflake = FakeSnowflake(table_exists=False, fail_copy=True)

    with pytest.raises(SnowflakeExecutionError, match="copy failed"):
        IcebergSyncRunner(
            object(),
            snowflake_client=snowflake,
            source_adapters={"bigquery": FakeSource()},
        ).run(base_payload)

    assert ("rollback",) in snowflake.calls
    assert ("write_run_log", "failure") in snowflake.calls


def test_handler_rolls_back_transaction_on_delete_failure(base_payload):
    snowflake = FakeSnowflake(table_exists=False, fail_delete=True)

    with pytest.raises(SnowflakeExecutionError, match="delete failed"):
        IcebergSyncRunner(
            object(),
            snowflake_client=snowflake,
            source_adapters={"bigquery": FakeSource()},
        ).run(base_payload)

    assert ("begin",) in snowflake.calls
    assert ("rollback",) in snowflake.calls
    assert ("commit",) not in snowflake.calls
    assert ("write_run_log", "failure") in snowflake.calls


def test_retryable_snowflake_internal_error_retries_and_succeeds(payload_factory):
    payload = payload_factory(
        retry__initial_delay_seconds=0,
        retry__jitter_seconds=0,
    )
    snowflake = FakeSnowflake(
        table_exists=False,
        copy_errors=[SnowflakeExecutionError("SQL execution internal error: incident 123")],
    )
    source = FakeSource()

    result = IcebergSyncRunner(
        object(),
        snowflake_client=snowflake,
        source_adapters={"bigquery": source},
        sleep_func=lambda seconds: None,
        jitter_func=lambda start, end: 0,
    ).run(payload)

    assert result["status"] == "success"
    assert result["retry"]["attempts"] == 2
    assert result["retry"]["retryable_errors"][0]["attempt"] == 1
    assert result["retry"]["retryable_errors"][0]["rolled_back"] is True
    assert snowflake.calls.count(("rollback",)) == 1
    assert snowflake.calls.count(("begin",)) == 2
    assert [call for call in source.calls if call[0] == "full_refresh"] == [
        ("full_refresh", "gs://bucket/dbt/run")
    ]
    assert (
        snowflake.calls.count(
            (
                "copy",
                '@"ANALYTICS"."PUBLIC"."EXPORT_STAGE"/dbt/run',
                None,
                None,
                False,
                "add_files_copy",
                None,
            )
        )
        == 2
    )


def test_retryable_snowflake_internal_error_messages_are_classified():
    assert is_retryable_snowflake_error(
        SnowflakeExecutionError("SQL execution internal error while loading table")
    )
    assert is_retryable_snowflake_error(SnowflakeExecutionError("Processing aborted; incident 42"))
    assert not is_retryable_snowflake_error(SnowflakeExecutionError("000603 XX000 300005"))
    assert is_retryable_snowflake_error(
        SnowflakeExecutionError("Scoped transaction started in stored procedure is incomplete")
    )
    assert not is_retryable_snowflake_error(ConfigError("invalid config"))


def test_run_log_lock_contention_errors_are_classified():
    assert is_retryable_run_log_error(
        SnowflakeExecutionError("000625 table has locked table ICEBERG_SYNC_RUN_LOG")
    )
    assert is_retryable_run_log_error(
        SnowflakeExecutionError("number of waiters for this lock exceeds the limit")
    )
    assert not is_retryable_run_log_error(SnowflakeExecutionError("copy failed"))


def test_non_retryable_snowflake_error_is_not_retried(base_payload):
    snowflake = FakeSnowflake(
        table_exists=False,
        copy_errors=[SnowflakeExecutionError("insufficient privileges to operate on table")],
    )

    with pytest.raises(SnowflakeExecutionError, match="insufficient privileges"):
        IcebergSyncRunner(
            object(),
            snowflake_client=snowflake,
            source_adapters={"bigquery": FakeSource()},
            sleep_func=lambda seconds: None,
        ).run(base_payload)

    assert snowflake.calls.count(("begin",)) == 1
    assert snowflake.calls.count(("rollback",)) == 1
    assert snowflake.run_logs[-1]["retry"]["retryable_errors"] == []


def test_retry_exhaustion_raises_original_error_and_logs_history(payload_factory):
    payload = payload_factory(
        retry__max_attempts=2,
        retry__initial_delay_seconds=0,
        retry__jitter_seconds=0,
    )
    error = SnowflakeExecutionError("SQL execution internal error incident")
    snowflake = FakeSnowflake(table_exists=False, copy_errors=[error, error])

    with pytest.raises(SnowflakeExecutionError, match="internal error"):
        IcebergSyncRunner(
            object(),
            snowflake_client=snowflake,
            source_adapters={"bigquery": FakeSource()},
            sleep_func=lambda seconds: None,
            jitter_func=lambda start, end: 0,
        ).run(payload)

    assert snowflake.calls.count(("begin",)) == 2
    assert snowflake.calls.count(("rollback",)) == 2
    assert snowflake.run_logs[-1]["retry"]["attempts"] == 2
    assert len(snowflake.run_logs[-1]["retry"]["retryable_errors"]) == 2


def test_rollback_failure_preserves_original_copy_error(payload_factory):
    payload = payload_factory(
        retry__max_attempts=1,
        retry__initial_delay_seconds=0,
        retry__jitter_seconds=0,
    )
    snowflake = FakeSnowflake(
        table_exists=False,
        fail_rollback=True,
        copy_errors=[SnowflakeExecutionError("SQL execution internal error incident")],
    )

    with pytest.raises(SnowflakeExecutionError, match="SQL execution internal error"):
        IcebergSyncRunner(
            object(),
            snowflake_client=snowflake,
            source_adapters={"bigquery": FakeSource()},
            sleep_func=lambda seconds: None,
            jitter_func=lambda start, end: 0,
        ).run(payload)

    retry_error = snowflake.run_logs[-1]["retry"]["retryable_errors"][0]
    assert retry_error["rolled_back"] is False
    assert "rollback failed" in retry_error["rollback_error_message"]


def test_config_schema_and_source_errors_are_not_retried(base_payload):
    for exc in (
        SchemaError("schema failed"),
        SourceError("permission denied", status_code=403),
    ):
        snowflake = FakeSnowflake(table_exists=False)
        source = FakeSource()

        def fail_map_schema(export_result, exc=exc):
            raise exc

        source.map_schema = fail_map_schema

        with pytest.raises(type(exc)):
            IcebergSyncRunner(
                object(),
                snowflake_client=snowflake,
                source_adapters={"bigquery": source},
                sleep_func=lambda seconds: None,
            ).run(base_payload)

        assert ("begin",) not in snowflake.calls
        assert snowflake.run_logs[-1]["retry"]["attempts"] == 0


def test_created_internal_table_is_dropped_on_failed_initial_run(base_payload):
    snowflake = FakeSnowflake(table_exists=False, fail_copy=True)

    with pytest.raises(SnowflakeExecutionError, match="copy failed"):
        IcebergSyncRunner(
            object(),
            snowflake_client=snowflake,
            source_adapters={"bigquery": FakeSource()},
        ).run(base_payload)

    assert ("drop_iceberg_table", "__ORDERS") in snowflake.calls
    assert snowflake.run_logs[-1]["cleanup"]["created_internal_table"] is True
    assert snowflake.run_logs[-1]["cleanup"]["dropped_created_internal_table"] is True


def test_preexisting_internal_table_is_not_dropped_on_failure(base_payload):
    snowflake = FakeSnowflake(table_exists=True, target_view_exists=True, fail_copy=True)

    with pytest.raises(SnowflakeExecutionError, match="copy failed"):
        IcebergSyncRunner(
            object(),
            snowflake_client=snowflake,
            source_adapters={"bigquery": FakeSource()},
        ).run(base_payload)

    assert not any(call[0] == "drop_iceberg_table" for call in snowflake.calls)
    assert snowflake.run_logs[-1]["cleanup"]["created_internal_table"] is False


def test_view_creation_failure_drops_new_internal_table(base_payload):
    snowflake = FakeSnowflake(table_exists=False, fail_create_view=True)

    with pytest.raises(SnowflakeExecutionError, match="view creation failed"):
        IcebergSyncRunner(
            object(),
            snowflake_client=snowflake,
            source_adapters={"bigquery": FakeSource()},
        ).run(base_payload)

    assert ("commit",) in snowflake.calls
    assert ("drop_iceberg_table", "__ORDERS") in snowflake.calls
    assert snowflake.run_logs[-1]["cleanup"]["dropped_created_internal_table"] is True


def test_cleanup_failure_preserves_original_error_and_logs_cleanup_failure(base_payload):
    snowflake = FakeSnowflake(table_exists=False, fail_copy=True, fail_drop=True)

    with pytest.raises(SnowflakeExecutionError, match="copy failed"):
        IcebergSyncRunner(
            object(),
            snowflake_client=snowflake,
            source_adapters={"bigquery": FakeSource()},
        ).run(base_payload)

    assert snowflake.run_logs[-1]["error_message"] == "SnowflakeExecutionError: copy failed"
    assert "drop failed" in snowflake.run_logs[-1]["cleanup"]["cleanup_error_message"]


def test_success_run_log_lock_contention_is_retried_and_does_not_fail(payload_factory):
    payload = payload_factory(
        retry__initial_delay_seconds=0,
        retry__jitter_seconds=0,
    )
    snowflake = FakeSnowflake(
        table_exists=False,
        run_log_errors=[
            SnowflakeExecutionError(
                "000625 locked table ICEBERG_SYNC_RUN_LOG; number of waiters exceeded"
            )
        ],
    )

    result = IcebergSyncRunner(
        object(),
        snowflake_client=snowflake,
        source_adapters={"bigquery": FakeSource()},
        sleep_func=lambda seconds: None,
        jitter_func=lambda start, end: 0,
    ).run(payload)

    assert result["status"] == "success"
    assert "run_log_error" not in result
    assert snowflake.calls.count(("write_run_log", "success")) == 2
    assert len(result["retry"]["run_log_errors"]) == 1
    assert snowflake.run_logs[-1]["status"] == "success"


def test_success_run_log_failure_is_best_effort_by_default(base_payload):
    snowflake = FakeSnowflake(
        table_exists=False,
        run_log_errors=[SnowflakeExecutionError("non-retryable run log write failed")],
    )

    result = IcebergSyncRunner(
        object(),
        snowflake_client=snowflake,
        source_adapters={"bigquery": FakeSource()},
    ).run(base_payload)

    assert result["status"] == "success"
    assert "run log write failed" in result["run_log_error"]
    assert snowflake.run_logs == []


def test_success_run_log_failure_can_be_strict(payload_factory):
    payload = payload_factory(run_log__fail_on_error=True)
    snowflake = FakeSnowflake(
        table_exists=False,
        run_log_errors=[SnowflakeExecutionError("non-retryable run log write failed")],
    )

    with pytest.raises(SnowflakeExecutionError, match="run log write failed"):
        IcebergSyncRunner(
            object(),
            snowflake_client=snowflake,
            source_adapters={"bigquery": FakeSource()},
        ).run(payload)


def test_failure_run_log_error_does_not_mask_original_load_error(base_payload):
    snowflake = FakeSnowflake(
        table_exists=False,
        fail_copy=True,
        run_log_errors=[SnowflakeExecutionError("non-retryable run log write failed")],
    )

    with pytest.raises(SnowflakeExecutionError, match="copy failed"):
        IcebergSyncRunner(
            object(),
            snowflake_client=snowflake,
            source_adapters={"bigquery": FakeSource()},
            sleep_func=lambda seconds: None,
            jitter_func=lambda start, end: 0,
        ).run(base_payload)

    assert snowflake.run_logs == []


def test_handler_skips_run_log_when_run_log_table_is_not_configured(payload_factory):
    payload = payload_factory(deployment__run_log_table=None)
    snowflake = FakeSnowflake(table_exists=False)

    IcebergSyncRunner(
        object(),
        snowflake_client=snowflake,
        source_adapters={"bigquery": FakeSource()},
    ).run(payload)

    assert not any(call[0] == "write_run_log" for call in snowflake.calls)
