from __future__ import annotations

import pytest

from procedure.errors import SchemaError, SnowflakeExecutionError, SourceError
from procedure.handler import IcebergSyncRunner
from procedure.schema import SnowflakeColumn
from procedure.sources.base import SourceExecutionContext, SourceExportResult


class FakeSnowflake:
    def __init__(
        self,
        *,
        table_exists=False,
        fail_copy=False,
        fail_delete=False,
        existing_columns=None,
    ):
        self.query_ids = []
        self.table_exists_value = table_exists
        self.fail_copy = fail_copy
        self.fail_delete = fail_delete
        self.existing_columns = existing_columns or [SnowflakeColumn("OrderID", "BIGINT")]
        self.calls = []
        self.run_logs = []

    def ensure_run_log(self, relation):
        self.calls.append(("ensure_run_log", relation))

    def table_exists(self, relation):
        self.calls.append(("table_exists", relation.identifier))
        return self.table_exists_value

    def resolve_stage_location(self, export_location, run_id):
        self.calls.append(("resolve_stage_location", export_location, run_id))

        class Stage:
            run_stage_location = '@"ANALYTICS"."PUBLIC"."EXPORT_STAGE"/dbt/run'
            gcs_run_uri = "gcs://bucket/dbt/run"

        return Stage()

    def create_iceberg_table(self, config, columns):
        self.calls.append(("create_iceberg_table", [column.source_name for column in columns]))

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

    def copy_into_iceberg(self, relation, stage_run_location):
        self.calls.append(("copy", stage_run_location))
        if self.fail_copy:
            raise SnowflakeExecutionError("copy failed")

    def commit(self):
        self.calls.append(("commit",))

    def rollback(self):
        self.calls.append(("rollback",))

    def write_run_log(self, relation, payload):
        self.calls.append(("write_run_log", payload["status"]))
        self.run_logs.append(payload)


class FakeSource:
    source_type = "bigquery"

    def __init__(self, *, fail_export=False, columns=None):
        self.calls = []
        self.fail_export = fail_export
        self.columns = columns or [SnowflakeColumn("OrderID", "BIGINT")]

    def export_location(self, config):
        self.calls.append(("export_location", config.source_type))
        return config.bigquery.export_location

    def export(self, config, context: SourceExecutionContext):
        self.calls.append((context.effective_mode, context.destination_uri))
        if self.fail_export:
            raise SourceError("export failed")
        return SourceExportResult(
            schema_fields=[{"name": "OrderID", "type": "INT64"}],
            segments=[{"destination_uri": context.destination_uri + "/segment-*.parquet"}],
            job_references=[{"jobId": "job-1"}],
        )

    def map_schema(self, export_result):
        self.calls.append(("map_schema", len(export_result.schema_fields)))
        return self.columns


def test_handler_full_refresh_success(base_payload):
    snowflake = FakeSnowflake(table_exists=False)
    source = FakeSource()

    result = IcebergSyncRunner(
        object(),
        snowflake_client=snowflake,
        source_adapters={"bigquery": source},
    ).run(base_payload)

    assert result["status"] == "success"
    assert result["effective_mode"] == "full_refresh"
    assert result["view_columns"] == [{"source_name": "OrderID", "alias": "order_id"}]
    assert ("delete", None) in snowflake.calls
    assert ("commit",) in snowflake.calls
    assert ("write_run_log", "success") in snowflake.calls


def test_handler_incremental_uses_incremental_predicate(payload_factory):
    payload = payload_factory(
        incremental_predicate="event_date >= '2026-01-01'",
        bigquery__incremental_predicates=["_PARTITIONDATE >= '2026-01-01'"],
    )
    snowflake = FakeSnowflake(table_exists=True)

    IcebergSyncRunner(
        object(),
        snowflake_client=snowflake,
        source_adapters={"bigquery": FakeSource()},
    ).run(payload)

    assert ("delete", "event_date >= '2026-01-01'") in snowflake.calls


def test_handler_existing_table_allows_additive_columns(base_payload):
    snowflake = FakeSnowflake(table_exists=True)
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
        existing_columns=[SnowflakeColumn("OrderID", "VARCHAR")],
    )

    with pytest.raises(SchemaError, match="incompatible type change"):
        IcebergSyncRunner(
            object(),
            snowflake_client=snowflake,
            source_adapters={"bigquery": FakeSource()},
        ).run(base_payload)

    assert ("write_run_log", "failure") in snowflake.calls
    assert ("copy", '@"ANALYTICS"."PUBLIC"."EXPORT_STAGE"/dbt/run') not in snowflake.calls


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


def test_handler_skips_run_log_when_run_log_table_is_not_configured(payload_factory):
    payload = payload_factory(deployment__run_log_table=None)
    snowflake = FakeSnowflake(table_exists=False)

    IcebergSyncRunner(
        object(),
        snowflake_client=snowflake,
        source_adapters={"bigquery": FakeSource()},
    ).run(payload)

    assert not any(call[0] == "write_run_log" for call in snowflake.calls)
