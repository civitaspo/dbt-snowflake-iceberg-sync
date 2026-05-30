from __future__ import annotations

import pytest

from procedure.errors import SnowflakeExecutionError
from procedure.handler import IcebergSyncRunner
from procedure.schema import SnowflakeColumn
from procedure.sources.base import SourceExecutionContext, SourceExportResult


class FakeSnowflake:
    def __init__(self, *, table_exists=False, fail_copy=False):
        self.query_ids = []
        self.table_exists_value = table_exists
        self.fail_copy = fail_copy
        self.calls = []

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
        return [SnowflakeColumn("OrderID", "BIGINT")]

    def add_columns(self, relation, columns):
        self.calls.append(("add_columns", [column.source_name for column in columns]))

    def begin(self):
        self.calls.append(("begin",))

    def delete_from_iceberg(self, relation, predicate):
        self.calls.append(("delete", predicate))

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


class FakeSource:
    source_type = "bigquery"

    def __init__(self):
        self.calls = []

    def export_location(self, config):
        self.calls.append(("export_location", config.source_type))
        return config.bigquery.export_location

    def export(self, config, context: SourceExecutionContext):
        self.calls.append((context.effective_mode, context.destination_uri))
        return SourceExportResult(
            schema_fields=[{"name": "OrderID", "type": "INT64"}],
            segments=[{"destination_uri": context.destination_uri + "/segment-*.parquet"}],
            job_references=[{"jobId": "job-1"}],
        )

    def map_schema(self, export_result):
        self.calls.append(("map_schema", len(export_result.schema_fields)))
        return [SnowflakeColumn("OrderID", "BIGINT")]


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
