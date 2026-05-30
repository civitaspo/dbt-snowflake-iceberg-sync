from __future__ import annotations

import pytest

from procedure.config import ConfigError, parse_config
from procedure.sources.base import SourceExecutionContext
from procedure.sources.bigquery import (
    BigQuerySourceAdapter,
    concrete_extract_tables,
    resolve_predicate_type,
    schema_table_id_for_extract,
    select_sql_with_predicates,
    staging_hash,
    staging_table_id_for,
)


class FakeBigQueryClient:
    def __init__(self):
        self.tables = {
            ("project", "dataset", "orders"): {
                "schema": {"fields": [{"name": "OrderID", "type": "INT64"}]}
            },
            ("project", "dataset", "events_20260101"): {
                "schema": {"fields": [{"name": "EventID", "type": "STRING"}]}
            },
            ("project", "dataset", "events_20260102"): {
                "schema": {"fields": [{"name": "EventID", "type": "STRING"}]}
            },
        }
        self.query_jobs = []
        self.extract_jobs = []
        self.patches = []

    def get_table(self, project_id, dataset_id, table_id):
        key = (project_id, dataset_id, table_id)
        if key not in self.tables:
            raise KeyError(key)
        table = dict(self.tables[key])
        table["tableReference"] = {
            "projectId": project_id,
            "datasetId": dataset_id,
            "tableId": table_id,
        }
        return table

    def list_tables(self, project_id, dataset_id, *, prefix=None):
        return [
            {"tableReference": {"tableId": table_id}}
            for (project, dataset, table_id) in self.tables
            if project == project_id
            and dataset == dataset_id
            and (prefix is None or table_id.startswith(prefix))
        ]

    def run_query_job(self, project_id, *, location, query, destination_table):
        self.query_jobs.append((query, destination_table))
        table_id = destination_table["tableId"]
        self.tables[(project_id, destination_table["datasetId"], table_id)] = {
            "schema": {"fields": [{"name": "OrderID", "type": "INT64"}]}
        }
        return {"jobReference": {"projectId": project_id, "jobId": "query-job"}}

    def run_extract_job(self, project_id, *, location, source_table, destination_uris):
        self.extract_jobs.append((source_table, destination_uris))
        return {"jobReference": {"projectId": project_id, "jobId": "extract-job"}}

    def patch_table(self, project_id, dataset_id, table_id, patch):
        self.patches.append((table_id, patch))
        table = self.tables[(project_id, dataset_id, table_id)]
        table.update(patch)
        return table


def test_auto_extract_without_predicates_uses_none(base_payload):
    config = parse_config(base_payload)

    assert resolve_predicate_type(config.bigquery, (), FakeBigQueryClient()) == "none"


def test_auto_extract_wildcard_uses_table_suffix(payload_factory):
    config = parse_config(payload_factory(bigquery__table_id="events_*"))

    assert (
        resolve_predicate_type(config.bigquery, ("20260101",), FakeBigQueryClient())
        == "table_suffix"
    )


def test_table_suffix_expands_concrete_tables(payload_factory):
    config = parse_config(
        payload_factory(
            bigquery__table_id="events_*",
            bigquery__export_predicate_type="table_suffix",
        )
    )

    assert concrete_extract_tables(
        config.bigquery, "table_suffix", ("20260101", "20260102"), FakeBigQueryClient()
    ) == ["events_20260101", "events_20260102"]


def test_table_suffix_requires_wildcard(base_payload):
    config = parse_config(base_payload)

    with pytest.raises(ConfigError, match="table id ending"):
        concrete_extract_tables(
            config.bigquery,
            "table_suffix",
            ("20260101",),
            FakeBigQueryClient(),
        )


def test_partition_decorator_inspects_base_table_schema():
    assert schema_table_id_for_extract("partition_decorator", "orders$20260101") == "orders"


def test_select_sql_wraps_where_predicates():
    sql = select_sql_with_predicates(
        "select * from `project.dataset.orders`",
        "where",
        ("event_date = '2026-01-01'", "event_date = '2026-01-02'"),
    )

    assert "FROM (" in sql
    assert "WHERE (event_date = '2026-01-01') OR (event_date = '2026-01-02')" in sql


def test_select_export_reuses_matching_staging_table(payload_factory):
    client = FakeBigQueryClient()
    payload = payload_factory(
        bigquery__export_strategy="select",
        bigquery__export_predicate_type="where",
        bigquery__incremental_predicates=["event_date = '2026-01-01'"],
        bigquery__staging_dataset_id="staging",
        incremental_predicate="event_date = '2026-01-01'",
        model__sql="select * from `project.dataset.orders`",
    )
    config = parse_config(payload)
    staging_table_id = staging_table_id_for(config, config.bigquery.incremental_predicates)
    client.tables[("project", "staging", staging_table_id)] = {
        "schema": {"fields": [{"name": "OrderID", "type": "INT64"}]},
        "labels": {
            "dbt_iceberg_sync_hash": staging_hash(config, config.bigquery.incremental_predicates)
        },
    }

    result = BigQuerySourceAdapter(client).export(
        config,
        context=SourceExecutionContext(
            effective_mode="incremental",
            destination_uri="gcs://bucket/prefix/run",
        ),
    )

    assert result.staging_table_reference == f"project.staging.{staging_table_id}"
    assert client.query_jobs == []
    assert len(client.extract_jobs) == 1


def test_select_export_force_rebuild_runs_query(payload_factory):
    client = FakeBigQueryClient()
    payload = payload_factory(
        bigquery__export_strategy="select",
        bigquery__staging_dataset_id="staging",
        bigquery__force_rebuild_staging_table=True,
        model__sql="select * from `project.dataset.orders`",
    )
    config = parse_config(payload)

    BigQuerySourceAdapter(client).export(
        config,
        context=SourceExecutionContext(
            effective_mode="full_refresh",
            destination_uri="gcs://bucket/prefix/run",
        ),
    )

    assert len(client.query_jobs) == 1
    assert len(client.patches) == 1
