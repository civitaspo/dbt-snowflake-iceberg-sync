from __future__ import annotations

import pytest

from procedure.config import ConfigError, parse_config
from procedure.errors import SourceError
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
    def __init__(self, *, fail_extract: bool = False, fail_query: bool = False):
        self.tables = {
            ("project", "dataset", "orders"): {
                "schema": {"fields": [{"name": "OrderID", "type": "INT64"}]}
            },
            ("project", "dataset", "orders_by_date"): {
                "schema": {"fields": [{"name": "OrderID", "type": "INT64"}]},
                "timePartitioning": {"type": "DAY", "field": "order_date"},
            },
            ("project", "dataset", "orders_by_bucket"): {
                "schema": {"fields": [{"name": "OrderID", "type": "INT64"}]},
                "rangePartitioning": {
                    "field": "bucket_id",
                    "range": {"start": "0", "end": "100", "interval": "10"},
                },
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
        self.fail_extract = fail_extract
        self.fail_query = fail_query

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
        if self.fail_query:
            raise SourceError("query failed")
        self.query_jobs.append((query, destination_table))
        table_id = destination_table["tableId"]
        self.tables[(project_id, destination_table["datasetId"], table_id)] = {
            "schema": {"fields": [{"name": "OrderID", "type": "INT64"}]}
        }
        return {"jobReference": {"projectId": project_id, "jobId": "query-job"}}

    def run_extract_job(self, project_id, *, location, source_table, destination_uris):
        if self.fail_extract:
            raise SourceError("extract failed")
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


def test_auto_extract_non_partitioned_table_with_predicates_is_rejected(base_payload):
    config = parse_config(base_payload)

    with pytest.raises(ConfigError, match="native partitioned table"):
        resolve_predicate_type(config.bigquery, ("20260101",), FakeBigQueryClient())


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


def test_extract_none_on_wildcard_lists_matching_tables(payload_factory):
    config = parse_config(payload_factory(bigquery__table_id="events_*"))

    assert concrete_extract_tables(
        config.bigquery, "none", (), FakeBigQueryClient()
    ) == ["events_20260101", "events_20260102"]


def test_auto_extract_time_partitioned_table_uses_partition_decorator(payload_factory):
    config = parse_config(payload_factory(bigquery__table_id="orders_by_date"))
    client = FakeBigQueryClient()

    assert (
        resolve_predicate_type(config.bigquery, ("20260101",), client)
        == "partition_decorator"
    )
    assert concrete_extract_tables(
        config.bigquery, "partition_decorator", ("20260101",), client
    ) == ["orders_by_date$20260101"]


def test_auto_extract_range_partitioned_table_uses_partition_decorator(payload_factory):
    config = parse_config(payload_factory(bigquery__table_id="orders_by_bucket"))
    client = FakeBigQueryClient()

    assert (
        resolve_predicate_type(config.bigquery, ("10",), client) == "partition_decorator"
    )
    assert concrete_extract_tables(
        config.bigquery, "partition_decorator", ("10",), client
    ) == ["orders_by_bucket$10"]


def test_extract_non_partitioned_table_runs_direct_extract(base_payload):
    config = parse_config(base_payload)
    client = FakeBigQueryClient()

    result = BigQuerySourceAdapter(client).export(
        config,
        context=SourceExecutionContext(
            effective_mode="full_refresh",
            destination_uri="gcs://bucket/prefix/run",
        ),
    )

    assert client.extract_jobs == [
        (
            {"projectId": "project", "datasetId": "dataset", "tableId": "orders"},
            ["gcs://bucket/prefix/run/segment-00000-*.parquet"],
        )
    ]
    assert result.segments == [
        {
            "table_id": "orders",
            "destination_uri": "gcs://bucket/prefix/run/segment-00000-*.parquet",
        }
    ]


def test_table_suffix_requires_wildcard(base_payload):
    config = parse_config(base_payload)

    with pytest.raises(ConfigError, match="table id ending"):
        concrete_extract_tables(
            config.bigquery,
            "table_suffix",
            ("20260101",),
            FakeBigQueryClient(),
        )


@pytest.mark.parametrize(
    ("predicate_type", "updates", "predicates", "message"),
    [
        (
            "table_suffix",
            {"bigquery__table_id": "events_*"},
            (),
            "at least one predicate",
        ),
        (
            "table_suffix",
            {"bigquery__table_id": "events_20260101"},
            ("20260101",),
            "ending with",
        ),
        ("partition_decorator", {}, (), "at least one predicate"),
        ("where", {}, ("event_date = DATE '2026-01-01'",), "does not support where"),
    ],
)
def test_rejects_invalid_extract_predicate_combinations(
    payload_factory, predicate_type, updates, predicates, message
):
    config = parse_config(payload_factory(**updates))

    with pytest.raises(ConfigError, match=message):
        concrete_extract_tables(
            config.bigquery,
            predicate_type,
            predicates,
            FakeBigQueryClient(),
        )


def test_partition_decorator_inspects_base_table_schema():
    assert schema_table_id_for_extract("partition_decorator", "orders$20260101") == "orders"


@pytest.mark.parametrize(
    ("predicate_type", "predicates", "expected"),
    [
        ("none", (), "select * from `project.dataset.orders`"),
        (
            "where",
            ("event_date = '2026-01-01'",),
            "WHERE (event_date = '2026-01-01')",
        ),
    ],
)
def test_select_sql_predicate_modes(predicate_type, predicates, expected):
    sql = select_sql_with_predicates(
        "select * from `project.dataset.orders`;",
        predicate_type,
        predicates,
    )

    assert expected in sql
    assert not sql.rstrip().endswith(";")


def test_select_sql_wraps_where_predicates():
    sql = select_sql_with_predicates(
        "select * from `project.dataset.orders`",
        "where",
        ("event_date = '2026-01-01'", "event_date = '2026-01-02'"),
    )

    assert "FROM (" in sql
    assert "WHERE (event_date = '2026-01-01') OR (event_date = '2026-01-02')" in sql


def test_select_sql_rejects_empty_model_sql():
    with pytest.raises(ConfigError, match="model SQL is required"):
        select_sql_with_predicates("", "none", ())


def test_select_sql_rejects_non_where_predicates():
    with pytest.raises(ConfigError, match="supports only where"):
        select_sql_with_predicates("select 1", "table_suffix", ("20260101",))


@pytest.mark.parametrize(
    ("predicates", "expected"),
    [((), "none"), (("event_date = DATE '2026-01-01'",), "where")],
)
def test_auto_select_predicate_type(predicates, expected, payload_factory):
    config = parse_config(
        payload_factory(
            bigquery__export_strategy="select",
            bigquery__staging_dataset_id="staging",
            model__sql="select * from `project.dataset.orders`",
        )
    )

    assert resolve_predicate_type(config.bigquery, predicates, FakeBigQueryClient()) == expected


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


@pytest.mark.parametrize(
    "existing_table",
    [
        None,
        {"schema": {"fields": [{"name": "OrderID", "type": "INT64"}]}},
        {
            "schema": {"fields": [{"name": "OrderID", "type": "INT64"}]},
            "labels": {"dbt_iceberg_sync_hash": "wrong"},
        },
        {
            "schema": {"fields": [{"name": "OrderID", "type": "INT64"}]},
            "labels": {"dbt_iceberg_sync_hash": "will-be-overwritten"},
            "expirationTime": "1",
        },
    ],
)
def test_select_export_rebuilds_when_staging_table_cannot_be_reused(
    payload_factory, existing_table
):
    client = FakeBigQueryClient()
    payload = payload_factory(
        bigquery__export_strategy="select",
        bigquery__staging_dataset_id="staging",
        model__sql="select * from `project.dataset.orders`",
    )
    config = parse_config(payload)
    staging_table_id = staging_table_id_for(config, ())
    if existing_table is not None:
        client.tables[("project", "staging", staging_table_id)] = existing_table

    BigQuerySourceAdapter(client).export(
        config,
        context=SourceExecutionContext(
            effective_mode="full_refresh",
            destination_uri="gcs://bucket/prefix/run",
        ),
    )

    assert len(client.query_jobs) == 1
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


def test_select_export_runs_query_then_extracts_staging_table(payload_factory):
    client = FakeBigQueryClient()
    payload = payload_factory(
        bigquery__export_strategy="select",
        bigquery__export_predicate_type="where",
        bigquery__full_refresh_predicates=["order_date = DATE '2026-01-01'"],
        bigquery__staging_dataset_id="staging",
        bigquery__staging_table_reuse=False,
        model__sql="select *\nfrom `project.dataset.orders`",
    )
    config = parse_config(payload)

    result = BigQuerySourceAdapter(client).export(
        config,
        context=SourceExecutionContext(
            effective_mode="full_refresh",
            destination_uri="gcs://bucket/prefix/run",
        ),
    )

    assert len(client.query_jobs) == 1
    query, destination_table = client.query_jobs[0]
    assert "WHERE (order_date = DATE '2026-01-01')" in query
    assert client.extract_jobs == [
        (
            destination_table,
            ["gcs://bucket/prefix/run/segment-00000-*.parquet"],
        )
    ]
    assert result.staging_table_reference == (
        f"project.staging.{destination_table['tableId']}"
    )


def test_extract_export_raises_when_no_tables_match(payload_factory):
    client = FakeBigQueryClient()
    client.tables = {}
    config = parse_config(
        payload_factory(
            bigquery__table_id="events_*",
            bigquery__export_predicate_type="none",
        )
    )

    with pytest.raises(SourceError, match="no BigQuery tables matched"):
        BigQuerySourceAdapter(client).export(
            config,
            context=SourceExecutionContext(
                effective_mode="full_refresh",
                destination_uri="gcs://bucket/prefix/run",
            ),
        )


def test_extract_export_propagates_bigquery_extract_failure(base_payload):
    config = parse_config(base_payload)

    with pytest.raises(SourceError, match="extract failed"):
        BigQuerySourceAdapter(FakeBigQueryClient(fail_extract=True)).export(
            config,
            context=SourceExecutionContext(
                effective_mode="full_refresh",
                destination_uri="gcs://bucket/prefix/run",
            ),
        )


def test_select_export_propagates_bigquery_query_failure(payload_factory):
    payload = payload_factory(
        bigquery__export_strategy="select",
        bigquery__staging_dataset_id="staging",
        bigquery__staging_table_reuse=False,
        model__sql="select * from `project.dataset.orders`",
    )
    config = parse_config(payload)

    with pytest.raises(SourceError, match="query failed"):
        BigQuerySourceAdapter(FakeBigQueryClient(fail_query=True)).export(
            config,
            context=SourceExecutionContext(
                effective_mode="full_refresh",
                destination_uri="gcs://bucket/prefix/run",
            ),
        )
