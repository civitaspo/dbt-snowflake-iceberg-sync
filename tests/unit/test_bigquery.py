import pytest

from procedure.config import IcebergSyncConfig
from procedure.errors import ConfigError, SourceError
from procedure.sources.bigquery import (
    BigQuerySource,
    ExportPlan,
    ExportSegment,
    ExportResult,
    resolve_export_predicate_type,
    select_sql_with_predicates,
    staging_table_hash,
    validate_table_suffix_source,
)

from .test_config import base_config


def test_select_auto_predicates_resolve_to_where():
    assert (
        resolve_export_predicate_type("select", "auto", "events", ["event_date = current_date"], None)
        == "where"
    )


def test_select_auto_without_predicates_resolves_to_none():
    assert resolve_export_predicate_type("select", "auto", "events", [], None) == "none"


def test_extract_auto_wildcard_resolves_to_table_suffix():
    assert resolve_export_predicate_type("extract", "auto", "events_*", ["20260101"], None) == "table_suffix"


def test_extract_auto_predicates_on_partitioned_table_resolve_to_decorator():
    assert (
        resolve_export_predicate_type("extract", "auto", "events", ["20260101"], True)
        == "partition_decorator"
    )


def test_partition_decorator_requires_partitioned_concrete_table():
    with pytest.raises(ConfigError, match="concrete"):
        resolve_export_predicate_type("extract", "partition_decorator", "events_*", ["20260101"], None)

    with pytest.raises(ConfigError, match="native BigQuery partitioned"):
        resolve_export_predicate_type("extract", "partition_decorator", "events", ["20260101"], False)


def test_extract_where_is_rejected():
    with pytest.raises(ConfigError, match="where"):
        resolve_export_predicate_type("extract", "where", "events", ["x"], True)


def test_table_suffix_requires_final_wildcard():
    with pytest.raises(ConfigError, match="ending with '_\\*'"):
        validate_table_suffix_source("events*")


def test_select_sql_wraps_predicates_with_or():
    sql = select_sql_with_predicates("select * from `p.d.t`", ["a = 1", "b = 2"])

    assert "FROM (" in sql
    assert "WHERE (a = 1) OR (b = 2)" in sql


def test_staging_hash_changes_with_target_identity():
    first = IcebergSyncConfig.from_dict(
        base_config(
            bigquery_export_strategy="select",
            bigquery_staging_dataset_id="staging",
            model_sql="select * from `p.d.t`",
        )
    )
    second_payload = base_config(
        bigquery_export_strategy="select",
        bigquery_staging_dataset_id="staging",
        model_sql="select * from `p.d.t`",
    )
    second_payload["target_relation"]["identifier"] = "OTHER"
    second = IcebergSyncConfig.from_dict(second_payload)

    assert staging_table_hash(first, []) != staging_table_hash(second, [])


class FakeSelectClient:
    def __init__(self, existing_table=None):
        self.existing_table = existing_table
        self.queried = False
        self.patched = False

    def get_table(self, project_id, dataset_id, table_id):
        if self.existing_table is None:
            raise SourceError("missing table")
        return self.existing_table

    def query_to_table(self, project_id, dataset_id, table_id, sql, location):
        self.queried = True
        return {"jobReference": {"projectId": project_id, "location": location, "jobId": "query_job"}}

    def patch_table(self, project_id, dataset_id, table_id, body):
        self.patched = True
        self.existing_table = {
            "schema": {"fields": [{"name": "id", "type": "INT64"}]},
            "labels": body["labels"],
            "expirationTime": body["expirationTime"],
        }


def test_select_staging_table_rebuild_records_query_job():
    payload = base_config(
        bigquery_export_strategy="select",
        bigquery_staging_dataset_id="staging",
        model_sql="select * from `p.d.t`",
    )
    config = IcebergSyncConfig.from_dict(payload)
    client = FakeSelectClient()
    source = BigQuerySource(client)

    plan = source.plan_export(config, "full_refresh", "gs://bucket/prefix")

    assert client.queried is True
    assert client.patched is True
    assert plan.setup_job_references == [{"projectId": "project", "location": "US", "jobId": "query_job"}]


def test_export_result_combines_setup_and_extract_job_references():
    plan = ExportPlan(
        predicate_type="none",
        predicates=[],
        segments=[
            ExportSegment(
                source_project_id="project",
                source_dataset_id="dataset",
                source_table_id="table",
                destination_uri="gs://bucket/prefix/*.parquet",
            )
        ],
        schema={"fields": []},
        setup_job_references=[{"jobId": "query_job"}],
    )
    result = ExportResult(plan=plan, job_references=[{"jobId": "extract_job"}])

    assert result.as_dict()["job_references"] == [{"jobId": "query_job"}, {"jobId": "extract_job"}]
