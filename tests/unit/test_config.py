from __future__ import annotations

import pytest

from procedure.config import ConfigError, parse_config
from procedure.handler import effective_mode_for


def test_parse_config_defaults(base_payload):
    config = parse_config(base_payload)

    assert config.source_type == "bigquery"
    assert config.bigquery.export_strategy == "extract"
    assert config.internal_relation.identifier == "__orders"
    assert config.iceberg_table.external_volume == "ICEBERG_EXTERNAL_VOLUME"


@pytest.mark.parametrize(
    ("updates", "message"),
    [
        ({"source_type": "postgres"}, "source_type"),
        ({"materialization_strategy": "merge"}, "materialization_strategy"),
        ({"incremental_strategy": "append"}, "incremental_strategy"),
        ({"bigquery__export_strategy": "load"}, "bigquery_export_strategy"),
        ({"bigquery__export_predicate_type": "having"}, "bigquery_export_predicate_type"),
        ({"iceberg_table__iceberg_version": 1}, "iceberg_table_iceberg_version"),
        ({"iceberg_table__storage_serialization_policy": "FAST"}, "storage_serialization_policy"),
        ({"partition_by": ["event_date"]}, "partition_by"),
        ({"cluster_by": ["event_name"]}, "cluster_by"),
    ],
)
def test_rejects_invalid_strategy_and_table_options(payload_factory, updates, message):
    payload = payload_factory(**updates)

    with pytest.raises(ConfigError, match=message):
        parse_config(payload)


def test_rejects_secret_material_in_model_config(payload_factory):
    payload = payload_factory(
        model_config={
            "google_cloud_service_account_secret_fqdn": "DB.SECRET.JSON",
        }
    )

    with pytest.raises(ConfigError, match="credential material"):
        parse_config(payload)


def test_select_requires_staging_dataset(payload_factory):
    payload = payload_factory(bigquery__export_strategy="select", model__sql="select 1")

    with pytest.raises(ConfigError, match="bigquery_staging_dataset_id"):
        parse_config(payload)


def test_rejects_disabled_change_tracking_for_iceberg_v3(payload_factory):
    payload = payload_factory(iceberg_table__change_tracking=False)

    with pytest.raises(ConfigError, match="change_tracking"):
        parse_config(payload)


def test_rejects_error_logging_for_copy_into(payload_factory):
    payload = payload_factory(iceberg_table__error_logging=True)

    with pytest.raises(ConfigError, match="error_logging"):
        parse_config(payload)


def test_incremental_predicates_must_pair_with_snowflake_predicate(payload_factory):
    payload = payload_factory(bigquery__incremental_predicates=["_PARTITIONDATE = '2026-01-01'"])

    with pytest.raises(ConfigError, match="both present or both absent"):
        parse_config(payload)


def test_incremental_predicate_must_pair_with_bigquery_predicates(payload_factory):
    payload = payload_factory(incremental_predicate='"event_date" = \'2026-01-01\'')

    with pytest.raises(ConfigError, match="both present or both absent"):
        parse_config(payload)


@pytest.mark.parametrize(
    ("updates", "message"),
    [
        (
            {
                "bigquery__export_strategy": "select",
                "bigquery__staging_dataset_id": "staging",
                "bigquery__export_predicate_type": "table_suffix",
                "model__sql": "select 1",
            },
            "select export strategy",
        ),
        (
            {
                "bigquery__export_strategy": "select",
                "bigquery__staging_dataset_id": "staging",
                "bigquery__export_predicate_type": "partition_decorator",
                "model__sql": "select 1",
            },
            "select export strategy",
        ),
        ({"bigquery__export_predicate_type": "where"}, "extract export strategy"),
        ({"model__sql": "select 1"}, "model SQL"),
    ],
)
def test_rejects_incompatible_bigquery_strategy_combinations(
    payload_factory, updates, message
):
    payload = payload_factory(**updates)

    with pytest.raises(ConfigError, match=message):
        parse_config(payload)


def test_rejects_user_or_table_stage_export_location(payload_factory):
    payload = payload_factory(bigquery__export_location="@~/exports")

    with pytest.raises(ConfigError, match="named Snowflake stage"):
        parse_config(payload)


@pytest.mark.parametrize(
    ("export_location", "message"),
    [
        ("gs://bucket/prefix", "stage|@"),
        ("", "export_location is required"),
        ("@%TABLE/prefix", "stage|@"),
    ],
)
def test_rejects_invalid_export_locations(payload_factory, export_location, message):
    payload = payload_factory(bigquery__export_location=export_location)

    with pytest.raises(ConfigError, match=message):
        parse_config(payload)


@pytest.mark.parametrize(
    ("path", "message"),
    [
        (("target_relation", "database"), "target_relation.database"),
        (("model", "unique_id"), "model.unique_id"),
        (("bigquery", "project_id"), "bigquery.project_id"),
        (("bigquery", "table_id"), "bigquery.table_id"),
        (("iceberg_table", "external_volume"), "iceberg_table.external_volume"),
    ],
)
def test_rejects_missing_required_payload_fields(base_payload, path, message):
    payload = dict(base_payload)
    current = payload
    for key in path[:-1]:
        current = current[key]
    current[path[-1]] = ""

    with pytest.raises(ConfigError, match=message):
        parse_config(payload)


def test_effective_mode_prefers_full_refresh_flag(payload_factory):
    config = parse_config(payload_factory(dbt_full_refresh=True))

    assert effective_mode_for(config, table_exists=True) == "full_refresh"


def test_effective_mode_prefers_materialization_full_refresh(payload_factory):
    config = parse_config(payload_factory(materialization_strategy="full_refresh"))

    assert effective_mode_for(config, table_exists=True) == "full_refresh"


def test_effective_mode_full_refresh_when_table_missing(base_payload):
    config = parse_config(base_payload)

    assert effective_mode_for(config, table_exists=False) == "full_refresh"
    assert effective_mode_for(config, table_exists=True) == "incremental"


def test_predicates_are_selected_by_effective_mode(payload_factory):
    config = parse_config(
        payload_factory(
            bigquery__full_refresh_predicates=["20260101"],
            bigquery__incremental_predicates=["20260102"],
            incremental_predicate='"event_date" = \'2026-01-02\'',
        )
    )

    assert config.predicates_for_mode("full_refresh") == ("20260101",)
    assert config.predicates_for_mode("incremental") == ("20260102",)
