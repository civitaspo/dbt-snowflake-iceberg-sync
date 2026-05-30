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


def test_rejects_secret_material_in_model_config(payload_factory):
    payload = payload_factory(
        model_config={
            "google_cloud_service_account_secret_fqdn": "DB.SECRET.JSON",
        }
    )

    with pytest.raises(ConfigError, match="credential material"):
        parse_config(payload)


def test_rejects_partition_by(payload_factory):
    payload = payload_factory(partition_by=["event_date"])

    with pytest.raises(ConfigError, match="partition_by"):
        parse_config(payload)


def test_select_requires_staging_dataset(payload_factory):
    payload = payload_factory(bigquery__export_strategy="select", model__sql="select 1")

    with pytest.raises(ConfigError, match="bigquery_staging_dataset_id"):
        parse_config(payload)


def test_rejects_invalid_storage_serialization_policy(payload_factory):
    payload = payload_factory(iceberg_table__storage_serialization_policy="FAST")

    with pytest.raises(ConfigError, match="storage_serialization_policy"):
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


def test_rejects_user_or_table_stage_export_location(payload_factory):
    payload = payload_factory(bigquery__export_location="@~/exports")

    with pytest.raises(ConfigError, match="named Snowflake stage"):
        parse_config(payload)


def test_effective_mode_prefers_full_refresh_flag(payload_factory):
    config = parse_config(payload_factory(dbt_full_refresh=True))

    assert effective_mode_for(config, table_exists=True) == "full_refresh"


def test_effective_mode_full_refresh_when_table_missing(base_payload):
    config = parse_config(base_payload)

    assert effective_mode_for(config, table_exists=False) == "full_refresh"
    assert effective_mode_for(config, table_exists=True) == "incremental"
