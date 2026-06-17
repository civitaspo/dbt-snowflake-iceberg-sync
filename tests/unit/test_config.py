from __future__ import annotations

import pytest

from procedure.config import ConfigError, parse_config
from procedure.handler import effective_mode_for


def test_parse_config_defaults(base_payload):
    config = parse_config(base_payload)

    assert config.source_type == "bigquery"
    assert config.bigquery.export_strategy == "extract"
    assert config.bigquery.export_compression == "ZSTD"
    assert config.bigquery.export_poll_interval_seconds == 30
    assert config.bigquery.export_poll_timeout_seconds == 3600
    assert config.target_relation.identifier == "ORDERS"
    assert config.internal_relation.identifier == "__ORDERS"
    assert config.iceberg_table.external_volume == "ICEBERG_EXTERNAL_VOLUME"
    assert config.retry.max_attempts == 3
    assert config.cleanup.created_table_on_failure is True
    assert config.run_log.fail_on_error is False


def test_parse_config_normalizes_only_snowflake_object_identifiers(payload_factory):
    payload = payload_factory(
        target_relation__database="analytics",
        target_relation__schema="public",
        target_relation__identifier="orders",
        internal_relation__database="analytics",
        internal_relation__schema="public",
        internal_relation__identifier="__orders",
        deployment__procedure_database="analytics",
        deployment__procedure_schema="util",
        deployment__procedure_name="iceberg_sync",
        deployment__run_log_table={
            "database": "analytics",
            "schema": "util",
            "identifier": "iceberg_sync_run_log",
        },
        bigquery__project_id="my-project",
        bigquery__dataset_id="mixed_Case_dataset",
        bigquery__table_id="Events_*",
    )

    config = parse_config(payload)

    assert config.target_relation.fqn == "ANALYTICS.PUBLIC.ORDERS"
    assert config.internal_relation.fqn == "ANALYTICS.PUBLIC.__ORDERS"
    assert config.deployment.procedure_database == "ANALYTICS"
    assert config.deployment.procedure_schema == "UTIL"
    assert config.deployment.procedure_name == "ICEBERG_SYNC"
    assert config.deployment.run_log_table is not None
    assert config.deployment.run_log_table.fqn == "ANALYTICS.UTIL.ICEBERG_SYNC_RUN_LOG"
    assert config.bigquery.project_id == "my-project"
    assert config.bigquery.dataset_id == "mixed_Case_dataset"
    assert config.bigquery.table_id == "Events_*"


@pytest.mark.parametrize(
    ("updates", "message"),
    [
        ({"source_type": "postgres"}, "source_type"),
        ({"materialization_strategy": "merge"}, "materialization_strategy"),
        ({"incremental_strategy": "append"}, "incremental_strategy"),
        ({"bigquery__export_strategy": "load"}, "bigquery_export_strategy"),
        ({"bigquery__export_compression": "brotli"}, "bigquery_export_compression"),
        ({"bigquery__export_predicate_type": "having"}, "bigquery_export_predicate_type"),
        ({"bigquery__export_poll_interval_seconds": 0}, "export_poll_interval"),
        ({"bigquery__export_poll_timeout_seconds": 0}, "export_poll_timeout"),
        (
            {
                "bigquery__export_poll_interval_seconds": 10,
                "bigquery__export_poll_timeout_seconds": 5,
            },
            "export_poll_interval",
        ),
        ({"iceberg_table__iceberg_version": 1}, "iceberg_table_iceberg_version"),
        ({"iceberg_table__storage_serialization_policy": "FAST"}, "storage_serialization_policy"),
        ({"retry__max_attempts": 0}, "iceberg_sync_retry_max_attempts"),
        ({"retry__initial_delay_seconds": -1}, "initial_delay"),
        ({"retry__max_delay_seconds": -1}, "max_delay"),
        ({"retry__backoff_multiplier": 0.9}, "backoff"),
        ({"retry__jitter_seconds": -1}, "jitter"),
        ({"cleanup__created_table_on_failure": "not-bool"}, "cleanup_created_table"),
        ({"run_log__fail_on_error": "not-bool"}, "run_log_fail_on_error"),
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


def test_select_requires_model_sql(payload_factory):
    payload = payload_factory(
        bigquery__export_strategy="select",
        bigquery__staging_dataset_id="staging",
        model__sql="",
    )

    with pytest.raises(ConfigError, match="model SQL is required"):
        parse_config(payload)


def test_string_boolean_values_are_coerced(payload_factory):
    payload = payload_factory(
        dbt_full_refresh="yes",
        bigquery__staging_table_reuse="false",
        bigquery__force_rebuild_staging_table="true",
        iceberg_table__copy_grants="1",
        iceberg_table__enable_data_compaction="0",
        run_log__fail_on_error="true",
    )

    config = parse_config(payload)

    assert config.dbt_full_refresh is True
    assert config.bigquery.staging_table_reuse is False
    assert config.bigquery.force_rebuild_staging_table is True
    assert config.iceberg_table.copy_grants is True
    assert config.iceberg_table.enable_data_compaction is False
    assert config.run_log.fail_on_error is True


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("none", "NONE"),
        ("SNAPPY", "SNAPPY"),
        ("gzip", "GZIP"),
        ("zstd", "ZSTD"),
    ],
)
def test_bigquery_export_compression_is_normalized(payload_factory, value, expected):
    config = parse_config(payload_factory(bigquery__export_compression=value))

    assert config.bigquery.export_compression == expected


def _strategy_config_matrix_cases():
    for export_strategy in ("extract", "select"):
        for predicate_type in (
            "auto",
            "none",
            "partition_decorator",
            "table_suffix",
            "where",
        ):
            for has_model_sql in (False, True):
                for has_staging_dataset in (False, True):
                    yield pytest.param(
                        export_strategy,
                        predicate_type,
                        has_model_sql,
                        has_staging_dataset,
                        id=(
                            f"{export_strategy}-{predicate_type}-"
                            f"{'sql' if has_model_sql else 'no_sql'}-"
                            f"{'staging' if has_staging_dataset else 'no_staging'}"
                        ),
                    )


@pytest.mark.parametrize(
    (
        "export_strategy",
        "predicate_type",
        "has_model_sql",
        "has_staging_dataset",
    ),
    list(_strategy_config_matrix_cases()),
)
def test_bigquery_strategy_config_matrix(
    payload_factory,
    export_strategy,
    predicate_type,
    has_model_sql,
    has_staging_dataset,
):
    payload = payload_factory(
        bigquery__export_strategy=export_strategy,
        bigquery__export_predicate_type=predicate_type,
        bigquery__staging_dataset_id=("staging" if has_staging_dataset else None),
        model__sql=("select * from `project.dataset.orders`" if has_model_sql else ""),
    )
    expected_error = _expected_strategy_config_error(
        export_strategy,
        predicate_type,
        has_model_sql,
        has_staging_dataset,
    )

    if expected_error:
        with pytest.raises(ConfigError, match=expected_error):
            parse_config(payload)
    else:
        config = parse_config(payload)
        assert config.bigquery.export_strategy == export_strategy
        assert config.bigquery.export_predicate_type == predicate_type


def _expected_strategy_config_error(
    export_strategy,
    predicate_type,
    has_model_sql,
    has_staging_dataset,
):
    if export_strategy == "extract":
        if predicate_type == "where":
            return "extract export strategy"
        if has_model_sql:
            return "model SQL"
        return None
    if not has_staging_dataset:
        return "bigquery_staging_dataset_id"
    if not has_model_sql:
        return "model SQL"
    if predicate_type in {"partition_decorator", "table_suffix"}:
        return "select export strategy"
    return None


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

    assert effective_mode_for(
        config,
        internal_table_exists=True,
        target_view_exists=True,
    ) == "full_refresh"


def test_effective_mode_prefers_materialization_full_refresh(payload_factory):
    config = parse_config(payload_factory(materialization_strategy="full_refresh"))

    assert effective_mode_for(
        config,
        internal_table_exists=True,
        target_view_exists=True,
    ) == "full_refresh"


def test_effective_mode_full_refresh_when_table_missing(base_payload):
    config = parse_config(base_payload)

    assert (
        effective_mode_for(
            config,
            internal_table_exists=False,
            target_view_exists=True,
        )
        == "full_refresh"
    )
    assert (
        effective_mode_for(
            config,
            internal_table_exists=True,
            target_view_exists=True,
        )
        == "incremental"
    )


def test_effective_mode_full_refresh_when_target_view_missing(base_payload):
    config = parse_config(base_payload)

    assert (
        effective_mode_for(
            config,
            internal_table_exists=True,
            target_view_exists=False,
        )
        == "full_refresh"
    )


@pytest.mark.parametrize(
    (
        "materialization_strategy",
        "dbt_full_refresh",
        "internal_table_exists",
        "target_view_exists",
        "expected",
    ),
    [
        ("full_refresh", False, False, False, "full_refresh"),
        ("full_refresh", False, False, True, "full_refresh"),
        ("full_refresh", False, True, False, "full_refresh"),
        ("full_refresh", False, True, True, "full_refresh"),
        ("full_refresh", True, False, False, "full_refresh"),
        ("full_refresh", True, False, True, "full_refresh"),
        ("full_refresh", True, True, False, "full_refresh"),
        ("full_refresh", True, True, True, "full_refresh"),
        ("incremental", False, False, False, "full_refresh"),
        ("incremental", False, False, True, "full_refresh"),
        ("incremental", False, True, False, "full_refresh"),
        ("incremental", False, True, True, "incremental"),
        ("incremental", True, False, False, "full_refresh"),
        ("incremental", True, False, True, "full_refresh"),
        ("incremental", True, True, False, "full_refresh"),
        ("incremental", True, True, True, "full_refresh"),
    ],
)
def test_effective_mode_matrix(
    payload_factory,
    materialization_strategy,
    dbt_full_refresh,
    internal_table_exists,
    target_view_exists,
    expected,
):
    config = parse_config(
        payload_factory(
            materialization_strategy=materialization_strategy,
            dbt_full_refresh=dbt_full_refresh,
        )
    )

    assert (
        effective_mode_for(
            config,
            internal_table_exists=internal_table_exists,
            target_view_exists=target_view_exists,
        )
        == expected
    )


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
