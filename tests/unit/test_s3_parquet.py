from __future__ import annotations

import pytest

from procedure.config import ConfigError, parse_config
from procedure.errors import SourceError
from procedure.snowflake import StageFile, StageLocation
from procedure.sources.base import SourceExecutionContext
from procedure.sources.s3_parquet import S3ParquetSourceAdapter
from procedure.sql import copy_into_sql, create_parquet_file_format_sql, infer_schema_sql


class FakeSnowflake:
    def __init__(self, *, listed_files=None, schema_fields=None):
        self.listed_files = listed_files or []
        self.schema_fields = schema_fields or [
            {
                "COLUMN_NAME": "OrderID",
                "TYPE": "NUMBER(19,0)",
                "NULLABLE": False,
                "ORDER_ID": 1,
            },
            {
                "COLUMN_NAME": "CustomerName",
                "TYPE": "TEXT",
                "NULLABLE": True,
                "ORDER_ID": 2,
            },
        ]
        self.query_ids = ["list-qid", "infer-qid"]
        self.calls = []

    def resolve_stage_location(self, export_location, run_id=None, **kwargs):
        self.calls.append(("resolve", export_location, run_id, kwargs))
        return StageLocation(
            stage_fqn='"ANALYTICS"."PUBLIC"."S3_EXPORT_STAGE"',
            stage_path="orders",
            run_stage_location='@"ANALYTICS"."PUBLIC"."S3_EXPORT_STAGE"/orders',
            remote_run_uri="s3://bucket/orders",
            stage_url="s3://bucket",
        )

    def list_stage_files(self, stage_location):
        self.calls.append(("list", stage_location))
        return list(self.listed_files)

    def infer_parquet_schema(self, *, location, file_format, files=None):
        self.calls.append(("infer", location, file_format, files))
        return list(self.schema_fields)


def test_parse_s3_parquet_config_defaults(s3_parquet_payload):
    config = parse_config(s3_parquet_payload)

    assert config.source_type == "s3_parquet"
    assert config.bigquery is None
    assert config.s3_parquet is not None
    assert config.s3_parquet.location == "@ANALYTICS.PUBLIC.S3_EXPORT_STAGE/orders"
    assert config.s3_parquet.full_refresh_paths == ("",)
    assert config.predicates_for_mode("incremental") == ("",)


def test_s3_parquet_requires_location(s3_payload_factory):
    payload = s3_payload_factory(s3_parquet__location="")

    with pytest.raises(ConfigError, match="s3_parquet.location"):
        parse_config(payload)


def test_s3_parquet_rejects_model_sql(s3_payload_factory):
    payload = s3_payload_factory(model__sql="select 1")

    with pytest.raises(ConfigError, match="model SQL is not supported"):
        parse_config(payload)


def test_s3_parquet_pairs_incremental_paths_with_predicate(s3_payload_factory):
    payload = s3_payload_factory(s3_parquet__incremental_paths=["dt=2026-01-01"])

    with pytest.raises(ConfigError, match="incremental_predicate"):
        parse_config(payload)


def test_s3_parquet_rejects_absolute_path_suffixes(s3_payload_factory):
    payload = s3_payload_factory(s3_parquet__full_refresh_paths=["s3://bucket/path"])

    with pytest.raises(ConfigError, match="relative"):
        parse_config(payload)


def test_s3_parquet_rejects_aws_credentials_in_model_config(s3_payload_factory):
    payload = s3_payload_factory(model_config={"aws_access_key_id": "AKIA..."})

    with pytest.raises(ConfigError, match="credential material"):
        parse_config(payload)


def test_s3_adapter_lists_infers_and_returns_schema(s3_parquet_payload):
    snowflake = FakeSnowflake(
        listed_files=[
            StageFile(
                name="s3://bucket/orders/part-000.parquet",
                size=10,
                last_modified="2026-01-02",
            ),
            StageFile(
                name="s3://bucket/orders/part-001.parquet",
                size=20,
                last_modified="2026-01-03",
            ),
        ]
    )
    adapter = S3ParquetSourceAdapter(snowflake)
    config = parse_config(s3_parquet_payload)

    state = adapter.start_export(
        config,
        SourceExecutionContext(effective_mode="full_refresh", destination_uri="s3://bucket/orders"),
    )

    assert state["status"] == "success"
    assert state["segments"][0]["file_count"] == 2
    assert state["load_locations"][0]["force"] is True
    columns = adapter.map_schema(
        adapter.export(
            config,
            SourceExecutionContext(
                effective_mode="full_refresh", destination_uri="s3://bucket/orders"
            ),
        )
    )
    assert [column.source_name for column in columns] == ["OrderID", "CustomerName"]
    assert columns[0].snowflake_type == "BIGINT"
    assert columns[1].snowflake_type == "VARCHAR"


def test_s3_adapter_applies_file_pattern_and_caps_infer_files(s3_payload_factory):
    payload = s3_payload_factory(
        s3_parquet__file_pattern=r".*keep.*[.]parquet",
        s3_parquet__infer_schema_max_file_count=1,
    )
    snowflake = FakeSnowflake(
        listed_files=[
            StageFile(name="s3://bucket/orders/keep-old.parquet", last_modified="2026-01-01"),
            StageFile(name="s3://bucket/orders/drop.parquet", last_modified="2026-01-02"),
            StageFile(name="s3://bucket/orders/keep-new.parquet", last_modified="2026-01-03"),
        ]
    )
    adapter = S3ParquetSourceAdapter(snowflake)

    state = adapter.start_export(
        parse_config(payload),
        SourceExecutionContext(effective_mode="full_refresh", destination_uri="s3://bucket/orders"),
    )

    infer_call = [call for call in snowflake.calls if call[0] == "infer"][0]
    assert infer_call[3] == ["keep-new.parquet"]
    assert state["segments"][0]["file_count"] == 2


def test_s3_adapter_skips_empty_location_when_configured(s3_payload_factory):
    payload = s3_payload_factory(s3_parquet__skip_missing_location=True)
    adapter = S3ParquetSourceAdapter(FakeSnowflake(listed_files=[]))

    state = adapter.start_export(
        parse_config(payload),
        SourceExecutionContext(effective_mode="full_refresh", destination_uri="s3://bucket/orders"),
    )

    assert state["status"] == "skipped"


def test_s3_adapter_fails_empty_location_by_default(s3_parquet_payload):
    adapter = S3ParquetSourceAdapter(FakeSnowflake(listed_files=[]))

    with pytest.raises(SourceError, match="no Parquet files"):
        adapter.start_export(
            parse_config(s3_parquet_payload),
            SourceExecutionContext(
                effective_mode="full_refresh", destination_uri="s3://bucket/orders"
            ),
        )


def test_s3_adapter_poll_export_is_passthrough(s3_parquet_payload):
    adapter = S3ParquetSourceAdapter(FakeSnowflake())
    state = {"status": "success", "schema_fields": []}

    assert adapter.poll_export(parse_config(s3_parquet_payload), state) is state


def test_copy_into_sql_emits_pattern_and_force(s3_parquet_payload):
    config = parse_config(s3_parquet_payload)
    sql = copy_into_sql(
        config.internal_relation,
        '@"ANALYTICS"."PUBLIC"."S3_EXPORT_STAGE"/orders',
        pattern=r".*[.]parquet",
        force=True,
    )

    assert "PATTERN = '.*[.]parquet'" in sql
    assert "FORCE = TRUE" in sql


def test_infer_schema_and_file_format_sql_renderers():
    sql = infer_schema_sql(
        location="@STAGE/path",
        file_format='"ANALYTICS"."UTIL"."FMT"',
        files=["a.parquet"],
    )
    assert "KIND => 'ICEBERG'" in sql
    assert "FILE_FORMAT => \"ANALYTICS\".\"UTIL\".\"FMT\"" in sql
    assert "FILE_FORMAT => '\"ANALYTICS\".\"UTIL\".\"FMT\"'" not in sql
    assert "USE_VECTORIZED_SCANNER = TRUE" in create_parquet_file_format_sql(
        '"ANALYTICS"."UTIL"."FMT"'
    )


def test_parse_declared_s3_parquet_columns(s3_payload_factory):
    payload = s3_payload_factory(
        s3_parquet__columns=[
            {
                "name": "OrderID",
                "type": "BIGINT",
                "nullable": False,
                "alias": "order_id",
            },
            {
                "name": "AmountText",
                "type": "VARCHAR",
                "expression": 'TRY_TO_NUMBER("AmountText")',
                "alias": "amount",
            },
        ],
        deployment__parquet_file_format=None,
    )

    config = parse_config(payload)

    assert config.s3_parquet is not None
    assert len(config.s3_parquet.columns) == 2
    assert config.s3_parquet.columns[1].expression == 'TRY_TO_NUMBER("AmountText")'
    assert config.deployment.parquet_file_format is None


def test_declared_columns_reject_empty_list(s3_payload_factory):
    payload = s3_payload_factory(s3_parquet__columns=[])

    with pytest.raises(ConfigError, match="must not be empty"):
        parse_config(payload)


def test_s3_adapter_uses_declared_columns_without_infer(s3_payload_factory):
    payload = s3_payload_factory(
        s3_parquet__columns=[
            {
                "name": "OrderID",
                "type": "BIGINT",
                "nullable": False,
                "alias": "order_id",
            },
            {
                "name": "AmountText",
                "type": "VARCHAR",
                "expression": 'TRY_TO_NUMBER("AmountText")',
                "alias": "amount",
            },
        ],
        deployment__parquet_file_format=None,
    )
    snowflake = FakeSnowflake(
        listed_files=[
            StageFile(
                name="s3://bucket/orders/part-000.parquet",
                size=10,
                last_modified="2026-01-02",
            ),
        ]
    )
    adapter = S3ParquetSourceAdapter(snowflake)
    config = parse_config(payload)

    state = adapter.start_export(
        config,
        SourceExecutionContext(effective_mode="full_refresh", destination_uri="s3://bucket/orders"),
    )

    assert state["status"] == "success"
    assert all(call[0] != "infer" for call in snowflake.calls)
    columns = adapter.map_schema(
        adapter.export(
            config,
            SourceExecutionContext(
                effective_mode="full_refresh", destination_uri="s3://bucket/orders"
            ),
        )
    )
    assert [column.source_name for column in columns] == ["OrderID", "AmountText"]
    assert columns[0].alias == "order_id"
    assert columns[1].expression == 'TRY_TO_NUMBER("AmountText")'
    assert columns[1].alias == "amount"