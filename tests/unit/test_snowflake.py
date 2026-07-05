from __future__ import annotations

import pytest

from procedure.errors import ConfigError
from procedure.snowflake import (
    SnowflakeClient,
    _query_id_from_result,
    _stage_url,
    parse_stage_location,
)


class FakeResult:
    def __init__(self, rows=None, query_id=None):
        self.rows = (
            rows
            if rows is not None
            else [
                {
                    "property": "URL",
                    "property_value": '["gcs://bucket/base/"]',
                }
            ]
        )
        if query_id:
            self.query_id = query_id

    def collect(self):
        return self.rows


class StaticSession:
    def __init__(self, rows):
        self.rows = rows

    def sql(self, statement):
        return FakeResult(self.rows)


class QueryIdSession:
    def sql(self, statement):
        return FakeResult([{"ok": 1}], query_id="query-123")


class FakeSession:
    def sql(self, statement):
        return FakeResult()


def test_parse_stage_location_rejects_missing_stage_name():
    with pytest.raises(ConfigError, match="named Snowflake stage"):
        parse_stage_location("@")


def test_parse_stage_location_rejects_too_many_stage_parts():
    with pytest.raises(ConfigError, match="invalid stage name"):
        parse_stage_location("@A.B.C.D/path")


def test_parse_stage_location_rejects_empty_stage_qualifier():
    with pytest.raises(ConfigError, match="invalid stage name"):
        parse_stage_location("@A..STAGE/path")


def test_parse_stage_location_rejects_empty_quoted_stage_part():
    with pytest.raises(ConfigError, match="invalid stage name"):
        parse_stage_location('@DB.""."STAGE"/path')


def test_parse_stage_location_supports_one_two_and_three_part_stage_names():
    assert parse_stage_location("@STAGE/path") == ('"STAGE"', "path")
    assert parse_stage_location("@SCHEMA.STAGE/path") == ('"SCHEMA"."STAGE"', "path")
    assert parse_stage_location("@DB.SCHEMA.STAGE/path") == (
        '"DB"."SCHEMA"."STAGE"',
        "path",
    )


def test_parse_stage_location_strips_redundant_path_slashes():
    assert parse_stage_location("@DB.SCHEMA.STAGE//a/b//") == (
        '"DB"."SCHEMA"."STAGE"',
        "a/b",
    )


def test_parse_stage_location_rejects_user_stage():
    with pytest.raises(ConfigError, match="named Snowflake stage"):
        parse_stage_location("@~/exports")


def test_parse_stage_location_rejects_table_stage():
    with pytest.raises(ConfigError, match="named Snowflake stage"):
        parse_stage_location("@%MY_TABLE/exports")


def test_parse_stage_location_quotes_named_stage():
    stage_fqn, stage_path = parse_stage_location("@analytics.public.export_stage/orders")

    assert stage_fqn == '"ANALYTICS"."PUBLIC"."EXPORT_STAGE"'
    assert stage_path == "orders"


def test_stage_url_accepts_desc_stage_url_array():
    assert (
        _stage_url(
            [
                {
                    "property": "URL",
                    "property_value": '["gcs://bucket/path/"]',
                }
            ]
        )
        == "gcs://bucket/path"
    )


def test_stage_url_rejects_missing_url_property():
    with pytest.raises(ConfigError, match="URL property"):
        _stage_url([{"property": "DIRECTORY", "property_value": "true"}])


def test_resolve_stage_location_uses_gs_uri_for_bigquery_export():
    location = SnowflakeClient(FakeSession()).resolve_stage_location(
        "@ANALYTICS.PUBLIC.EXPORT_STAGE/prefix",
        "run-id",
    )

    assert location.run_stage_location == '@"ANALYTICS"."PUBLIC"."EXPORT_STAGE"/prefix/run-id'
    assert location.gcs_run_uri == "gs://bucket/base/prefix/run-id"


def test_resolve_stage_location_rejects_non_gcs_stage():
    session = StaticSession([{"property": "URL", "property_value": '["s3://bucket/base/"]'}])

    with pytest.raises(ConfigError, match="backed by GCS"):
        SnowflakeClient(session).resolve_stage_location(
            "@ANALYTICS.PUBLIC.EXPORT_STAGE/prefix",
            "run-id",
        )


def test_execute_tracks_query_ids():
    client = SnowflakeClient(QueryIdSession())

    assert client.execute("select 1") == [{"ok": 1}]
    assert client.query_ids == ["query-123"]


def test_query_id_lookup_does_not_call_dynamic_getattr():
    class SnowparkLikeResult:
        def __getattr__(self, name):
            raise AssertionError("dynamic getattr should not be called")

    assert _query_id_from_result(SnowparkLikeResult()) is None
