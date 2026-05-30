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
    def collect(self):
        return [
            {
                "property": "URL",
                "property_value": '["gcs://bucket/base/"]',
            }
        ]


class FakeSession:
    def sql(self, statement):
        return FakeResult()


def test_parse_stage_location_rejects_user_stage():
    with pytest.raises(ConfigError, match="named Snowflake stage"):
        parse_stage_location("@~/exports")


def test_parse_stage_location_rejects_table_stage():
    with pytest.raises(ConfigError, match="named Snowflake stage"):
        parse_stage_location("@%MY_TABLE/exports")


def test_parse_stage_location_quotes_named_stage():
    stage_fqn, stage_path = parse_stage_location("@ANALYTICS.PUBLIC.EXPORT_STAGE/orders")

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


def test_resolve_stage_location_uses_gs_uri_for_bigquery_export():
    location = SnowflakeClient(FakeSession()).resolve_stage_location(
        "@ANALYTICS.PUBLIC.EXPORT_STAGE/prefix",
        "run-id",
    )

    assert location.run_stage_location == '@"ANALYTICS"."PUBLIC"."EXPORT_STAGE"/prefix/run-id'
    assert location.gcs_run_uri == "gs://bucket/base/prefix/run-id"


def test_query_id_lookup_does_not_call_dynamic_getattr():
    class SnowparkLikeResult:
        def __getattr__(self, name):
            raise AssertionError("dynamic getattr should not be called")

    assert _query_id_from_result(SnowparkLikeResult()) is None
