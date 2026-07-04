import pytest

from procedure.config import IcebergSyncConfig
from procedure.schema import SnowflakeColumn
from procedure.snowflake import (
    SnowflakeClient,
    parse_named_stage_location,
    render_copy_into_sql,
    render_create_iceberg_table,
)

from .test_config import base_config


def test_parse_named_stage_location():
    assert parse_named_stage_location("@DB.SCHEMA.STAGE/prefix/path") == (
        "DB.SCHEMA.STAGE",
        "prefix/path",
    )


def test_copy_into_uses_required_add_files_options():
    sql = render_copy_into_sql('"DB"."MART"."__EVENTS"', "@DB.SCHEMA.STAGE/prefix", "run-1")

    assert "load_mode = add_files_copy" in sql
    assert "match_by_column_name = case_sensitive" in sql
    assert "purge = false" in sql
    assert "from @DB.SCHEMA.STAGE/prefix/run-1/" in sql


def test_create_iceberg_table_renders_managed_table_options():
    config = IcebergSyncConfig.from_dict(base_config(iceberg_table_copy_grants=True))
    sql = render_create_iceberg_table(config, [SnowflakeColumn("ID", "NUMBER(38,0)", "id")])

    assert "create or replace iceberg table" in sql
    assert "copy grants" in sql
    assert "external_volume = 'ICEBERG_VOLUME'" in sql
    assert "catalog = 'SNOWFLAKE'" in sql


class FakeQuery:
    def __init__(self, session, sql):
        self.session = session
        self.sql = sql

    def collect(self):
        self.session.statements.append(self.sql)
        if self.sql.startswith("copy into"):
            raise RuntimeError("copy failed")
        return []


class FakeSession:
    def __init__(self):
        self.statements = []

    def sql(self, sql):
        return FakeQuery(self, sql)


def test_load_copy_rolls_back_on_failure():
    config = IcebergSyncConfig.from_dict(base_config())
    session = FakeSession()
    client = SnowflakeClient(session)

    with pytest.raises(Exception, match="copy failed"):
        client.load_copy(config, "@DB.SCHEMA.STAGE/prefix", "run-1", "full_refresh")

    assert session.statements[0] == "begin"
    assert "rollback" in session.statements
