"""Snowflake session wrapper used by the procedure handler."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

from .config import RelationConfig
from .errors import ConfigError, SnowflakeExecutionError
from .schema import SnowflakeColumn, columns_from_snowflake_describe
from .sql import (
    alter_table_add_columns_sql,
    copy_into_sql,
    create_iceberg_table_sql,
    create_run_log_table_sql,
    delete_sql,
    desc_stage_sql,
    insert_run_log_sql,
    relation_sql,
)
from .utils import quote_fqn, quote_stage_fqn, sql_string


@dataclass(frozen=True)
class StageLocation:
    stage_fqn: str
    stage_path: str
    run_stage_location: str
    gcs_run_uri: str


class SnowflakeClient:
    def __init__(self, session: Any):
        self.session = session
        self.query_ids: list[str] = []

    def execute(self, statement: str) -> list[Any]:
        try:
            result = self.session.sql(statement)
            rows = result.collect()
            query_id = _query_id_from_result(result)
            if query_id:
                self.query_ids.append(query_id)
            return list(rows)
        except Exception as exc:  # pragma: no cover - exercised through mocks
            raise SnowflakeExecutionError(str(exc)) from exc

    def table_exists(self, relation: RelationConfig) -> bool:
        rows = self.execute(
            "SELECT COUNT(*) AS TABLE_COUNT "
            f"FROM {quote_fqn(relation.database, 'INFORMATION_SCHEMA', 'TABLES')} "
            f"WHERE TABLE_SCHEMA = {sql_string(relation.schema)} "
            f"AND TABLE_NAME = {sql_string(relation.identifier)}"
        )
        if not rows:
            return False
        value = _first_value(rows[0])
        return int(value or 0) > 0

    def describe_table(self, relation: RelationConfig) -> list[SnowflakeColumn]:
        return columns_from_snowflake_describe(
            self.execute(f"DESCRIBE TABLE {relation_sql(relation)}")
        )

    def create_iceberg_table(self, config: Any, columns: list[SnowflakeColumn]) -> None:
        self.execute(create_iceberg_table_sql(config, columns))

    def add_columns(self, relation: RelationConfig, columns: list[SnowflakeColumn]) -> None:
        for statement in alter_table_add_columns_sql(relation, columns):
            self.execute(statement)

    def resolve_stage_location(self, export_location: str, run_id: str) -> StageLocation:
        stage_fqn, stage_path = parse_stage_location(export_location)
        rows = self.execute(desc_stage_sql(stage_fqn))
        url = _stage_url(rows)
        if not url.startswith("gcs://"):
            raise ConfigError(
                "bigquery_export_location must reference a Snowflake stage backed by GCS"
            )
        run_path = "/".join(part.strip("/") for part in (stage_path, run_id) if part.strip("/"))
        run_stage_location = f"@{stage_fqn}/{run_path}" if run_path else f"@{stage_fqn}"
        export_url = "gs://" + url.removeprefix("gcs://")
        gcs_run_uri = "/".join(
            part.strip("/") for part in (export_url, run_path) if part.strip("/")
        )
        if not gcs_run_uri.startswith("gs://"):
            gcs_run_uri = "gs://" + gcs_run_uri.removeprefix("gs:/").lstrip("/")
        return StageLocation(
            stage_fqn=stage_fqn,
            stage_path=stage_path,
            run_stage_location=run_stage_location,
            gcs_run_uri=gcs_run_uri,
        )

    def begin(self) -> None:
        self.execute("BEGIN")

    def commit(self) -> None:
        self.execute("COMMIT")

    def rollback(self) -> None:
        self.execute("ROLLBACK")

    def delete_from_iceberg(self, relation: RelationConfig, predicate: str | None) -> None:
        self.execute(delete_sql(relation, predicate))

    def copy_into_iceberg(self, relation: RelationConfig, stage_run_location: str) -> None:
        self.execute(copy_into_sql(relation, stage_run_location))

    def ensure_run_log(self, relation: RelationConfig | None) -> None:
        if relation is not None:
            self.execute(create_run_log_table_sql(relation))

    def write_run_log(self, relation: RelationConfig | None, payload: dict[str, Any]) -> None:
        if relation is not None:
            self.execute(insert_run_log_sql(relation, payload))


def parse_stage_location(export_location: str) -> tuple[str, str]:
    if not export_location.startswith("@"):
        raise ConfigError("bigquery_export_location must start with @")
    raw = export_location[1:]
    if not raw or raw.startswith(("~", "%")):
        raise ConfigError(
            "bigquery_export_location must be a named Snowflake stage, not a user or table stage"
        )
    if "/" in raw:
        stage_raw, stage_path = raw.split("/", 1)
    else:
        stage_raw, stage_path = raw, ""
    raw_parts = stage_raw.split(".")
    if any(part == "" for part in raw_parts):
        raise ConfigError("bigquery_export_location contains an invalid stage name")
    stage_parts = [part.strip('"') for part in raw_parts]
    if any(part == "" for part in stage_parts) or not 1 <= len(stage_parts) <= 3:
        raise ConfigError("bigquery_export_location contains an invalid stage name")
    return quote_stage_fqn(stage_parts), stage_path.strip("/")


def _stage_url(rows: list[Any]) -> str:
    for row in rows:
        data = _row_to_mapping(row)
        key = str(
            data.get("property") or data.get("PROPERTY") or data.get("name") or data.get("NAME")
        )
        value = (
            data.get("property_value")
            or data.get("PROPERTY_VALUE")
            or data.get("value")
            or data.get("VALUE")
        )
        if key.upper() == "URL" and value:
            return _normalize_stage_url(value)
    raise ConfigError("DESC STAGE did not return a URL property")


def _normalize_stage_url(value: Any) -> str:
    text = str(value).strip()
    if text.startswith("["):
        try:
            urls = json.loads(text)
        except json.JSONDecodeError:
            urls = []
        if urls:
            text = str(urls[0]).strip()
    return text.rstrip("/")


def _query_id_from_result(result: Any) -> str | None:
    data = getattr(result, "__dict__", {})
    for attr in ("query_id", "queryId", "sfqid"):
        value = data.get(attr)
        if value:
            return str(value)
    return None


def _first_value(row: Any) -> Any:
    if isinstance(row, dict):
        return next(iter(row.values()))
    if hasattr(row, "as_dict"):
        data = row.as_dict()
        return next(iter(data.values()))
    if hasattr(row, "asDict"):
        data = row.asDict()
        return next(iter(data.values()))
    match = re.search(r"[-+]?\d+", str(row))
    return match.group(0) if match else None


def _row_to_mapping(row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        return row
    if hasattr(row, "as_dict"):
        return row.as_dict()
    if hasattr(row, "asDict"):
        return row.asDict()
    return {
        key: getattr(row, key)
        for key in dir(row)
        if not key.startswith("_") and not callable(getattr(row, key))
    }
