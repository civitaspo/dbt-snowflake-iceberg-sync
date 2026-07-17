"""Snowflake session wrapper used by the procedure handler."""

from __future__ import annotations

import json
import re
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from .config import GCS_STAGE_SCHEMES, S3_STAGE_SCHEMES, RelationConfig
from .errors import ConfigError, SnowflakeExecutionError
from .schema import SnowflakeColumn, ViewColumn, columns_from_snowflake_describe
from .sql import (
    alter_table_add_columns_sql,
    copy_into_sql,
    create_iceberg_table_sql,
    create_or_alter_run_log_table_sql,
    create_or_replace_view_sql,
    delete_sql,
    desc_stage_sql,
    drop_iceberg_table_sql,
    infer_schema_sql,
    insert_run_log_sql,
    list_files_sql,
    relation_sql,
)
from .utils import quote_fqn, quote_stage_fqn, sql_string


@dataclass(frozen=True)
class StageLocation:
    stage_fqn: str
    stage_path: str
    run_stage_location: str
    remote_run_uri: str
    stage_url: str


@dataclass(frozen=True)
class StageFile:
    name: str
    size: int | None = None
    last_modified: str | None = None


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
        return self.relation_exists(relation)

    def relation_exists(
        self, relation: RelationConfig, *, expected_type: str | None = None
    ) -> bool:
        type_filter = ""
        if expected_type is not None:
            type_filter = f" AND TABLE_TYPE = {sql_string(expected_type.upper())}"
        rows = self.execute(
            "SELECT COUNT(*) AS TABLE_COUNT "
            f"FROM {quote_fqn(relation.database, 'INFORMATION_SCHEMA', 'TABLES')} "
            f"WHERE TABLE_SCHEMA = {sql_string(relation.schema)} "
            f"AND TABLE_NAME = {sql_string(relation.identifier)}"
            f"{type_filter}"
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

    def drop_iceberg_table(self, relation: RelationConfig) -> None:
        self.execute(drop_iceberg_table_sql(relation))

    def add_columns(self, relation: RelationConfig, columns: list[SnowflakeColumn]) -> None:
        for statement in alter_table_add_columns_sql(relation, columns):
            self.execute(statement)

    def create_or_replace_view(
        self,
        target: RelationConfig,
        internal: RelationConfig,
        columns: list[ViewColumn],
    ) -> None:
        self.execute(create_or_replace_view_sql(target, internal, columns))

    def resolve_stage_location(
        self,
        export_location: str,
        run_id: str | None = None,
        *,
        allowed_schemes: Sequence[str] = GCS_STAGE_SCHEMES,
        field_name: str = "bigquery_export_location",
        cloud_label: str = "GCS",
    ) -> StageLocation:
        stage_fqn, stage_path = parse_stage_location(export_location, field_name=field_name)
        rows = self.execute(desc_stage_sql(stage_fqn))
        url = _stage_url(rows)
        if not any(url.startswith(scheme) for scheme in allowed_schemes):
            raise ConfigError(
                f"{field_name} must reference a Snowflake stage backed by {cloud_label}"
            )
        path_parts = [part for part in (stage_path, run_id or "") if part and str(part).strip("/")]
        run_path = "/".join(part.strip("/") for part in path_parts)
        run_stage_location = f"@{stage_fqn}/{run_path}" if run_path else f"@{stage_fqn}"
        remote_run_uri = _remote_uri_for_stage_url(url, run_path)
        return StageLocation(
            stage_fqn=stage_fqn,
            stage_path=stage_path,
            run_stage_location=run_stage_location,
            remote_run_uri=remote_run_uri,
            stage_url=url,
        )

    def list_stage_files(self, stage_location: str) -> list[StageFile]:
        rows = self.execute(list_files_sql(stage_location))
        files: list[StageFile] = []
        for row in rows:
            data = _row_to_mapping(row)
            name = data.get("name") or data.get("NAME")
            if not name:
                continue
            size_value = data.get("size") or data.get("SIZE")
            last_modified = data.get("last_modified") or data.get("LAST_MODIFIED")
            files.append(
                StageFile(
                    name=str(name),
                    size=int(size_value) if size_value not in (None, "") else None,
                    last_modified=str(last_modified) if last_modified not in (None, "") else None,
                )
            )
        return files

    def infer_parquet_schema(
        self,
        *,
        location: str,
        file_format: str,
        files: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        rows = self.execute(
            infer_schema_sql(location=location, file_format=file_format, files=files)
        )
        return [_row_to_mapping(row) for row in rows]

    def begin(self) -> None:
        self.execute("BEGIN")

    def commit(self) -> None:
        self.execute("COMMIT")

    def rollback(self) -> None:
        self.execute("ROLLBACK")

    def delete_from_iceberg(self, relation: RelationConfig, predicate: str | None) -> None:
        self.execute(delete_sql(relation, predicate))

    def copy_into_iceberg(
        self,
        relation: RelationConfig,
        stage_run_location: str,
        *,
        pattern: str | None = None,
        files: list[str] | None = None,
        force: bool = False,
    ) -> None:
        self.execute(
            copy_into_sql(
                relation,
                stage_run_location,
                pattern=pattern,
                files=files,
                force=force,
            )
        )

    def ensure_run_log(self, relation: RelationConfig | None) -> None:
        if relation is not None:
            self.execute(create_or_alter_run_log_table_sql(relation))

    def write_run_log(self, relation: RelationConfig | None, payload: dict[str, Any]) -> None:
        if relation is not None:
            self.execute(insert_run_log_sql(relation, payload))


def parse_stage_location(
    export_location: str,
    *,
    field_name: str = "bigquery_export_location",
) -> tuple[str, str]:
    if not export_location.startswith("@"):
        raise ConfigError(f"{field_name} must start with @")
    raw = export_location[1:]
    if not raw or raw.startswith(("~", "%")):
        raise ConfigError(
            f"{field_name} must be a named Snowflake stage, not a user or table stage"
        )
    if "/" in raw:
        stage_raw, stage_path = raw.split("/", 1)
    else:
        stage_raw, stage_path = raw, ""
    raw_parts = stage_raw.split(".")
    if any(part == "" for part in raw_parts):
        raise ConfigError(f"{field_name} contains an invalid stage name")
    stage_parts = [part.strip('"') for part in raw_parts]
    if any(part == "" for part in stage_parts) or not 1 <= len(stage_parts) <= 3:
        raise ConfigError(f"{field_name} contains an invalid stage name")
    return quote_stage_fqn(stage_parts), stage_path.strip("/")


def stage_relative_file_name(
    listed_name: str,
    *,
    stage_url: str,
    stage_path: str = "",
) -> str:
    """Convert a LIST name into a path relative to the stage root URL."""

    name = listed_name.strip().lstrip("/")
    stage_url_normalized = stage_url.rstrip("/")
    remote_prefixes = (
        stage_url_normalized,
        _gs_uri_from_gcs(stage_url_normalized),
    )
    for prefix in remote_prefixes:
        if name.startswith(prefix + "/"):
            name = name[len(prefix) + 1 :]
            break
        if name == prefix:
            name = ""
            break

    stage_path_normalized = stage_path.strip("/")
    if stage_path_normalized:
        if name.startswith(stage_path_normalized + "/"):
            name = name[len(stage_path_normalized) + 1 :]
        elif name == stage_path_normalized:
            name = ""
    return name.lstrip("/")


def is_s3_stage_url(url: str) -> bool:
    return any(url.startswith(scheme) for scheme in S3_STAGE_SCHEMES)


def _remote_uri_for_stage_url(url: str, run_path: str) -> str:
    remote_base = "gs://" + url.removeprefix("gcs://") if url.startswith("gcs://") else url
    remote_run_uri = "/".join(
        part.strip("/") for part in (remote_base, run_path) if part and part.strip("/")
    )
    if url.startswith("gcs://") and not remote_run_uri.startswith("gs://"):
        remote_run_uri = "gs://" + remote_run_uri.removeprefix("gs:/").lstrip("/")
    return remote_run_uri


def _gs_uri_from_gcs(url: str) -> str:
    if url.startswith("gcs://"):
        return "gs://" + url.removeprefix("gcs://")
    return url


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
