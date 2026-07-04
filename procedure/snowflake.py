from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .config import IcebergSyncConfig
from .errors import SnowflakeSyncError
from .schema import SnowflakeColumn, validate_schema_compatibility
from .sql import (
    bool_literal,
    csv,
    quote_identifier,
    quote_relation,
    stage_copy_location,
    string_literal,
)


@dataclass(frozen=True)
class StageLocation:
    stage_name: str
    stage_path: str
    gcs_uri: str

    @property
    def snowflake_location(self) -> str:
        if self.stage_path:
            return f"@{self.stage_name}/{self.stage_path.strip('/')}"
        return f"@{self.stage_name}"


def parse_named_stage_location(value: str) -> tuple[str, str]:
    if not value or not value.startswith("@"):
        raise SnowflakeSyncError(
            "bigquery_export_location must be a named Snowflake stage location such as @DB.SCHEMA.STAGE/prefix."
        )
    without_at = value[1:]
    if "/" in without_at:
        stage_name, stage_path = without_at.split("/", 1)
    else:
        stage_name, stage_path = without_at, ""
    if not stage_name:
        raise SnowflakeSyncError("bigquery_export_location is missing the stage name.")
    return stage_name, stage_path.strip("/")


class SnowflakeClient:
    def __init__(self, session: Any) -> None:
        self.session = session

    def execute(self, sql: str) -> list[Any]:
        try:
            return self.session.sql(sql).collect()
        except Exception as exc:  # pragma: no cover - Snowpark exception type is runtime-provided.
            raise SnowflakeSyncError(f"Snowflake SQL failed: {sql}\n{exc}") from exc

    def query_id(self) -> str | None:
        try:
            rows = self.session.sql("select last_query_id()").collect()
            return rows[0][0] if rows else None
        except Exception:
            return None

    def table_exists(self, database: str, schema: str, identifier: str) -> bool:
        like_pattern = identifier.replace("'", "''")
        schema_relation = quote_relation(database, schema, "")
        rows = self.execute(
            f"show iceberg tables like {string_literal(like_pattern)} in schema "
            f"{schema_relation}"
        )
        return len(rows) > 0

    def describe_stage_location(self, named_stage_location: str) -> StageLocation:
        stage_name, stage_path = parse_named_stage_location(named_stage_location)
        rows = self.execute(f"desc stage {stage_name}")
        url = None
        for row in rows:
            values = row.as_dict() if hasattr(row, "as_dict") else row.asDict() if hasattr(row, "asDict") else {}
            property_name = (
                values.get("property")
                or values.get("Property")
                or values.get("PROPERTY")
                or (row[0] if len(row) > 0 else None)
            )
            property_value = (
                values.get("property_value")
                or values.get("Property Value")
                or values.get("PROPERTY_VALUE")
                or (row[1] if len(row) > 1 else None)
            )
            if str(property_name).upper() in {"URL", "STAGE_LOCATION"} and property_value:
                url = str(property_value)
                break
        if not url:
            raise SnowflakeSyncError(f"Could not resolve backing cloud URL from DESC STAGE {stage_name}.")
        if not url.lower().startswith(("gcs://", "gs://")):
            raise SnowflakeSyncError(f"Stage {stage_name} must point at a GCS URL for BigQuery export.")
        normalized = "gs://" + url.split("://", 1)[1].strip("/")
        if stage_path:
            normalized = f"{normalized}/{stage_path}"
        return StageLocation(stage_name=stage_name, stage_path=stage_path, gcs_uri=normalized)

    def existing_columns(self, database: str, schema: str, identifier: str) -> dict[str, str]:
        rows = self.execute(f"describe table {quote_relation(database, schema, identifier)}")
        columns: dict[str, str] = {}
        for row in rows:
            values = row.as_dict() if hasattr(row, "as_dict") else row.asDict() if hasattr(row, "asDict") else {}
            name = values.get("name") or values.get("Name") or values.get("NAME") or (row[0] if len(row) > 0 else None)
            data_type = (
                values.get("type")
                or values.get("Type")
                or values.get("TYPE")
                or (row[1] if len(row) > 1 else None)
            )
            if name and data_type:
                columns[str(name)] = str(data_type)
        return columns

    def create_or_alter_iceberg_table(
        self,
        config: IcebergSyncConfig,
        columns: list[SnowflakeColumn],
        exists: bool,
    ) -> None:
        internal = config.internal_relation
        relation = quote_relation(internal.database, internal.schema, internal.identifier)
        if not exists:
            ddl = render_create_iceberg_table(config, columns)
            self.execute(ddl)
            return

        additive_columns = validate_schema_compatibility(
            self.existing_columns(internal.database, internal.schema, internal.identifier),
            columns,
        )
        for column in additive_columns:
            self.execute(
                f"alter iceberg table {relation} add column "
                f"{quote_identifier(column.storage_name)} {column.snowflake_type}"
            )

    def load_copy(
        self,
        config: IcebergSyncConfig,
        named_stage_location: str,
        run_prefix: str,
        effective_mode: str,
    ) -> list[str]:
        internal = config.internal_relation
        relation = quote_relation(internal.database, internal.schema, internal.identifier)
        query_ids: list[str] = []
        self.execute("begin")
        try:
            if effective_mode == "incremental" and config.incremental_predicate:
                self.execute(f"delete from {relation} where {config.incremental_predicate}")
            else:
                self.execute(f"delete from {relation}")
            query_ids.append(self.query_id() or "")

            self.execute(render_copy_into_sql(relation, named_stage_location, run_prefix))
            query_ids.append(self.query_id() or "")
            self.execute("commit")
            return [query_id for query_id in query_ids if query_id]
        except Exception:
            self.execute("rollback")
            raise


def render_create_iceberg_table(config: IcebergSyncConfig, columns: list[SnowflakeColumn]) -> str:
    internal = config.internal_relation
    relation = quote_relation(internal.database, internal.schema, internal.identifier)
    column_sql = ",\n  ".join(
        f"{quote_identifier(column.storage_name)} {column.snowflake_type}"
        for column in columns
    )
    header = f"create or replace iceberg table {relation}"
    if config.iceberg_table_copy_grants:
        header += " copy grants"
    clauses = [
        f"{header} (\n  {column_sql}\n)",
        f"external_volume = {string_literal(config.iceberg_table_external_volume)}",
        "catalog = 'SNOWFLAKE'",
        f"base_location = {string_literal(config.iceberg_table_base_location)}",
        f"target_file_size = {string_literal(config.iceberg_table_target_file_size)}",
        "storage_serialization_policy = "
        f"{string_literal(config.iceberg_table_storage_serialization_policy)}",
        "data_retention_time_in_days = "
        f"{int(config.iceberg_table_data_retention_time_in_days)}",
        f"change_tracking = {bool_literal(config.iceberg_table_change_tracking)}",
        f"iceberg_version = {int(config.iceberg_table_iceberg_version)}",
        "enable_iceberg_merge_on_read = "
        f"{bool_literal(config.iceberg_table_enable_iceberg_merge_on_read)}",
        "enable_data_compaction = "
        f"{bool_literal(config.iceberg_table_enable_data_compaction)}",
    ]
    if config.iceberg_table_max_data_extension_time_in_days is not None:
        clauses.append(
            "max_data_extension_time_in_days = "
            f"{int(config.iceberg_table_max_data_extension_time_in_days)}"
        )
    return "\n".join(clauses)


def render_copy_into_sql(relation: str, named_stage_location: str, run_prefix: str) -> str:
    return "\n".join(
        [
            f"copy into {relation}",
            f"from {stage_copy_location(named_stage_location, run_prefix)}",
            "file_format = (type = parquet use_vectorized_scanner = true)",
            "load_mode = add_files_copy",
            "match_by_column_name = case_sensitive",
            "purge = false",
        ]
    )


def render_view_sql(target_relation: str, internal_relation: str, columns: list[SnowflakeColumn]) -> str:
    select_list = csv(
        f"{quote_identifier(column.storage_name)} as {column.alias}"
        for column in columns
    )
    return f"create or replace view {target_relation} as select {select_list} from {internal_relation}"
