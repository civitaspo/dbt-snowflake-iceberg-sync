from __future__ import annotations

from typing import Any

from .config import IcebergSyncConfig
from .gcp_auth import build_gcp_credentials
from .run_log import ensure_run_log_table, write_run_log
from .schema import columns_from_bigquery_schema
from .snowflake import SnowflakeClient
from .sources.bigquery import BigQueryRestClient, BigQuerySource
from .utils import new_run_id, utc_now_iso


def _secret_string(alias: str) -> str:
    try:
        import _snowflake  # type: ignore[import-not-found]
    except ImportError as exc:  # pragma: no cover - Snowflake-only import.
        raise RuntimeError("Snowflake secret access is available only inside Snowflake.") from exc
    return _snowflake.get_generic_secret_string(alias)


def main(session: Any, config: dict[str, Any]) -> dict[str, Any]:
    parsed = IcebergSyncConfig.from_dict(dict(config))
    run_id = new_run_id()
    started_at = utc_now_iso()
    snowflake = SnowflakeClient(session)
    export_result = None
    snowflake_query_ids: list[str] = []
    effective_mode = None

    try:
        ensure_run_log_table(snowflake, parsed)
        internal = parsed.internal_relation
        internal_exists = snowflake.table_exists(
            internal.database,
            internal.schema,
            internal.identifier,
        )
        effective_mode = parsed.effective_mode(internal_exists)
        parsed.validate_incremental_pairing(effective_mode)

        stage_location = snowflake.describe_stage_location(parsed.bigquery_export_location)
        run_prefix = f"dbt_iceberg_sync/{run_id}"
        gcs_export_prefix = f"{stage_location.gcs_uri.rstrip('/')}/{run_prefix}"

        credentials = build_gcp_credentials(session, parsed.deployment, _secret_string)
        source = BigQuerySource(BigQueryRestClient(credentials))
        export_plan = source.plan_export(parsed, effective_mode, gcs_export_prefix)
        export_result = source.export(export_plan, parsed)

        columns = columns_from_bigquery_schema(export_plan.schema)
        snowflake.create_or_alter_iceberg_table(parsed, columns, internal_exists)
        snowflake_query_ids = snowflake.load_copy(
            parsed,
            parsed.bigquery_export_location,
            run_prefix,
            effective_mode,
        )

        write_run_log(
            snowflake,
            parsed,
            run_id=run_id,
            effective_mode=effective_mode,
            export_result=export_result,
            snowflake_query_ids=snowflake_query_ids,
            status="success",
            error_message=None,
            started_at=started_at,
        )
        return {
            "status": "success",
            "run_id": run_id,
            "effective_mode": effective_mode,
            "internal_relation": parsed.internal_relation.rendered,
            "view_columns": [column.as_dict() for column in columns],
            "export": export_result.as_dict(),
            "snowflake_query_ids": snowflake_query_ids,
        }
    except Exception as exc:
        try:
            write_run_log(
                snowflake,
                parsed,
                run_id=run_id,
                effective_mode=effective_mode,
                export_result=export_result,
                snowflake_query_ids=snowflake_query_ids,
                status="failure",
                error_message=str(exc),
                started_at=started_at,
            )
        finally:
            raise
