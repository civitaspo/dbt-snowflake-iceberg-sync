"""S3 Parquet source planning and schema inference."""

from __future__ import annotations

import re
from typing import Any

from ..config import S3_STAGE_SCHEMES, IcebergSyncConfig
from ..errors import ConfigError, SourceError
from ..schema import SnowflakeColumn, map_parquet_infer_schema
from ..snowflake import SnowflakeClient, stage_relative_file_name
from .base import SourceExecutionContext, SourceExportResult

# Snowflake COPY FILES clause accepts at most 1000 paths per statement.
COPY_FILES_BATCH_SIZE = 1000


class S3ParquetSourceAdapter:
    source_type = "s3_parquet"

    def __init__(self, snowflake: SnowflakeClient):
        self.snowflake = snowflake

    def export_location(self, config: IcebergSyncConfig) -> str:
        if config.s3_parquet is None:
            raise ConfigError("s3_parquet config is required when source_type='s3_parquet'")
        return config.s3_parquet.location

    def export(
        self,
        config: IcebergSyncConfig,
        context: SourceExecutionContext,
    ) -> SourceExportResult:
        state = self.start_export(config, context)
        if state.get("status") == "skipped":
            return SourceExportResult(
                schema_fields=[],
                segments=state.get("segments", []),
                job_references=state.get("job_references", []),
                staging_table_reference=None,
                skipped=True,
                skip_reason=state.get("skip_reason"),
            )
        if state.get("status") != "success":
            raise SourceError(state.get("error_message") or "S3 Parquet planning failed")
        return SourceExportResult(
            schema_fields=state.get("schema_fields", []),
            segments=state.get("segments", []),
            job_references=state.get("job_references", []),
            staging_table_reference=None,
        )

    def map_schema(self, export_result: SourceExportResult) -> list[SnowflakeColumn]:
        return map_parquet_infer_schema(export_result.schema_fields)

    def start_export(
        self,
        config: IcebergSyncConfig,
        context: SourceExecutionContext,
    ) -> dict[str, Any]:
        if config.s3_parquet is None:
            raise ConfigError("s3_parquet config is required when source_type='s3_parquet'")
        s3 = config.s3_parquet
        use_declared_columns = bool(config.columns)
        if not use_declared_columns and not config.deployment.parquet_file_format:
            raise ConfigError(
                "deployment.parquet_file_format is required when source_type='s3_parquet' "
                "and columns is not set"
            )

        base_stage = self.snowflake.resolve_stage_location(
            s3.location,
            None,
            allowed_schemes=S3_STAGE_SCHEMES,
            field_name="s3_parquet_location",
            cloud_label="S3",
        )
        paths = config.predicates_for_mode(context.effective_mode)
        segments: list[dict[str, Any]] = []
        matched_files: list[tuple[str, str | None]] = []
        job_references: list[dict[str, Any]] = []

        for path_suffix in paths:
            stage_location = _join_stage_location(base_stage.run_stage_location, path_suffix)
            listed = self.snowflake.list_stage_files(stage_location)
            query_ids = list(self.snowflake.query_ids)
            if query_ids:
                job_references.append(
                    {
                        "operation": "list",
                        "stage_location": stage_location,
                        "query_id": query_ids[-1],
                    }
                )
            segment_files: list[str] = []
            total_bytes = 0
            for stage_file in listed:
                relative_name = stage_relative_file_name(
                    stage_file.name,
                    stage_url=base_stage.stage_url,
                    stage_path=base_stage.stage_path,
                )
                if path_suffix:
                    prefix = path_suffix.strip("/") + "/"
                    if relative_name.startswith(prefix):
                        candidate = relative_name[len(prefix) :]
                    elif relative_name == path_suffix.strip("/"):
                        candidate = ""
                    else:
                        candidate = relative_name
                else:
                    candidate = relative_name
                if not candidate or candidate.endswith("/"):
                    continue
                if s3.file_pattern and not re.search(s3.file_pattern, candidate):
                    continue
                segment_files.append(candidate)
                if stage_file.size is not None:
                    total_bytes += stage_file.size
                matched_name = candidate
                if path_suffix:
                    matched_name = f"{path_suffix.strip('/')}/{candidate}"
                matched_files.append((matched_name, stage_file.last_modified))
            segments.append(
                {
                    "stage_location": stage_location,
                    "path_suffix": path_suffix,
                    "file_count": len(segment_files),
                    "total_bytes": total_bytes,
                    "files": segment_files,
                }
            )

        if not matched_files:
            if s3.skip_missing_location:
                return {
                    "status": "skipped",
                    "skip_reason": "no Parquet files matched s3_parquet_location",
                    "schema_fields": [],
                    "segments": segments,
                    "job_references": job_references,
                    "staging_table_reference": None,
                }
            raise SourceError("no Parquet files matched s3_parquet_location")

        schema_fields: list[dict[str, Any]] = []
        if not use_declared_columns:
            infer_files = _select_infer_schema_files(
                matched_files,
                max_file_count=s3.infer_schema_max_file_count,
            )
            schema_fields = self.snowflake.infer_parquet_schema(
                location=base_stage.run_stage_location,
                file_format=config.deployment.parquet_file_format or "",
                files=infer_files,
            )
            query_ids = list(self.snowflake.query_ids)
            if query_ids:
                job_references.append(
                    {
                        "operation": "infer_schema",
                        "stage_location": base_stage.run_stage_location,
                        "query_id": query_ids[-1],
                        "files": infer_files,
                    }
                )
        return {
            "status": "success",
            "schema_fields": schema_fields,
            "segments": segments,
            "job_references": job_references,
            "staging_table_reference": None,
            "load_locations": load_locations_from_segments(segments),
        }

    def poll_export(
        self,
        config: IcebergSyncConfig,
        state: dict[str, Any],
    ) -> dict[str, Any]:
        return state


def load_locations_from_segments(segments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    locations: list[dict[str, Any]] = []
    for segment in segments:
        files = [str(name) for name in (segment.get("files") or []) if name]
        if not files:
            continue
        stage_location = str(segment["stage_location"])
        for offset in range(0, len(files), COPY_FILES_BATCH_SIZE):
            locations.append(
                {
                    "stage_location": stage_location,
                    "files": files[offset : offset + COPY_FILES_BATCH_SIZE],
                    "force": True,
                }
            )
    return locations


def _join_stage_location(base_location: str, path_suffix: str) -> str:
    suffix = path_suffix.strip("/")
    if not suffix:
        return base_location.rstrip("/")
    return f"{base_location.rstrip('/')}/{suffix}"


def _select_infer_schema_files(
    matched_files: list[tuple[str, str | None]],
    *,
    max_file_count: int,
) -> list[str]:
    ordered = sorted(
        matched_files,
        key=lambda item: (item[1] or "", item[0]),
        reverse=True,
    )
    selected = [name for name, _ in ordered[:max_file_count]]
    return selected
