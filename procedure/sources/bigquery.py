"""BigQuery source planning and export implementation."""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol
from urllib.parse import quote

from ..config import BigQueryConfig, IcebergSyncConfig
from ..errors import ConfigError, SourceError
from ..schema import SnowflakeColumn, map_bigquery_schema
from ..utils import stable_hash
from .base import SourceExecutionContext, SourceExportResult


class BigQueryClientProtocol(Protocol):
    def get_table(self, project_id: str, dataset_id: str, table_id: str) -> dict[str, Any]: ...

    def list_tables(
        self, project_id: str, dataset_id: str, *, prefix: str | None = None
    ) -> list[dict[str, Any]]: ...

    def run_query_job(
        self,
        project_id: str,
        *,
        location: str,
        query: str,
        destination_table: dict[str, str],
    ) -> dict[str, Any]: ...

    def insert_query_job(
        self,
        project_id: str,
        *,
        location: str,
        query: str,
        destination_table: dict[str, str],
    ) -> dict[str, Any]: ...

    def run_extract_job(
        self,
        project_id: str,
        *,
        location: str,
        source_table: dict[str, str],
        destination_uris: list[str],
        compression: str,
    ) -> dict[str, Any]: ...

    def insert_extract_job(
        self,
        project_id: str,
        *,
        location: str,
        source_table: dict[str, str],
        destination_uris: list[str],
        compression: str,
    ) -> dict[str, Any]: ...

    def get_job(self, project_id: str, *, location: str, job_id: str) -> dict[str, Any]: ...

    def patch_table(
        self,
        project_id: str,
        dataset_id: str,
        table_id: str,
        patch: dict[str, Any],
    ) -> dict[str, Any]: ...


class BigQuerySourceAdapter:
    source_type = "bigquery"

    def __init__(self, client: BigQueryClientProtocol):
        self.client = client

    def export_location(self, config: IcebergSyncConfig) -> str:
        return config.bigquery.export_location

    def export(
        self,
        config: IcebergSyncConfig,
        context: SourceExecutionContext,
    ) -> SourceExportResult:
        predicates = config.predicates_for_mode(context.effective_mode)
        try:
            predicate_type = resolve_predicate_type(config.bigquery, predicates, self.client)
        except SourceError as exc:
            if _should_skip_missing_extract_table(config.bigquery, exc):
                return _skipped_export_result()
            raise
        if config.bigquery.export_strategy == "extract":
            return self._export_extract(
                config.bigquery,
                predicate_type,
                predicates,
                context.destination_uri,
            )
        return self._export_select(config, predicate_type, predicates, context.destination_uri)

    def map_schema(self, export_result: SourceExportResult) -> list[SnowflakeColumn]:
        return map_bigquery_schema(export_result.schema_fields)

    def start_export(
        self,
        config: IcebergSyncConfig,
        context: SourceExecutionContext,
    ) -> dict[str, Any]:
        predicates = config.predicates_for_mode(context.effective_mode)
        try:
            predicate_type = resolve_predicate_type(config.bigquery, predicates, self.client)
        except SourceError as exc:
            if _should_skip_missing_extract_table(config.bigquery, exc):
                return _skipped_export_state()
            raise
        if config.bigquery.export_strategy == "extract":
            return self._start_extract(
                config.bigquery,
                predicate_type,
                predicates,
                context.destination_uri,
            )
        return self._start_select(config, predicate_type, predicates, context.destination_uri)

    def poll_export(
        self,
        config: IcebergSyncConfig,
        state: dict[str, Any],
    ) -> dict[str, Any]:
        phase = state.get("phase")
        if phase == "query":
            return self._poll_select_query(config, state)
        if phase == "extract":
            return self._poll_extract(state)
        raise ConfigError("unknown BigQuery export phase")

    def _export_extract(
        self,
        bq: BigQueryConfig,
        predicate_type: str,
        predicates: tuple[str, ...],
        destination_uri: str,
    ) -> SourceExportResult:
        tables = concrete_extract_tables(bq, predicate_type, predicates, self.client)
        if not tables:
            if bq.skip_missing_tables:
                return _skipped_export_result("no BigQuery tables matched the extract plan")
            raise SourceError("no BigQuery tables matched the extract plan")

        job_refs: list[dict[str, Any]] = []
        segments: list[dict[str, Any]] = []
        schema_fields: list[dict[str, Any]] | None = None
        for table_id in tables:
            schema_table_id = schema_table_id_for_extract(predicate_type, table_id)
            try:
                table = self.client.get_table(bq.project_id, bq.dataset_id, schema_table_id)
            except SourceError as exc:
                if _should_skip_missing_extract_table(bq, exc):
                    continue
                raise
            if schema_fields is None:
                schema_fields = table.get("schema", {}).get("fields", [])
            segment_uri = (
                f"{destination_uri.rstrip('/')}/segment-{len(segments):05d}-*.parquet"
            )
            try:
                job = self.client.run_extract_job(
                    bq.project_id,
                    location=bq.location,
                    source_table={
                        "projectId": bq.project_id,
                        "datasetId": bq.dataset_id,
                        "tableId": table_id,
                    },
                    destination_uris=[segment_uri],
                    compression=bq.export_compression,
                )
            except SourceError as exc:
                if _should_skip_missing_extract_table(bq, exc):
                    continue
                raise
            job_refs.append(job.get("jobReference", job))
            segments.append({"table_id": table_id, "destination_uri": segment_uri})
        if not segments and bq.skip_missing_tables:
            return _skipped_export_result()
        return SourceExportResult(
            schema_fields=schema_fields or [],
            segments=segments,
            job_references=job_refs,
        )

    def _export_select(
        self,
        config: IcebergSyncConfig,
        predicate_type: str,
        predicates: tuple[str, ...],
        destination_uri: str,
    ) -> SourceExportResult:
        bq = config.bigquery
        if predicate_type not in {"none", "where"}:
            raise ConfigError("select export strategy allows only none or where predicates")
        assert bq.staging_dataset_id is not None
        staging_table_id = staging_table_id_for(config, predicates)
        staging_ref = {
            "projectId": bq.project_id,
            "datasetId": bq.staging_dataset_id,
            "tableId": staging_table_id,
        }
        existing = _get_table_or_none(
            self.client, bq.project_id, bq.staging_dataset_id, staging_table_id
        )
        expected_hash = staging_hash(config, predicates)
        reuse = (
            bq.staging_table_reuse
            and not bq.force_rebuild_staging_table
            and existing is not None
            and _table_has_hash(existing, expected_hash)
            and not _table_is_expired(existing)
        )
        job_refs: list[dict[str, Any]] = []
        if not reuse:
            query = select_sql_with_predicates(config.model.sql, predicate_type, predicates)
            job = self.client.run_query_job(
                bq.project_id,
                location=bq.location,
                query=query,
                destination_table=staging_ref,
            )
            job_refs.append(job.get("jobReference", job))
            expiration = datetime.now(tz=UTC) + timedelta(hours=bq.staging_table_expiration_hours)
            self.client.patch_table(
                bq.project_id,
                bq.staging_dataset_id,
                staging_table_id,
                {
                    "expirationTime": str(int(expiration.timestamp() * 1000)),
                    "labels": {"dbt_iceberg_sync_hash": expected_hash[:63]},
                },
            )

        table = self.client.get_table(bq.project_id, bq.staging_dataset_id, staging_table_id)
        schema_fields = table.get("schema", {}).get("fields", [])
        segment_uri = f"{destination_uri.rstrip('/')}/segment-00000-*.parquet"
        extract_job = self.client.run_extract_job(
            bq.project_id,
            location=bq.location,
            source_table=staging_ref,
            destination_uris=[segment_uri],
            compression=bq.export_compression,
        )
        job_refs.append(extract_job.get("jobReference", extract_job))
        return SourceExportResult(
            schema_fields=schema_fields,
            segments=[{"table_id": staging_table_id, "destination_uri": segment_uri}],
            job_references=job_refs,
            staging_table_reference=(f"{bq.project_id}.{bq.staging_dataset_id}.{staging_table_id}"),
        )

    def _start_extract(
        self,
        bq: BigQueryConfig,
        predicate_type: str,
        predicates: tuple[str, ...],
        destination_uri: str,
    ) -> dict[str, Any]:
        tables = concrete_extract_tables(bq, predicate_type, predicates, self.client)
        if not tables:
            if bq.skip_missing_tables:
                return _skipped_export_state("no BigQuery tables matched the extract plan")
            raise SourceError("no BigQuery tables matched the extract plan")

        job_refs: list[dict[str, Any]] = []
        segments: list[dict[str, Any]] = []
        schema_fields: list[dict[str, Any]] | None = None
        for table_id in tables:
            schema_table_id = schema_table_id_for_extract(predicate_type, table_id)
            try:
                table = self.client.get_table(bq.project_id, bq.dataset_id, schema_table_id)
            except SourceError as exc:
                if _should_skip_missing_extract_table(bq, exc):
                    continue
                raise
            if schema_fields is None:
                schema_fields = table.get("schema", {}).get("fields", [])
            segment_uri = (
                f"{destination_uri.rstrip('/')}/segment-{len(segments):05d}-*.parquet"
            )
            try:
                job = self.client.insert_extract_job(
                    bq.project_id,
                    location=bq.location,
                    source_table={
                        "projectId": bq.project_id,
                        "datasetId": bq.dataset_id,
                        "tableId": table_id,
                    },
                    destination_uris=[segment_uri],
                    compression=bq.export_compression,
                )
            except SourceError as exc:
                if _should_skip_missing_extract_table(bq, exc):
                    continue
                raise
            job_ref = job.get("jobReference", job)
            job_refs.append(job_ref)
            segments.append({"table_id": table_id, "destination_uri": segment_uri})

        if not segments and bq.skip_missing_tables:
            return _skipped_export_state()
        state = {
            "status": "running",
            "phase": "extract",
            "schema_fields": schema_fields or [],
            "segments": segments,
            "job_references": job_refs,
            "pending_jobs": job_refs,
            "staging_table_reference": None,
        }
        return self._poll_extract(state)

    def _start_select(
        self,
        config: IcebergSyncConfig,
        predicate_type: str,
        predicates: tuple[str, ...],
        destination_uri: str,
    ) -> dict[str, Any]:
        bq = config.bigquery
        if predicate_type not in {"none", "where"}:
            raise ConfigError("select export strategy allows only none or where predicates")
        assert bq.staging_dataset_id is not None
        staging_table_id = staging_table_id_for(config, predicates)
        staging_ref = {
            "projectId": bq.project_id,
            "datasetId": bq.staging_dataset_id,
            "tableId": staging_table_id,
        }
        existing = _get_table_or_none(
            self.client, bq.project_id, bq.staging_dataset_id, staging_table_id
        )
        expected_hash = staging_hash(config, predicates)
        reuse = (
            bq.staging_table_reuse
            and not bq.force_rebuild_staging_table
            and existing is not None
            and _table_has_hash(existing, expected_hash)
            and not _table_is_expired(existing)
        )
        state = {
            "status": "running",
            "phase": "query",
            "predicate_type": predicate_type,
            "predicates": list(predicates),
            "destination_uri": destination_uri,
            "staging_ref": staging_ref,
            "staging_table_reference": (
                f"{bq.project_id}.{bq.staging_dataset_id}.{staging_table_id}"
            ),
            "expected_hash": expected_hash,
            "job_references": [],
            "pending_jobs": [],
        }
        if reuse:
            return self._submit_select_extract(config, state)

        query = select_sql_with_predicates(config.model.sql, predicate_type, predicates)
        job = self.client.insert_query_job(
            bq.project_id,
            location=bq.location,
            query=query,
            destination_table=staging_ref,
        )
        job_ref = job.get("jobReference", job)
        state["query_job_reference"] = job_ref
        state["job_references"] = [job_ref]
        state["pending_jobs"] = [job_ref]
        return self._poll_select_query(config, state)

    def _poll_select_query(
        self,
        config: IcebergSyncConfig,
        state: dict[str, Any],
    ) -> dict[str, Any]:
        query_ref = state.get("query_job_reference")
        if query_ref:
            job = self._get_job(query_ref)
            if not _job_done(job):
                return state
            _raise_if_job_failed(job)

            bq = config.bigquery
            expiration = datetime.now(tz=UTC) + timedelta(
                hours=bq.staging_table_expiration_hours
            )
            staging_ref = state["staging_ref"]
            self.client.patch_table(
                bq.project_id,
                staging_ref["datasetId"],
                staging_ref["tableId"],
                {
                    "expirationTime": str(int(expiration.timestamp() * 1000)),
                    "labels": {
                        "dbt_iceberg_sync_hash": str(state["expected_hash"])[:63]
                    },
                },
            )
        return self._submit_select_extract(config, state)

    def _submit_select_extract(
        self,
        config: IcebergSyncConfig,
        state: dict[str, Any],
    ) -> dict[str, Any]:
        bq = config.bigquery
        staging_ref = state["staging_ref"]
        table = self.client.get_table(
            bq.project_id,
            staging_ref["datasetId"],
            staging_ref["tableId"],
        )
        schema_fields = table.get("schema", {}).get("fields", [])
        segment_uri = f"{str(state['destination_uri']).rstrip('/')}/segment-00000-*.parquet"
        extract_job = self.client.insert_extract_job(
            bq.project_id,
            location=bq.location,
            source_table=staging_ref,
            destination_uris=[segment_uri],
            compression=bq.export_compression,
        )
        extract_ref = extract_job.get("jobReference", extract_job)
        state.update(
            {
                "status": "running",
                "phase": "extract",
                "schema_fields": schema_fields,
                "segments": [
                    {"table_id": staging_ref["tableId"], "destination_uri": segment_uri}
                ],
                "job_references": [*state.get("job_references", []), extract_ref],
                "pending_jobs": [extract_ref],
            }
        )
        state.pop("query_job_reference", None)
        return self._poll_extract(state)

    def _poll_extract(self, state: dict[str, Any]) -> dict[str, Any]:
        pending_refs = list(state.get("pending_jobs") or [])
        remaining = []
        for job_ref in pending_refs:
            job = self._get_job(job_ref)
            if not _job_done(job):
                remaining.append(job_ref)
                continue
            _raise_if_job_failed(job)
        if remaining:
            state["pending_jobs"] = remaining
            return state
        return {
            "status": "success",
            "schema_fields": state.get("schema_fields", []),
            "segments": state.get("segments", []),
            "job_references": state.get("job_references", []),
            "staging_table_reference": state.get("staging_table_reference"),
        }

    def _get_job(self, job_ref: dict[str, Any]) -> dict[str, Any]:
        job_id = job_ref.get("jobId")
        project_id = job_ref.get("projectId")
        location = job_ref.get("location")
        if not job_id or not project_id or not location:
            raise SourceError("BigQuery job reference is missing projectId, location, or jobId")
        return self.client.get_job(project_id, location=location, job_id=job_id)


def resolve_predicate_type(
    bq: BigQueryConfig,
    predicates: tuple[str, ...],
    client: BigQueryClientProtocol | None = None,
) -> str:
    requested = bq.export_predicate_type
    if requested != "auto":
        _validate_predicate_type(bq, requested, predicates)
        return requested
    if bq.export_strategy == "select":
        return "where" if predicates else "none"
    if not predicates:
        return "none"
    if bq.table_id.endswith("_*"):
        return "table_suffix"
    if client is None:
        raise ConfigError("auto partition predicate resolution requires BigQuery table metadata")
    table = client.get_table(bq.project_id, bq.dataset_id, bq.table_id)
    if table.get("timePartitioning") or table.get("rangePartitioning"):
        return "partition_decorator"
    raise ConfigError(
        "auto extract predicates require a wildcard table or a native partitioned table"
    )


def concrete_extract_tables(
    bq: BigQueryConfig,
    predicate_type: str,
    predicates: tuple[str, ...],
    client: BigQueryClientProtocol,
) -> list[str]:
    _validate_predicate_type(bq, predicate_type, predicates)
    if predicate_type == "none":
        if bq.table_id.endswith("_*"):
            prefix = bq.table_id[:-1]
            return [
                _table_id(table)
                for table in client.list_tables(bq.project_id, bq.dataset_id, prefix=prefix)
                if _table_id(table).startswith(prefix)
            ]
        return [bq.table_id]
    if predicate_type == "table_suffix":
        prefix = bq.table_id[:-1]
        return [prefix + predicate for predicate in predicates]
    if predicate_type == "partition_decorator":
        if bq.table_id.endswith("_*") or "*" in bq.table_id:
            raise ConfigError(
                "partition_decorator requires a concrete native partitioned table"
            )
        table = client.get_table(bq.project_id, bq.dataset_id, bq.table_id)
        if not _is_native_partitioned(table):
            raise ConfigError("partition_decorator requires a native partitioned table")
        return [f"{bq.table_id}${predicate}" for predicate in predicates]
    raise ConfigError(f"unsupported extract predicate type: {predicate_type}")


def schema_table_id_for_extract(predicate_type: str, table_id: str) -> str:
    if predicate_type == "partition_decorator":
        return table_id.split("$", 1)[0]
    return table_id


def select_sql_with_predicates(
    model_sql: str, predicate_type: str, predicates: tuple[str, ...]
) -> str:
    sql = model_sql.strip().rstrip(";")
    if not sql:
        raise ConfigError("model SQL is required for bigquery_export_strategy='select'")
    if predicate_type == "none":
        if predicates:
            raise ConfigError("none predicate type does not accept predicates")
        return sql
    if predicate_type != "where":
        raise ConfigError("select export strategy supports only where predicates")
    if not predicates:
        raise ConfigError("where predicate type requires at least one predicate")
    predicate_sql = " OR ".join(f"({predicate})" for predicate in predicates)
    return f"SELECT *\nFROM (\n{sql}\n) AS __dbt_iceberg_sync_src\nWHERE {predicate_sql}"


def staging_hash(config: IcebergSyncConfig, predicates: tuple[str, ...]) -> str:
    bq = config.bigquery
    return stable_hash(
        {
            "model_sql": config.model.sql,
            "predicates": list(predicates),
            "source": {
                "project_id": bq.project_id,
                "dataset_id": bq.dataset_id,
                "table_id": bq.table_id,
                "location": bq.location,
            },
            "target": {
                "database": config.target_relation.database,
                "schema": config.target_relation.schema,
                "identifier": config.target_relation.identifier,
            },
            "export": {
                "strategy": bq.export_strategy,
                "predicate_type": bq.export_predicate_type,
            },
        },
        length=32,
    )


def staging_table_id_for(config: IcebergSyncConfig, predicates: tuple[str, ...]) -> str:
    return "__dbt_iceberg_sync_" + staging_hash(config, predicates)[:24]


class BigQueryRestClient:
    """Minimal BigQuery REST client that works in Snowflake Python procedures."""

    def __init__(
        self,
        credentials: Any,
        requests_session: Any | None = None,
    ):
        import requests
        from google.auth.transport.requests import Request

        self.credentials = _coerce_google_credentials(credentials)
        self.auth_request = Request()
        self.requests = requests_session or requests.Session()

    def get_table(self, project_id: str, dataset_id: str, table_id: str) -> dict[str, Any]:
        return self._request(
            "GET",
            "/projects/"
            f"{quote(project_id)}/datasets/{quote(dataset_id)}/tables/"
            f"{quote(table_id, safe='$')}",
        )

    def list_tables(
        self, project_id: str, dataset_id: str, *, prefix: str | None = None
    ) -> list[dict[str, Any]]:
        tables: list[dict[str, Any]] = []
        page_token: str | None = None
        while True:
            params = {"maxResults": "1000"}
            if page_token:
                params["pageToken"] = page_token
            response = self._request(
                "GET",
                f"/projects/{quote(project_id)}/datasets/{quote(dataset_id)}/tables",
                params=params,
            )
            for table in response.get("tables", []):
                table_id = _table_id(table)
                if prefix is None or table_id.startswith(prefix):
                    tables.append(table)
            page_token = response.get("nextPageToken")
            if not page_token:
                return tables

    def run_query_job(
        self,
        project_id: str,
        *,
        location: str,
        query: str,
        destination_table: dict[str, str],
    ) -> dict[str, Any]:
        return self._wait_for_job(
            self.insert_query_job(
                project_id,
                location=location,
                query=query,
                destination_table=destination_table,
            )
        )

    def insert_query_job(
        self,
        project_id: str,
        *,
        location: str,
        query: str,
        destination_table: dict[str, str],
    ) -> dict[str, Any]:
        body = {
            "jobReference": {"projectId": project_id, "location": location},
            "configuration": {
                "query": {
                    "query": query,
                    "useLegacySql": False,
                    "destinationTable": destination_table,
                    "writeDisposition": "WRITE_TRUNCATE",
                }
            },
        }
        return self._insert_job(project_id, body)

    def run_extract_job(
        self,
        project_id: str,
        *,
        location: str,
        source_table: dict[str, str],
        destination_uris: list[str],
        compression: str,
    ) -> dict[str, Any]:
        return self._wait_for_job(
            self.insert_extract_job(
                project_id,
                location=location,
                source_table=source_table,
                destination_uris=destination_uris,
                compression=compression,
            )
        )

    def insert_extract_job(
        self,
        project_id: str,
        *,
        location: str,
        source_table: dict[str, str],
        destination_uris: list[str],
        compression: str,
    ) -> dict[str, Any]:
        body = {
            "jobReference": {"projectId": project_id, "location": location},
            "configuration": {
                "extract": {
                    "sourceTable": source_table,
                    "destinationUris": destination_uris,
                    "destinationFormat": "PARQUET",
                    "compression": compression,
                }
            },
        }
        return self._insert_job(project_id, body)

    def get_job(self, project_id: str, *, location: str, job_id: str) -> dict[str, Any]:
        return self._request(
            "GET",
            f"/projects/{quote(project_id)}/jobs/{quote(job_id)}",
            params={"location": location},
        )

    def patch_table(
        self, project_id: str, dataset_id: str, table_id: str, patch: dict[str, Any]
    ) -> dict[str, Any]:
        return self._request(
            "PATCH",
            f"/projects/{quote(project_id)}/datasets/{quote(dataset_id)}/tables/{quote(table_id)}",
            json=patch,
        )

    def _insert_job(self, project_id: str, body: dict[str, Any]) -> dict[str, Any]:
        return self._request("POST", f"/projects/{quote(project_id)}/jobs", json=body)

    def _wait_for_job(self, job: dict[str, Any]) -> dict[str, Any]:
        ref = job.get("jobReference", {})
        job_id = ref.get("jobId")
        project_id = ref.get("projectId")
        location = ref.get("location")
        if not job_id:
            raise SourceError("BigQuery job response did not include jobId")
        if not project_id or not location:
            raise SourceError("BigQuery job response did not include projectId or location")
        while True:
            current = self.get_job(project_id, location=location, job_id=job_id)
            if _job_done(current):
                _raise_if_job_failed(current)
                return current
            time.sleep(2)

    def _request(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        if not self.credentials.valid:
            self.credentials.refresh(self.auth_request)
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {self.credentials.token}"
        headers["Accept"] = "application/json"
        if "json" in kwargs:
            headers["Content-Type"] = "application/json"
        response = self.requests.request(
            method,
            "https://bigquery.googleapis.com/bigquery/v2" + path,
            headers=headers,
            **kwargs,
        )
        if response.status_code >= 400:
            raise SourceError(
                f"BigQuery API error {response.status_code}: {response.text}",
                status_code=response.status_code,
            )
        return response.json() if response.text else {}


def _coerce_google_credentials(credentials: Any) -> Any:
    if hasattr(credentials, "refresh") and hasattr(credentials, "valid"):
        return credentials

    from google.oauth2 import service_account

    return service_account.Credentials.from_service_account_info(
        credentials,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )


def _validate_predicate_type(
    bq: BigQueryConfig, predicate_type: str, predicates: tuple[str, ...]
) -> None:
    if bq.export_strategy == "select" and predicate_type not in {"none", "where"}:
        raise ConfigError("select export strategy allows only none or where predicates")
    if bq.export_strategy == "extract" and predicate_type == "where":
        raise ConfigError("extract export strategy does not support where predicates")
    if predicate_type == "none" and predicates:
        raise ConfigError("none predicate type does not accept predicates")
    if predicate_type == "where" and not predicates:
        raise ConfigError("where predicate type requires at least one predicate")
    if predicate_type == "table_suffix":
        if not bq.table_id.endswith("_*") or bq.table_id.count("*") != 1:
            raise ConfigError("table_suffix requires a table id ending with '_*'")
        if not predicates:
            raise ConfigError("table_suffix requires at least one predicate")
    if predicate_type == "partition_decorator" and not predicates:
        raise ConfigError("partition_decorator requires at least one predicate")


def _get_table_or_none(
    client: BigQueryClientProtocol, project_id: str, dataset_id: str, table_id: str
) -> dict[str, Any] | None:
    try:
        return client.get_table(project_id, dataset_id, table_id)
    except SourceError as exc:
        status_code = exc.status_code or getattr(exc, "http_status", None)
        if status_code == 404:
            return None
        raise


def _should_skip_missing_extract_table(bq: BigQueryConfig, exc: SourceError) -> bool:
    return (
        bq.export_strategy == "extract"
        and bq.skip_missing_tables
        and _is_missing_table_not_found(exc)
    )


def _is_missing_table_not_found(exc: SourceError) -> bool:
    status_code = exc.status_code or getattr(exc, "http_status", None)
    if status_code != 404:
        return False

    payload = _bigquery_error_payload(exc)
    if isinstance(payload, dict):
        error = payload.get("error")
        if isinstance(error, dict):
            messages = [error.get("message")]
            errors = error.get("errors")
            if isinstance(errors, list):
                messages.extend(
                    item.get("message") for item in errors if isinstance(item, dict)
                )
            return any(_is_missing_table_message(message) for message in messages)

    return _is_missing_table_message(str(exc))


def _bigquery_error_payload(exc: SourceError) -> dict[str, Any] | None:
    message = str(exc)
    json_start = message.find("{")
    if json_start < 0:
        return None
    try:
        parsed = json.loads(message[json_start:])
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _is_missing_table_message(message: Any) -> bool:
    normalized = " ".join(str(message or "").split()).lower()
    return "not found: table " in normalized


def _skipped_export_result(
    reason: str = "BigQuery extract source table was not found",
) -> SourceExportResult:
    return SourceExportResult(
        schema_fields=[],
        segments=[],
        job_references=[],
        skipped=True,
        skip_reason=reason,
    )


def _skipped_export_state(
    reason: str = "BigQuery extract source table was not found",
) -> dict[str, Any]:
    return {
        "status": "skipped",
        "phase": "extract",
        "schema_fields": [],
        "segments": [],
        "job_references": [],
        "pending_jobs": [],
        "staging_table_reference": None,
        "skip_reason": reason,
    }


def _table_has_hash(table: dict[str, Any], expected_hash: str) -> bool:
    labels = table.get("labels") or {}
    return labels.get("dbt_iceberg_sync_hash") == expected_hash[:63]


def _is_native_partitioned(table: dict[str, Any]) -> bool:
    return bool(table.get("timePartitioning") or table.get("rangePartitioning"))


def _table_is_expired(table: dict[str, Any]) -> bool:
    expiration = table.get("expirationTime")
    if not expiration:
        return False
    try:
        return int(expiration) <= int(datetime.now(tz=UTC).timestamp() * 1000)
    except ValueError:
        return False


def _table_id(table: dict[str, Any]) -> str:
    ref = table.get("tableReference") or {}
    return str(ref.get("tableId") or table.get("id", "").split(".")[-1])


def _job_done(job: dict[str, Any]) -> bool:
    return (job.get("status") or {}).get("state") == "DONE"


def _raise_if_job_failed(job: dict[str, Any]) -> None:
    status = job.get("status") or {}
    if status.get("errorResult"):
        raise SourceError("BigQuery job failed: " + json.dumps(status["errorResult"]))
