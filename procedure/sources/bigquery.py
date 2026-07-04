from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

import requests

from ..config import EffectiveMode, IcebergSyncConfig, PredicateType
from ..errors import ConfigError, SourceError
from ..utils import stable_hash

BIGQUERY_API_ROOT = "https://bigquery.googleapis.com/bigquery/v2"


@dataclass(frozen=True)
class ExportSegment:
    source_project_id: str
    source_dataset_id: str
    source_table_id: str
    destination_uri: str
    predicate: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "source_project_id": self.source_project_id,
            "source_dataset_id": self.source_dataset_id,
            "source_table_id": self.source_table_id,
            "destination_uri": self.destination_uri,
            "predicate": self.predicate,
        }


@dataclass(frozen=True)
class ExportPlan:
    predicate_type: PredicateType
    predicates: list[str]
    segments: list[ExportSegment]
    schema: dict[str, Any]
    staging_table: dict[str, str] | None = None
    setup_job_references: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class ExportResult:
    plan: ExportPlan
    job_references: list[dict[str, Any]] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        job_references = [*self.plan.setup_job_references, *self.job_references]
        return {
            "predicate_type": self.plan.predicate_type,
            "predicates": self.plan.predicates,
            "segments": [segment.as_dict() for segment in self.plan.segments],
            "staging_table": self.plan.staging_table,
            "job_references": job_references,
        }


def resolve_export_predicate_type(
    export_strategy: str,
    requested_type: str,
    table_id: str,
    predicates: list[str],
    is_partitioned: bool | None,
) -> PredicateType:
    requested = requested_type or "auto"
    if requested not in {"auto", "none", "partition_decorator", "table_suffix", "where"}:
        raise ConfigError(f"Unsupported BigQuery predicate type: {requested}")

    if export_strategy == "select":
        if requested not in {"auto", "none", "where"}:
            raise ConfigError("select exports support only auto, none, or where predicates.")
        if requested == "auto":
            return "where" if predicates else "none"
        if requested == "where" and not predicates:
            raise ConfigError("bigquery_export_predicate_type='where' requires predicates.")
        if requested == "none" and predicates:
            raise ConfigError("bigquery_export_predicate_type='none' does not allow predicates.")
        return requested  # type: ignore[return-value]

    if export_strategy != "extract":
        raise ConfigError(f"Unsupported BigQuery export strategy: {export_strategy}")
    if requested == "where":
        raise ConfigError("extract exports do not support where predicates.")
    if requested == "none":
        if predicates:
            raise ConfigError("bigquery_export_predicate_type='none' does not allow predicates.")
        return "none"
    if requested == "table_suffix":
        validate_table_suffix_source(table_id)
        if not predicates:
            raise ConfigError("table_suffix predicates require at least one suffix value.")
        return "table_suffix"
    if requested == "partition_decorator":
        if "*" in table_id:
            raise ConfigError("partition_decorator requires a concrete native BigQuery partitioned table.")
        if not predicates:
            raise ConfigError("partition_decorator predicates require at least one decorator value.")
        if is_partitioned is not True:
            raise ConfigError("partition_decorator requires a native BigQuery partitioned table.")
        return "partition_decorator"

    if table_id.endswith("_*"):
        validate_table_suffix_source(table_id)
        if predicates:
            return "table_suffix"
        return "none"
    if not predicates:
        return "none"
    if is_partitioned:
        return "partition_decorator"
    raise ConfigError(
        "auto predicate resolution for extract requires a wildcard table id, no predicates, "
        "or native partitioned BigQuery table metadata."
    )


def validate_table_suffix_source(table_id: str) -> None:
    if table_id.count("*") != 1 or not table_id.endswith("_*"):
        raise ConfigError("table_suffix requires exactly one wildcard in a table id ending with '_*'.")


def select_sql_with_predicates(model_sql: str, predicates: list[str]) -> str:
    if not predicates:
        return model_sql
    where_clause = " OR ".join(f"({predicate})" for predicate in predicates)
    return (
        "SELECT *\n"
        "FROM (\n"
        f"{model_sql.rstrip()}\n"
        ") AS __dbt_iceberg_sync_src\n"
        f"WHERE {where_clause}"
    )


def staging_table_hash(config: IcebergSyncConfig, predicates: list[str]) -> str:
    payload = {
        "model_sql": config.model_sql,
        "predicates": predicates,
        "source": {
            "project": config.google_cloud_project_id,
            "dataset": config.bigquery_dataset_id,
            "table": config.bigquery_table_id,
        },
        "target": {
            "database": config.target_relation.database if config.target_relation else None,
            "schema": config.target_relation.schema if config.target_relation else None,
            "identifier": config.target_relation.identifier if config.target_relation else None,
        },
        "export": {
            "strategy": config.bigquery_export_strategy,
            "predicate_type": config.bigquery_export_predicate_type,
            "location": config.bigquery_location,
        },
    }
    return stable_hash(payload)


class BigQueryRestClient:
    def __init__(self, credentials: Any, timeout_seconds: int = 30) -> None:
        """Create a BigQuery REST client from google-auth credentials.

        Any credential object exposing ``valid``, ``token``, and ``refresh``
        works: static service account keys, workload identity federation
        (external account) credentials, or impersonated credentials.
        """
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()
        self.credentials = credentials

    def _headers(self) -> dict[str, str]:
        if not self.credentials.valid:
            try:
                from google.auth.transport.requests import Request
            except ImportError as exc:
                raise SourceError("google-auth is required inside the Snowflake procedure.") from exc

            self.credentials.refresh(Request())
        return {
            "Authorization": f"Bearer {self.credentials.token}",
            "Content-Type": "application/json",
        }

    def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        url = f"{BIGQUERY_API_ROOT}{path}"
        response = self.session.request(
            method,
            url,
            headers=self._headers(),
            params=params,
            json=body,
            timeout=self.timeout_seconds,
        )
        if response.status_code >= 400:
            raise SourceError(f"BigQuery API request failed: {response.status_code} {response.text}")
        if not response.text:
            return {}
        return response.json()

    def get_table(self, project_id: str, dataset_id: str, table_id: str) -> dict[str, Any]:
        return self.request("GET", f"/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}")

    def patch_table(self, project_id: str, dataset_id: str, table_id: str, body: dict[str, Any]) -> None:
        self.request(
            "PATCH",
            f"/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}",
            body=body,
        )

    def list_tables(self, project_id: str, dataset_id: str, prefix: str) -> list[str]:
        tables: list[str] = []
        page_token: str | None = None
        while True:
            params = {"pageToken": page_token} if page_token else {}
            result = self.request(
                "GET",
                f"/projects/{project_id}/datasets/{dataset_id}/tables",
                params=params,
            )
            for table in result.get("tables", []):
                table_id = table.get("tableReference", {}).get("tableId")
                if table_id and table_id.startswith(prefix):
                    tables.append(table_id)
            page_token = result.get("nextPageToken")
            if not page_token:
                return sorted(tables)

    def insert_job(self, project_id: str, location: str, configuration: dict[str, Any]) -> dict[str, Any]:
        body = {
            "jobReference": {
                "projectId": project_id,
                "location": location,
                "jobId": "dbt_iceberg_sync_" + stable_hash(configuration, 12) + "_" + uuid4().hex[:12],
            },
            "configuration": configuration,
        }
        return self.request("POST", f"/projects/{project_id}/jobs", body=body)

    def get_job(self, project_id: str, location: str, job_id: str) -> dict[str, Any]:
        return self.request(
            "GET",
            f"/projects/{project_id}/jobs/{job_id}",
            params={"location": location},
        )

    def wait_for_job(self, job_reference: dict[str, Any], poll_seconds: float = 2.0) -> dict[str, Any]:
        project_id = job_reference["projectId"]
        location = job_reference.get("location")
        job_id = job_reference["jobId"]
        while True:
            job = self.get_job(project_id, location, job_id)
            status = job.get("status", {})
            if status.get("state") == "DONE":
                if "errorResult" in status:
                    raise SourceError(f"BigQuery job failed: {status['errorResult']}")
                return job
            time.sleep(poll_seconds)

    def extract_table(
        self,
        project_id: str,
        dataset_id: str,
        table_id: str,
        destination_uri: str,
        location: str,
    ) -> dict[str, Any]:
        job = self.insert_job(
            project_id,
            location,
            {
                "extract": {
                    "sourceTable": {
                        "projectId": project_id,
                        "datasetId": dataset_id,
                        "tableId": table_id,
                    },
                    "destinationUris": [destination_uri],
                    "destinationFormat": "PARQUET",
                }
            },
        )
        return self.wait_for_job(job["jobReference"])

    def query_to_table(
        self,
        project_id: str,
        dataset_id: str,
        table_id: str,
        sql: str,
        location: str,
    ) -> dict[str, Any]:
        job = self.insert_job(
            project_id,
            location,
            {
                "query": {
                    "query": sql,
                    "useLegacySql": False,
                    "destinationTable": {
                        "projectId": project_id,
                        "datasetId": dataset_id,
                        "tableId": table_id,
                    },
                    "writeDisposition": "WRITE_TRUNCATE",
                    "createDisposition": "CREATE_IF_NEEDED",
                }
            },
        )
        return self.wait_for_job(job["jobReference"])


class BigQuerySource:
    def __init__(self, client: BigQueryRestClient) -> None:
        self.client = client

    def plan_export(
        self,
        config: IcebergSyncConfig,
        mode: EffectiveMode,
        gcs_export_prefix: str,
    ) -> ExportPlan:
        predicates = config.predicates_for_mode(mode)
        base_table = None
        if config.bigquery_export_strategy == "extract" and not config.bigquery_table_id.endswith("_*"):
            base_table = self.client.get_table(
                config.google_cloud_project_id,
                config.bigquery_dataset_id,
                config.bigquery_table_id,
            )
        is_partitioned = bool(base_table and base_table.get("timePartitioning"))

        predicate_type = resolve_export_predicate_type(
            config.bigquery_export_strategy,
            config.bigquery_export_predicate_type,
            config.bigquery_table_id,
            predicates,
            is_partitioned=is_partitioned if base_table is not None else None,
        )

        if config.bigquery_export_strategy == "select":
            return self._plan_select_export(config, predicate_type, predicates, gcs_export_prefix)
        return self._plan_extract_export(config, predicate_type, predicates, gcs_export_prefix, base_table)

    def export(self, plan: ExportPlan, config: IcebergSyncConfig) -> ExportResult:
        job_references: list[dict[str, Any]] = []
        for segment in plan.segments:
            job = self.client.extract_table(
                segment.source_project_id,
                segment.source_dataset_id,
                segment.source_table_id,
                segment.destination_uri,
                config.bigquery_location,
            )
            job_references.append(job["jobReference"])
        return ExportResult(plan=plan, job_references=job_references)

    def _plan_extract_export(
        self,
        config: IcebergSyncConfig,
        predicate_type: PredicateType,
        predicates: list[str],
        gcs_export_prefix: str,
        base_table: dict[str, Any] | None,
    ) -> ExportPlan:
        project_id = config.google_cloud_project_id
        dataset_id = config.bigquery_dataset_id
        table_id = config.bigquery_table_id
        segments: list[ExportSegment] = []

        if predicate_type == "none" and table_id.endswith("_*"):
            validate_table_suffix_source(table_id)
            prefix = table_id[:-1]
            table_ids = self.client.list_tables(project_id, dataset_id, prefix)
            if not table_ids:
                raise SourceError(f"No BigQuery tables matched wildcard prefix {prefix!r}.")
        elif predicate_type == "table_suffix":
            validate_table_suffix_source(table_id)
            table_ids = [table_id.replace("*", predicate) for predicate in predicates]
        elif predicate_type == "partition_decorator":
            table_ids = [f"{table_id}${predicate}" for predicate in predicates]
        elif predicate_type == "none":
            table_ids = [table_id]
        else:
            raise ConfigError(f"Unsupported extract predicate type: {predicate_type}")

        schema_table_id = table_ids[0].split("$", 1)[0]
        schema = (
            base_table.get("schema")
            if base_table is not None and schema_table_id == table_id
            else self.client.get_table(project_id, dataset_id, schema_table_id).get("schema", {})
        )

        for index, source_table_id in enumerate(table_ids):
            segments.append(
                ExportSegment(
                    source_project_id=project_id,
                    source_dataset_id=dataset_id,
                    source_table_id=source_table_id,
                    destination_uri=f"{gcs_export_prefix.rstrip('/')}/segment_{index:05d}_*.parquet",
                    predicate=predicates[index] if index < len(predicates) else None,
                )
            )
        return ExportPlan(
            predicate_type=predicate_type,
            predicates=predicates,
            segments=segments,
            schema=schema,
        )

    def _plan_select_export(
        self,
        config: IcebergSyncConfig,
        predicate_type: PredicateType,
        predicates: list[str],
        gcs_export_prefix: str,
    ) -> ExportPlan:
        if predicate_type not in {"none", "where"}:
            raise ConfigError(f"Unsupported select predicate type: {predicate_type}")
        staging_hash = staging_table_hash(config, predicates)
        table_id = "__dbt_iceberg_sync_" + staging_hash[:24]
        project_id = config.google_cloud_project_id
        dataset_id = config.bigquery_staging_dataset_id
        should_reuse = False
        setup_job_references: list[dict[str, Any]] = []

        if config.bigquery_staging_table_reuse and not config.force_rebuild_staging_table:
            try:
                existing = self.client.get_table(project_id, dataset_id, table_id)
                labels = existing.get("labels", {})
                expiration_ms = int(existing.get("expirationTime", "0"))
                expires_at = datetime.fromtimestamp(expiration_ms / 1000, timezone.utc)
                should_reuse = (
                    labels.get("iceberg_sync_hash") == staging_hash[:63]
                    and expires_at > datetime.now(timezone.utc)
                )
            except SourceError:
                should_reuse = False

        if not should_reuse:
            sql = select_sql_with_predicates(config.model_sql, predicates if predicate_type == "where" else [])
            query_job = self.client.query_to_table(project_id, dataset_id, table_id, sql, config.bigquery_location)
            setup_job_references.append(query_job["jobReference"])
            expires_at = datetime.now(timezone.utc) + timedelta(
                hours=config.bigquery_staging_table_expiration_hours
            )
            self.client.patch_table(
                project_id,
                dataset_id,
                table_id,
                {
                    "expirationTime": str(int(expires_at.timestamp() * 1000)),
                    "labels": {"iceberg_sync_hash": staging_hash[:63]},
                },
            )

        table = self.client.get_table(project_id, dataset_id, table_id)
        segment = ExportSegment(
            source_project_id=project_id,
            source_dataset_id=dataset_id,
            source_table_id=table_id,
            destination_uri=f"{gcs_export_prefix.rstrip('/')}/segment_00000_*.parquet",
        )
        return ExportPlan(
            predicate_type=predicate_type,
            predicates=predicates,
            segments=[segment],
            schema=table.get("schema", {}),
            staging_table={
                "project_id": project_id,
                "dataset_id": dataset_id,
                "table_id": table_id,
                "hash": staging_hash,
                "reused": str(should_reuse).lower(),
            },
            setup_job_references=setup_job_references,
        )
