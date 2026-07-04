from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

from .errors import ConfigError
from .utils import as_bool, ensure_list

SourceType = Literal["bigquery"]
MaterializationStrategy = Literal["full_refresh", "incremental"]
ExportStrategy = Literal["extract", "select"]
PredicateType = Literal["auto", "none", "partition_decorator", "table_suffix", "where"]
EffectiveMode = Literal["full_refresh", "incremental"]

GCP_AUTH_METHODS = {"service_account_key", "workload_identity_federation"}


FORBIDDEN_MODEL_CONFIG_KEYS = {
    "gcp_sa_secret_fqdn",
    "gcp_sa_secret_alias",
    "gcp_auth_method",
    "gcp_wif_secret_fqdn",
    "gcp_wif_audience",
    "gcp_service_account_impersonation",
    "google_application_credentials",
    "google_credentials",
    "gcp_service_account_json",
    "service_account_json",
    "private_key",
    "password",
    "secret",
}


@dataclass(frozen=True)
class RelationConfig:
    database: str
    schema: str
    identifier: str
    rendered: str | None = None

    @classmethod
    def from_dict(cls, value: dict[str, Any], name: str) -> "RelationConfig":
        try:
            return cls(
                database=str(value["database"]),
                schema=str(value["schema"]),
                identifier=str(value["identifier"]),
                rendered=value.get("rendered"),
            )
        except KeyError as exc:
            raise ConfigError(f"{name} relation is missing {exc.args[0]}") from exc


@dataclass(frozen=True)
class DeploymentConfig:
    procedure_database: str | None = None
    procedure_schema: str | None = None
    procedure_name: str | None = None
    run_log_table: str = "ICEBERG_SYNC_RUN_LOG"
    run_log_enabled: bool = True
    gcp_auth_method: str = "service_account_key"
    gcp_sa_secret_alias: str = "gcp_sa_credentials_json"
    gcp_wif_secret_fqdn: str | None = None
    gcp_wif_audience: str | None = None
    gcp_service_account_impersonation: str | None = None

    @classmethod
    def from_dict(cls, value: dict[str, Any] | None) -> "DeploymentConfig":
        value = value or {}
        config = cls(
            procedure_database=value.get("procedure_database"),
            procedure_schema=value.get("procedure_schema"),
            procedure_name=value.get("procedure_name"),
            run_log_table=value.get("run_log_table", "ICEBERG_SYNC_RUN_LOG"),
            run_log_enabled=as_bool(value.get("run_log_enabled"), True),
            gcp_auth_method=value.get("gcp_auth_method") or "service_account_key",
            gcp_sa_secret_alias=value.get("gcp_sa_secret_alias", "gcp_sa_credentials_json"),
            gcp_wif_secret_fqdn=value.get("gcp_wif_secret_fqdn"),
            gcp_wif_audience=value.get("gcp_wif_audience"),
            gcp_service_account_impersonation=value.get("gcp_service_account_impersonation"),
        )
        config.validate()
        return config

    def validate(self) -> None:
        if self.gcp_auth_method not in GCP_AUTH_METHODS:
            raise ConfigError(
                "gcp_auth_method must be 'service_account_key' or 'workload_identity_federation'."
            )
        wif_keys = {
            "gcp_wif_secret_fqdn": self.gcp_wif_secret_fqdn,
            "gcp_wif_audience": self.gcp_wif_audience,
            "gcp_service_account_impersonation": self.gcp_service_account_impersonation,
        }
        if self.gcp_auth_method == "workload_identity_federation":
            required = ("gcp_wif_secret_fqdn", "gcp_wif_audience")
            missing = [key for key in required if wif_keys[key] in {None, ""}]
            if missing:
                raise ConfigError(
                    "gcp_auth_method='workload_identity_federation' requires: "
                    + ", ".join(missing)
                )
        else:
            unexpected = [key for key, val in wif_keys.items() if val not in {None, ""}]
            if unexpected:
                raise ConfigError(
                    ", ".join(sorted(unexpected))
                    + " are only valid with gcp_auth_method='workload_identity_federation'."
                )


@dataclass(frozen=True)
class IcebergSyncConfig:
    source_type: SourceType = "bigquery"
    materialization_strategy: MaterializationStrategy = "incremental"
    bigquery_export_strategy: ExportStrategy = "extract"
    google_cloud_project_id: str | None = None
    bigquery_dataset_id: str | None = None
    bigquery_table_id: str | None = None
    bigquery_location: str | None = None
    bigquery_export_location: str | None = None
    bigquery_export_predicate_type: PredicateType = "auto"
    bigquery_export_full_refresh_predicates: list[str] = field(default_factory=list)
    bigquery_export_incremental_predicates: list[str] = field(default_factory=list)
    bigquery_staging_dataset_id: str | None = None
    bigquery_staging_table_expiration_hours: int = 24
    bigquery_staging_table_reuse: bool = True
    force_rebuild_staging_table: bool = False
    incremental_strategy: str = "delete+copy"
    incremental_predicate: str | None = None
    iceberg_table_external_volume: str | None = None
    iceberg_table_base_location: str | None = None
    iceberg_table_target_file_size: str = "AUTO"
    iceberg_table_storage_serialization_policy: str = "COMPATIBLE"
    iceberg_table_data_retention_time_in_days: int = 7
    iceberg_table_max_data_extension_time_in_days: int | None = None
    iceberg_table_change_tracking: bool = False
    iceberg_table_copy_grants: bool = False
    iceberg_table_error_logging: bool = True
    iceberg_table_iceberg_version: int = 3
    iceberg_table_enable_iceberg_merge_on_read: bool = True
    iceberg_table_enable_data_compaction: bool = True
    partition_by: list[str] = field(default_factory=list)
    cluster_by: list[str] = field(default_factory=list)
    target_relation: RelationConfig | None = None
    internal_relation: RelationConfig | None = None
    model_sql: str = ""
    model_unique_id: str | None = None
    invocation_id: str | None = None
    dbt_full_refresh: bool = False
    deployment: DeploymentConfig = field(default_factory=DeploymentConfig)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "IcebergSyncConfig":
        forbidden = [key for key in FORBIDDEN_MODEL_CONFIG_KEYS if value.get(key) is not None]
        if forbidden:
            joined = ", ".join(sorted(forbidden))
            raise ConfigError(
                "Credential material and Snowflake secret bindings must not be present "
                f"in model config: {joined}"
            )

        config = cls(
            source_type=value.get("source_type", "bigquery"),
            materialization_strategy=value.get("materialization_strategy", "incremental"),
            bigquery_export_strategy=value.get("bigquery_export_strategy", "extract"),
            google_cloud_project_id=value.get("google_cloud_project_id"),
            bigquery_dataset_id=value.get("bigquery_dataset_id"),
            bigquery_table_id=value.get("bigquery_table_id"),
            bigquery_location=value.get("bigquery_location"),
            bigquery_export_location=value.get("bigquery_export_location"),
            bigquery_export_predicate_type=value.get("bigquery_export_predicate_type", "auto"),
            bigquery_export_full_refresh_predicates=[
                str(item) for item in ensure_list(value.get("bigquery_export_full_refresh_predicates"))
            ],
            bigquery_export_incremental_predicates=[
                str(item) for item in ensure_list(value.get("bigquery_export_incremental_predicates"))
            ],
            bigquery_staging_dataset_id=value.get("bigquery_staging_dataset_id"),
            bigquery_staging_table_expiration_hours=int(
                value.get("bigquery_staging_table_expiration_hours", 24)
            ),
            bigquery_staging_table_reuse=as_bool(value.get("bigquery_staging_table_reuse"), True),
            force_rebuild_staging_table=as_bool(value.get("force_rebuild_staging_table"), False),
            incremental_strategy=value.get("incremental_strategy", "delete+copy"),
            incremental_predicate=value.get("incremental_predicate"),
            iceberg_table_external_volume=value.get("iceberg_table_external_volume"),
            iceberg_table_base_location=value.get("iceberg_table_base_location"),
            iceberg_table_target_file_size=value.get("iceberg_table_target_file_size", "AUTO"),
            iceberg_table_storage_serialization_policy=value.get(
                "iceberg_table_storage_serialization_policy", "COMPATIBLE"
            ),
            iceberg_table_data_retention_time_in_days=int(
                value.get("iceberg_table_data_retention_time_in_days", 7)
            ),
            iceberg_table_max_data_extension_time_in_days=value.get(
                "iceberg_table_max_data_extension_time_in_days"
            ),
            iceberg_table_change_tracking=as_bool(value.get("iceberg_table_change_tracking"), False),
            iceberg_table_copy_grants=as_bool(value.get("iceberg_table_copy_grants"), False),
            iceberg_table_error_logging=as_bool(value.get("iceberg_table_error_logging"), True),
            iceberg_table_iceberg_version=int(value.get("iceberg_table_iceberg_version", 3)),
            iceberg_table_enable_iceberg_merge_on_read=as_bool(
                value.get("iceberg_table_enable_iceberg_merge_on_read"), True
            ),
            iceberg_table_enable_data_compaction=as_bool(
                value.get("iceberg_table_enable_data_compaction"), True
            ),
            partition_by=[str(item) for item in ensure_list(value.get("partition_by"))],
            cluster_by=[str(item) for item in ensure_list(value.get("cluster_by"))],
            target_relation=RelationConfig.from_dict(value["target_relation"], "target"),
            internal_relation=RelationConfig.from_dict(value["internal_relation"], "internal"),
            model_sql=value.get("model_sql") or "",
            model_unique_id=value.get("model_unique_id"),
            invocation_id=value.get("invocation_id"),
            dbt_full_refresh=as_bool(value.get("dbt_full_refresh"), False),
            deployment=DeploymentConfig.from_dict(value.get("deployment")),
        )
        config.validate()
        return config

    def validate(self) -> None:
        if self.source_type != "bigquery":
            raise ConfigError("source_type='bigquery' is the only supported source type.")
        if self.materialization_strategy not in {"full_refresh", "incremental"}:
            raise ConfigError("materialization_strategy must be 'full_refresh' or 'incremental'.")
        if self.bigquery_export_strategy not in {"extract", "select"}:
            raise ConfigError("bigquery_export_strategy must be 'extract' or 'select'.")
        if self.bigquery_export_predicate_type not in {
            "auto",
            "none",
            "partition_decorator",
            "table_suffix",
            "where",
        }:
            raise ConfigError("Unsupported bigquery_export_predicate_type.")
        if self.bigquery_export_strategy == "extract" and self.bigquery_export_predicate_type == "where":
            raise ConfigError("bigquery_export_strategy='extract' does not support where predicates.")
        if self.bigquery_export_strategy == "select" and self.bigquery_export_predicate_type not in {
            "auto",
            "none",
            "where",
        }:
            raise ConfigError("bigquery_export_strategy='select' supports only none, where, or auto.")
        if self.incremental_strategy != "delete+copy":
            raise ConfigError("Only incremental_strategy='delete+copy' is supported.")
        if self.partition_by:
            raise ConfigError("partition_by is not supported in the first scope.")
        if self.cluster_by:
            raise ConfigError("cluster_by is not supported in the first scope.")

        required = {
            "google_cloud_project_id": self.google_cloud_project_id,
            "bigquery_dataset_id": self.bigquery_dataset_id,
            "bigquery_table_id": self.bigquery_table_id,
            "bigquery_location": self.bigquery_location,
            "bigquery_export_location": self.bigquery_export_location,
            "iceberg_table_external_volume": self.iceberg_table_external_volume,
        }
        missing = [key for key, val in required.items() if val in {None, ""}]
        if missing:
            raise ConfigError("Missing required model config: " + ", ".join(missing))

        if self.bigquery_export_strategy == "select":
            if not self.bigquery_staging_dataset_id:
                raise ConfigError(
                    "bigquery_staging_dataset_id is required when bigquery_export_strategy='select'."
                )
            if not self.model_sql.strip():
                raise ConfigError("Model SQL is required when bigquery_export_strategy='select'.")

    def effective_mode(self, internal_table_exists: bool) -> EffectiveMode:
        if self.dbt_full_refresh:
            return "full_refresh"
        if self.materialization_strategy == "full_refresh":
            return "full_refresh"
        if not internal_table_exists:
            return "full_refresh"
        return "incremental"

    def predicates_for_mode(self, mode: EffectiveMode) -> list[str]:
        if mode == "full_refresh":
            return self.bigquery_export_full_refresh_predicates
        return self.bigquery_export_incremental_predicates

    def validate_incremental_pairing(self, mode: EffectiveMode) -> None:
        if mode != "incremental":
            return
        has_bigquery_predicates = bool(self.bigquery_export_incremental_predicates)
        has_snowflake_predicate = bool(self.incremental_predicate)
        if has_bigquery_predicates != has_snowflake_predicate:
            raise ConfigError(
                "Incremental mode requires BigQuery predicates and incremental_predicate "
                "to be both present or both absent."
            )
