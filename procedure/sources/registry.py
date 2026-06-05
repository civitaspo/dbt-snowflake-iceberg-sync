"""Source adapter registry."""

from __future__ import annotations

import json
from collections.abc import Callable

from ..config import IcebergSyncConfig
from ..errors import IcebergSyncError
from ..utils import load_snowflake_secret
from .base import SourceAdapter
from .bigquery import BigQueryRestClient, BigQuerySourceAdapter

SourceAdapterFactory = Callable[[IcebergSyncConfig], SourceAdapter]


def default_source_adapter_factories() -> dict[str, SourceAdapterFactory]:
    return {"bigquery": _bigquery_adapter}


def create_source_adapter(
    config: IcebergSyncConfig,
    factories: dict[str, SourceAdapterFactory] | None = None,
) -> SourceAdapter:
    registry = default_source_adapter_factories() if factories is None else factories
    factory = registry.get(config.source_type)
    if factory is None:
        raise IcebergSyncError(f"unsupported source_type: {config.source_type}")
    return factory(config)


def _bigquery_adapter(config: IcebergSyncConfig) -> SourceAdapter:
    alias = config.deployment.google_cloud_service_account_secret_alias
    if not alias:
        raise IcebergSyncError(
            "google_cloud_service_account_secret_alias is required to call the BigQuery API"
        )
    secret = load_snowflake_secret(alias)
    google_cloud_service_account_info = json.loads(secret, strict=False)
    return BigQuerySourceAdapter(BigQueryRestClient(google_cloud_service_account_info))
