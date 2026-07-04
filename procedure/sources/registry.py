"""Source adapter registry."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from ..config import IcebergSyncConfig
from ..errors import IcebergSyncError
from ..gcp_auth import build_gcp_credentials
from ..utils import load_snowflake_secret
from .base import SourceAdapter
from .bigquery import BigQueryRestClient, BigQuerySourceAdapter

SourceAdapterFactory = Callable[[IcebergSyncConfig], SourceAdapter]


def default_source_adapter_factories(
    *, session: Any | None = None
) -> dict[str, SourceAdapterFactory]:
    return {"bigquery": lambda config: _bigquery_adapter(config, session=session)}


def create_source_adapter(
    config: IcebergSyncConfig,
    factories: dict[str, SourceAdapterFactory] | None = None,
    *,
    session: Any | None = None,
) -> SourceAdapter:
    registry = (
        default_source_adapter_factories(session=session) if factories is None else factories
    )
    factory = registry.get(config.source_type)
    if factory is None:
        raise IcebergSyncError(f"unsupported source_type: {config.source_type}")
    return factory(config)


def _bigquery_adapter(
    config: IcebergSyncConfig,
    *,
    session: Any | None = None,
) -> SourceAdapter:
    credentials = build_gcp_credentials(
        session,
        config.deployment,
        secret_reader=load_snowflake_secret,
    )
    return BigQuerySourceAdapter(BigQueryRestClient(credentials))
