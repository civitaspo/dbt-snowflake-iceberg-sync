class IcebergSyncError(Exception):
    """Base exception for procedure-visible sync failures."""


class ConfigError(IcebergSyncError):
    """Raised when the dbt-provided config is invalid."""


class SourceError(IcebergSyncError):
    """Raised when source planning or export fails."""


class SchemaError(IcebergSyncError):
    """Raised when source and target schemas are incompatible."""


class SnowflakeSyncError(IcebergSyncError):
    """Raised when Snowflake DDL or DML fails."""

