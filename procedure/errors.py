"""Package-specific exception types."""


class IcebergSyncError(Exception):
    """Base error for user-visible sync failures."""


class ConfigError(IcebergSyncError):
    """Raised when dbt or procedure configuration is invalid."""


class SourceError(IcebergSyncError):
    """Raised when the external source cannot be planned or exported."""


class SchemaError(IcebergSyncError):
    """Raised when schema mapping or compatibility checks fail."""


class SnowflakeExecutionError(IcebergSyncError):
    """Raised when Snowflake execution fails."""
