"""Package-specific exception types."""


class IcebergSyncError(Exception):
    """Base error for user-visible sync failures."""


class ConfigError(IcebergSyncError):
    """Raised when dbt or procedure configuration is invalid."""


class SourceError(IcebergSyncError):
    """Raised when the external source cannot be planned or exported."""

    def __init__(self, message: str, *, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


class SchemaError(IcebergSyncError):
    """Raised when schema mapping or compatibility checks fail."""


class SnowflakeExecutionError(IcebergSyncError):
    """Raised when Snowflake execution fails."""
