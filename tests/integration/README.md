# Integration Tests

Integration tests are intentionally opt-in because they require live Snowflake,
BigQuery, and GCS resources.

Set `DBT_SNOWFLAKE_ICEBERG_SYNC_RUN_INTEGRATION=1` and provide the environment
variables documented in the root README before running:

```bash
uv run pytest -m integration
```
