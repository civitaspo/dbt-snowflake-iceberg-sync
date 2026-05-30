# Integration Tests

Integration tests are intentionally opt-in because they require live Snowflake,
BigQuery, and GCS resources.

Set `DBT_SNOWFLAKE_ICEBERG_SYNC_RUN_INTEGRATION=1` and provide the environment
variables documented in the root README before running:

```bash
uv run pytest -m integration
```

The integration suite contains separate tests for:

- concrete non-partitioned BigQuery extract
- native time-partitioned extract through partition decorators
- native integer range-partitioned extract through partition decorators
- query execution export through a BigQuery staging table
- incremental `delete+copy` over three dbt runs: create, incremental, repeat

Fixture tables are supplied by environment variables. The tests create temporary
Snowflake procedures, views, Iceberg tables, run logs, BigQuery extract jobs, and
GCS files, but they do not create or delete the BigQuery fixture tables. See the
root README for the complete environment variable list.
