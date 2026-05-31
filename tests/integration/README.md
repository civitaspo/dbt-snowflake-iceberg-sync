# Integration Tests

Integration tests are intentionally opt-in because they require live Snowflake,
BigQuery, and GCS resources.

Set `DBT_SNOWFLAKE_ICEBERG_SYNC_RUN_INTEGRATION=1` and provide the environment
variables documented in the root README before running:

```bash
uv run pytest -m integration
```

The integration suite contains separate tests for:

- concrete non-partitioned BigQuery extract with `auto` and explicit `none`
- native time-partitioned extract with `auto` and explicit partition decorators
- native integer range-partitioned extract with `auto` and explicit partition decorators
- sharded table extract with all-shard and suffix-filtered plans
- query execution export with `auto`, `none`, and `where`
- staging table reuse and forced rebuild behavior
- incremental `delete+copy` over three dbt runs: create, incremental, repeat
- invalid parameter combinations that must fail at the dbt/procedure boundary

Fixture tables are supplied by environment variables. The tests create temporary
Snowflake procedures, views, Iceberg tables, run logs, BigQuery extract jobs, and
GCS files, but they do not create or delete the BigQuery fixture tables. See the
root README for the complete environment variable list.
