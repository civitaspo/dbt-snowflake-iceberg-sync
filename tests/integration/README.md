# Integration Tests

Integration tests are intentionally opt-in because they require live Snowflake,
BigQuery, and GCS resources.

Set `DBT_SNOWFLAKE_ICEBERG_SYNC_RUN_INTEGRATION=1` and provide the environment
variables documented in the root README before running:

```bash
uv run pytest -m integration
```

By default, the integration suite uses the `dbt` executable from the active
Python environment. To run the same tests with dbt Fusion, install the Fusion
CLI and set `DBT_SNOWFLAKE_ICEBERG_SYNC_DBT_EXECUTABLE` to that executable:

```bash
DBT_SNOWFLAKE_ICEBERG_SYNC_DBT_EXECUTABLE=/path/to/dbtf uv run pytest -m integration
```

The integration suite contains separate tests for:

- concrete non-partitioned BigQuery extract with `auto` and explicit `none`
- native BigQuery `DATETIME` extract mapped to Snowflake `TIMESTAMP_NTZ(6)`
- native time-partitioned extract with `auto` and explicit partition decorators
- native integer range-partitioned extract with `auto` and explicit partition decorators
- sharded table extract with all-shard and suffix-filtered plans
- query execution export with `auto`, `none`, and `where`
- staging table reuse and forced rebuild behavior
- incremental `delete+copy` over three dbt runs: create, incremental, repeat
- invalid parameter combinations that must fail at the dbt/procedure boundary

Mocked procedure-level retry and failed-initial-run cleanup behavior is covered
by unit tests, not the live integration suite.

Fixture tables are supplied by environment variables. The tests create temporary
Snowflake procedures, views, Iceberg tables, run logs, BigQuery extract jobs, and
GCS files, but they do not create or delete the BigQuery fixture tables. See the
root README for the complete environment variable list.
