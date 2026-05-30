# Agent Notes

- Keep repository text, comments, tests, and documentation in English.
- Do not add organization-specific identifiers, account names, schemas, stages, or secrets.
- Keep credential material out of dbt model config, compiled SQL, logs, and tests.
- Integration tests must stay opt-in and controlled by environment variables.
- Prefer small, reviewable changes and mocked unit tests for default CI.

## Tooling

- Install pinned tools with mise:

```bash
mise install --locked
```

- Use `uv run` consistently for Python, dbt, lint, and test commands.
- Keep `uv`, ShellCheck, ghalint, pinact, and disable-checkout-persist-credentials managed by mise.
- Do not hide CI workflows behind mise tasks; keep the failing command visible in the GitHub Actions step.

## GitHub Actions

- Pin public GitHub Actions to immutable SHAs.
- Use `persist-credentials: false` with `actions/checkout` unless a workflow explicitly needs push credentials.
- Keep workflow permissions least-privilege and job names descriptive.
- Run workflow linting with ghalint, pinact, and disable-checkout-persist-credentials.
- Use Securefix for automated workflow security fixes when configured.
- Do not provide hidden defaults for required repository variables in workflows; fail clearly when required configuration is missing.

## Verification

For local unit checks, run:

```bash
uv run ruff check procedure tests
uv run pytest tests/unit
uv run dbt parse --profiles-dir tests/ci_profiles --no-version-check --no-partial-parse
```

For integration behavior, use the opt-in integration tests and keep Snowflake
access through approved company tooling.

Integration tests are intentionally skipped unless
`DBT_SNOWFLAKE_ICEBERG_SYNC_RUN_INTEGRATION=1` is set. Supply BigQuery, GCS, and
Snowflake fixture/resource settings through environment variables only; do not
commit company project IDs, account names, schemas, stages, fixture table names,
or credential values. See `README.md` and `tests/integration/README.md` for the
complete environment variable list.

The integration suite should cover more than a happy path. Keep coverage for:

- non-partitioned BigQuery extract
- time-partitioned extract through partition decorators
- integer range-partitioned extract through partition decorators
- query execution export through a BigQuery staging table
- incremental `delete+copy`, including a repeated incremental run

Run the opt-in suite with:

```bash
uv run pytest -m integration tests/integration
```

The tests may create temporary Snowflake procedures, views, Iceberg tables, run
logs, BigQuery extract jobs, and GCS files under generated prefixes. They should
not create or delete caller-provided BigQuery fixture tables.
