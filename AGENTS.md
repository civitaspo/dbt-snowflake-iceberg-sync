# Agent Notes

- Keep repository text, comments, tests, and documentation in English.
- Use Conventional Commits for pull request titles (`feat`, `fix`, `docs`,
  `refactor`, `test`, `ci`, `build`, `chore`, `perf`, `revert`; use `!` for
  breaking changes).
- Never push directly to `main`. Open a pull request and squash-merge after
  required checks pass. Sign commits.
- Do not edit `CHANGELOG.md` on feature PRs; the Release PR owns it via git-cliff.
- Do not add organization-specific identifiers, account names, schemas, stages, or secrets.
- Keep credential material out of dbt model config, compiled SQL, logs, and tests.
- Strong credentials live only in `civitaspo/securefix-server`.
- Integration tests must stay opt-in and controlled by environment variables.
- Prefer small, reviewable changes and mocked unit tests for default CI.

## Tooling

- Install pinned tools with mise:

```bash
mise install --locked
```

- Use `uv run` consistently for Python, dbt, lint, and test commands.
- Keep `uv`, ShellCheck, git-cliff, ghalint, pinact, and
  disable-checkout-persist-credentials managed by mise.
- Before opening a pull request, run `mise run lint` plus the verification
  commands below.
- Do not hide CI workflows behind mise tasks; keep the failing command visible in the GitHub Actions step.

## GitHub Actions

- Pin public GitHub Actions to immutable SHAs.
- Use `persist-credentials: false` with `actions/checkout` unless a workflow explicitly needs push credentials.
- Keep workflow permissions least-privilege and job names descriptive.
- Run workflow linting with ghalint, pinact, and disable-checkout-persist-credentials.
- Use Securefix for automated workflow security fixes when configured.
- Approvals for trusted authors are requested through `csm-actions/approve-pr-action`.
- Do not provide hidden defaults for required repository variables in workflows; fail clearly when required configuration is missing.
- Keep live integration CI approval-only. Do not pass Snowflake, BigQuery, GCS,
  fixture values, or credentials through GitHub Actions secrets.

See [docs/securefix.md](docs/securefix.md) and [docs/releasing.md](docs/releasing.md).

## Verification

For local unit checks, run:

```bash
uv run ruff check procedure tests
uv run pytest tests/unit
uv run dbt parse --profiles-dir tests/ci_profiles --no-version-check
```

For dbt Fusion validation, run:

```bash
dbtf parse --profiles-dir tests/ci_profiles --no-version-check
```

Do not pass partial-parse flags to Fusion commands.

For integration behavior, use the opt-in integration tests and keep Snowflake
access through approved company tooling.

Integration tests are intentionally skipped unless
`DBT_SNOWFLAKE_ICEBERG_SYNC_RUN_INTEGRATION=1` is set. Supply BigQuery, GCS, S3,
and Snowflake fixture/resource settings through environment variables only; do not
commit company project IDs, account names, schemas, stages, fixture table names,
or credential values. See `README.md` and `tests/integration/README.md` for the
complete environment variable list.

The integration suite should cover more than a happy path. Keep coverage for:

- non-partitioned BigQuery extract
- BigQuery `DATETIME` extract mapped to Snowflake `TIMESTAMP_NTZ(6)`
- BigQuery `TIMESTAMP` extract mapped to Snowflake `TIMESTAMP_NTZ(6)`
- time-partitioned extract through partition decorators
- integer range-partitioned extract through partition decorators
- sharded BigQuery extract through wildcard and table suffix plans
- query execution export through BigQuery staging tables with `auto`, `none`,
  and `where` predicates
- BigQuery staging table reuse and forced rebuild behavior
- incremental `delete+copy`, including a repeated incremental run
- procedure-level Snowflake retry and failed-initial-run cleanup behavior
- invalid dbt/materialization parameter combinations that must fail
- S3 Parquet full refresh from a generated prefix
- S3 Parquet pattern-filtered load
- S3 Parquet incremental `delete+copy` with a repeated run (`FORCE = TRUE`)
- S3 Parquet empty-location skip vs fail behavior
- S3 Parquet additive schema evolution
- nested `OBJECT` / `ARRAY(OBJECT)` schema evolution matrix (add, reorder,
  widen, keep-missing+warn, deep nest, incompatible type fail, combined)
- BigQuery nested `STRUCT` / `RECORD` field add (select export)
- S3 Parquet invalid incremental path/predicate combinations

Run the opt-in suite with:

```bash
uv run pytest -m integration tests/integration
```

To run integration tests with dbt Fusion, set
`DBT_SNOWFLAKE_ICEBERG_SYNC_DBT_EXECUTABLE` to the `dbtf` executable. Relative
or absolute `vars.iceberg_sync.handler_local_path` values both work; the package
absolute-izes relative paths before Snowflake `PUT file://...`, preferring
`DBT_PROJECT_DIR` when set so Fusion and `--project-dir` runs do not depend on
the CLI working directory.

The tests may create temporary Snowflake procedures, views, Iceberg tables, run
logs, BigQuery extract jobs, GCS files, and S3 Parquet files under generated
prefixes. They should not create or delete caller-provided BigQuery fixture
tables. S3 Parquet fixtures are generated by unloading from Snowflake into the
caller-provided S3 stage.

The GitHub Actions integration workflow checks only PR approval state. It should
pass when the current PR head has a fresh approval from an `OWNER`, `MEMBER`, or
`COLLABORATOR`, excluding the PR author, and should bypass approval when the PR
author is a repository owner. Run live integration tests outside GitHub with
company-managed credentials after that approval.
