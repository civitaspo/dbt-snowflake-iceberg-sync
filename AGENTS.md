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
