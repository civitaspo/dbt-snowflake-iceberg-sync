# Repository Guidelines

## Project Scope

This repository contains `dbt-snowflake-iceberg-sync`, a dbt package that syncs external source data into Snowflake-managed Iceberg tables through a Snowflake-only materialization.

## Contributor Expectations

- Write commits, pull request descriptions, documentation, comments, and user-facing messages in English.
- Keep changes small, reviewable, and focused on the package behavior described in the README.
- Prefer clear dbt macros, explicit Python procedure modules, and behavior-focused tests over clever abstractions.
- Document security-sensitive behavior, especially credential handling and exported object cleanup semantics.
- Avoid generated files unless they are required for reproducible dependency resolution.

## Verification

For ordinary package changes, run:

```bash
uv run pytest
```

Live Snowflake, BigQuery, and GCS integration tests must be opt-in and controlled by environment variables.
