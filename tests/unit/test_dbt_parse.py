import os
import shutil
import subprocess
from pathlib import Path


def test_dbt_parse_fixture_compiles_package_macros(tmp_path):
    dbt = shutil.which("dbt")
    assert dbt is not None, "dbt executable is required for parse tests"

    repo_root = Path(__file__).resolve().parents[2]
    project_dir = tmp_path / "dbt_compile"
    models_dir = project_dir / "models"
    models_dir.mkdir(parents=True)
    (project_dir / "dbt_project.yml").write_text(
        """
name: dbt_snowflake_iceberg_sync_compile_tests
version: 0.1.0
config-version: 2
profile: dbt_snowflake_iceberg_sync_compile_tests
vars:
  iceberg_sync:
    procedure_database: DB
    procedure_schema: UTIL
    procedure_name: ICEBERG_SYNC
models:
  dbt_snowflake_iceberg_sync_compile_tests:
    +schema: mart
""".strip(),
        encoding="utf-8",
    )
    (project_dir / "packages.yml").write_text(
        f"""
packages:
  - local: {repo_root}
""".strip(),
        encoding="utf-8",
    )
    (project_dir / "profiles.yml").write_text(
        f"""
dbt_snowflake_iceberg_sync_compile_tests:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: {tmp_path / "compile_tests.duckdb"}
""".strip(),
        encoding="utf-8",
    )
    (models_dir / "extract_model.sql").write_text(
        """
{{
  config(
    materialized='iceberg_sync',
    source_type='bigquery',
    materialization_strategy='incremental',
    bigquery_export_strategy='extract',
    google_cloud_project_id='example-project',
    bigquery_dataset_id='analytics',
    bigquery_table_id='events',
    bigquery_location='US',
    bigquery_export_location='@DB.UTIL.BQ_EXPORT_STAGE/dbt',
    iceberg_table_external_volume='ICEBERG_VOLUME'
  )
}}

select 1 as placeholder
""".strip(),
        encoding="utf-8",
    )
    (models_dir / "select_model.sql").write_text(
        """
{{
  config(
    materialized='iceberg_sync',
    source_type='bigquery',
    materialization_strategy='incremental',
    bigquery_export_strategy='select',
    google_cloud_project_id='example-project',
    bigquery_dataset_id='analytics',
    bigquery_table_id='events',
    bigquery_location='US',
    bigquery_export_location='@DB.UTIL.BQ_EXPORT_STAGE/dbt',
    bigquery_export_predicate_type='where',
    bigquery_export_incremental_predicates=["event_date = DATE '2026-01-01'"],
    bigquery_staging_dataset_id='dbt_staging',
    incremental_predicate="event_date = DATE '2026-01-01'",
    iceberg_table_external_volume='ICEBERG_VOLUME'
  )
}}

select
  *
from `example-project.analytics.events`
""".strip(),
        encoding="utf-8",
    )

    env = {
        **os.environ,
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
    }

    deps = subprocess.run(
        [dbt, "deps", "--project-dir", str(project_dir), "--profiles-dir", str(project_dir)],
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    assert deps.returncode == 0, deps.stdout + deps.stderr

    parse = subprocess.run(
        [dbt, "parse", "--project-dir", str(project_dir), "--profiles-dir", str(project_dir)],
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    assert parse.returncode == 0, parse.stdout + parse.stderr
