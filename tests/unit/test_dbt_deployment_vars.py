"""Macro-level tests for top-level dbt var overrides of WIF deployment keys.

dbt renders jinja only in top-level string vars, never inside the nested
vars.iceberg_sync map, so per-target values must be configurable through
dedicated top-level vars. These tests run a probe macro through a throwaway
dbt + duckdb project and assert the resolution precedence:
top-level iceberg_sync_<key> var > nested vars.iceberg_sync.<key> > default.
"""

import json
import os
import re
import shutil
import subprocess
from pathlib import Path


NESTED_AUDIENCE = (
    "//iam.googleapis.com/projects/000000000000/locations/global/"
    "workloadIdentityPools/nested-pool/providers/nested-provider"
)
TOP_LEVEL_AUDIENCE_TEMPLATE = (
    "//iam.googleapis.com/projects/000000000001/locations/global/"
    "workloadIdentityPools/{{ target.name }}-pool/providers/{{ target.name }}-provider"
)


def _write_project(project_dir, tmp_path, repo_root):
    macros_dir = project_dir / "macros"
    macros_dir.mkdir(parents=True)
    (project_dir / "dbt_project.yml").write_text(
        f"""
name: dbt_snowflake_iceberg_sync_var_tests
version: 0.1.0
config-version: 2
profile: dbt_snowflake_iceberg_sync_var_tests
vars:
  iceberg_sync:
    procedure_database: DB
    procedure_schema: UTIL
    procedure_name: ICEBERG_SYNC
    gcp_auth_method: workload_identity_federation
    gcp_wif_secret_fqdn: DB.AUTH.NESTED_WIF
    gcp_wif_audience: {NESTED_AUDIENCE}
  iceberg_sync_gcp_wif_audience: "{TOP_LEVEL_AUDIENCE_TEMPLATE}"
  iceberg_sync_gcp_service_account_impersonation: "sync-{{{{ target.name }}}}@example-project.iam.gserviceaccount.com"
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
dbt_snowflake_iceberg_sync_var_tests:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: {tmp_path / "var_tests.duckdb"}
""".strip(),
        encoding="utf-8",
    )
    (macros_dir / "print_wif_deployment_vars.sql").write_text(
        """
{% macro print_wif_deployment_vars() %}
  {% set deployment = var('iceberg_sync', {}) %}
  {% set resolved = {
    'gcp_auth_method': dbt_snowflake_iceberg_sync.iceberg_sync_deployment_var(deployment, 'gcp_auth_method', 'service_account_key'),
    'gcp_wif_secret_fqdn': dbt_snowflake_iceberg_sync.iceberg_sync_deployment_var(deployment, 'gcp_wif_secret_fqdn'),
    'gcp_wif_audience': dbt_snowflake_iceberg_sync.iceberg_sync_deployment_var(deployment, 'gcp_wif_audience'),
    'gcp_service_account_impersonation': dbt_snowflake_iceberg_sync.iceberg_sync_deployment_var(deployment, 'gcp_service_account_impersonation')
  } %}
  {{ log("WIF_DEPLOYMENT_VARS=" ~ tojson(resolved), info=true) }}
{% endmacro %}
""".strip(),
        encoding="utf-8",
    )


def test_top_level_vars_override_nested_wif_keys(tmp_path):
    dbt = shutil.which("dbt")
    assert dbt is not None, "dbt executable is required for macro tests"

    repo_root = Path(__file__).resolve().parents[2]
    project_dir = tmp_path / "dbt_var_tests"
    _write_project(project_dir, tmp_path, repo_root)

    env = {
        **os.environ,
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
    }
    base_args = ["--project-dir", str(project_dir), "--profiles-dir", str(project_dir)]

    deps = subprocess.run(
        [dbt, "deps", *base_args],
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    assert deps.returncode == 0, deps.stdout + deps.stderr

    run_operation = subprocess.run(
        [dbt, "run-operation", "print_wif_deployment_vars", *base_args],
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    assert run_operation.returncode == 0, run_operation.stdout + run_operation.stderr

    match = re.search(r"WIF_DEPLOYMENT_VARS=(\{.*\})", run_operation.stdout)
    assert match is not None, run_operation.stdout
    resolved = json.loads(match.group(1))

    # Top-level vars win and are jinja-rendered with target context.
    assert resolved["gcp_wif_audience"] == (
        "//iam.googleapis.com/projects/000000000001/locations/global/"
        "workloadIdentityPools/dev-pool/providers/dev-provider"
    )
    assert (
        resolved["gcp_service_account_impersonation"]
        == "sync-dev@example-project.iam.gserviceaccount.com"
    )
    # Keys without a top-level override fall back to the nested map.
    assert resolved["gcp_auth_method"] == "workload_identity_federation"
    assert resolved["gcp_wif_secret_fqdn"] == "DB.AUTH.NESTED_WIF"
