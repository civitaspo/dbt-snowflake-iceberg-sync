import os

import pytest


@pytest.mark.integration
def test_live_integration_requires_explicit_opt_in():
    if os.getenv("DBT_SNOWFLAKE_ICEBERG_SYNC_RUN_INTEGRATION") != "1":
        pytest.skip("Live Snowflake, BigQuery, and GCS integration tests are opt-in.")
