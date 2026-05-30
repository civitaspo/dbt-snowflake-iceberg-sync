from __future__ import annotations

from procedure.config import parse_config
from procedure.run_log import build_run_log_payload
from procedure.utils import utcnow


def test_run_log_payload_contains_required_fields(base_payload):
    config = parse_config(base_payload)
    now = utcnow()

    payload = build_run_log_payload(
        config=config,
        run_id="run-1",
        effective_mode="full_refresh",
        predicates=(),
        export_segments=[{"destination_uri": "gcs://bucket/run/*.parquet"}],
        source_job_references=[{"jobId": "job-1"}],
        staging_table_reference=None,
        snowflake_query_ids=["query-1"],
        status="success",
        error_message=None,
        started_at=now,
        finished_at=now,
    )

    assert payload["run_id"] == "run-1"
    assert payload["target_view"] == '"ANALYTICS"."PUBLIC"."orders"'
    assert payload["internal_iceberg_table"] == '"ANALYTICS"."PUBLIC"."__orders"'
    assert payload["source_job_references"] == [{"jobId": "job-1"}]
