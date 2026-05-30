from __future__ import annotations

from procedure.config import parse_config
from procedure.sources import registry


class FakeBigQueryRestClient:
    def __init__(self, google_cloud_service_account_info):
        self.google_cloud_service_account_info = google_cloud_service_account_info


def test_bigquery_adapter_accepts_snowflake_secret_with_literal_key_newlines(
    base_payload, monkeypatch
):
    secret = '{"type":"service_account","private_key":"-----BEGIN-----\n-----END-----"}'
    captured = {}

    def fake_client(service_account_info):
        captured["service_account_info"] = service_account_info
        return FakeBigQueryRestClient(service_account_info)

    monkeypatch.setattr(registry, "load_snowflake_secret", lambda alias: secret)
    monkeypatch.setattr(registry, "BigQueryRestClient", fake_client)

    adapter = registry.create_source_adapter(parse_config(base_payload))

    assert adapter.source_type == "bigquery"
    assert captured["service_account_info"]["private_key"] == "-----BEGIN-----\n-----END-----"
