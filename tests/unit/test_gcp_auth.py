from __future__ import annotations

import base64
import json
import time

import pytest

from procedure import gcp_auth
from procedure.config import DeploymentConfig
from procedure.errors import ConfigError, SourceError
from procedure.gcp_auth import (
    SnowflakeWorkloadIdentityFederationSubjectTokenSupplier,
    build_gcp_credentials,
    decode_jwt_expiry,
    normalize_workload_identity_federation_audience,
)

AUDIENCE = (
    "//iam.googleapis.com/projects/000000000000/locations/global/"
    "workloadIdentityPools/example-pool/providers/example-provider"
)


def make_jwt(claims: dict[str, object]) -> str:
    def encode(part: dict[str, object]) -> str:
        return base64.urlsafe_b64encode(json.dumps(part).encode()).rstrip(b"=").decode()

    return f"{encode({'alg': 'RS256'})}.{encode(claims)}.signature"


class FakeQueryResult:
    def __init__(self, rows):
        self._rows = rows

    def collect(self):
        return self._rows


class FakeSession:
    def __init__(self, token):
        self.token = token
        self.queries = []

    def sql(self, query):
        self.queries.append(query)
        return FakeQueryResult([[self.token]])


def workload_identity_federation_deployment(**overrides) -> DeploymentConfig:
    value = {
        "gcp_auth_method": "workload_identity_federation",
        "gcp_wif_secret_fqdn": "DB.AUTH.WORKLOAD_IDENTITY_FEDERATION_GCP",
        "gcp_wif_audience": AUDIENCE,
    }
    value.update(overrides)
    return DeploymentConfig(**value)


def test_decode_jwt_expiry_reads_exp_claim():
    assert decode_jwt_expiry(make_jwt({"exp": 1234567890})) == 1234567890.0


def test_decode_jwt_expiry_tolerates_invalid_tokens():
    assert decode_jwt_expiry("not-a-jwt") is None
    assert decode_jwt_expiry("a.%%%.c") is None
    assert decode_jwt_expiry(make_jwt({"sub": "no-exp"})) is None


def test_normalize_workload_identity_federation_audience_accepts_both_spellings():
    assert normalize_workload_identity_federation_audience(AUDIENCE) == (
        AUDIENCE,
        "https:" + AUDIENCE,
    )
    assert normalize_workload_identity_federation_audience("https:" + AUDIENCE) == (
        AUDIENCE,
        "https:" + AUDIENCE,
    )


def test_normalize_workload_identity_federation_audience_rejects_other_values():
    with pytest.raises(ConfigError, match="gcp_wif_audience"):
        normalize_workload_identity_federation_audience(
            "projects/000000000000/locations/global/workloadIdentityPools/p"
        )


def test_supplier_issues_token_with_escaped_literals():
    token = make_jwt({"exp": time.time() + 900})
    session = FakeSession(token)
    supplier = SnowflakeWorkloadIdentityFederationSubjectTokenSupplier(
        session,
        "DB.AUTH.O'BRIEN",
        "https:" + AUDIENCE,
    )

    assert supplier.get_subject_token() == token
    assert len(session.queries) == 1
    query = session.queries[0]
    assert query.startswith("SELECT SYSTEM$ISSUE_WORKLOAD_IDENTITY_FEDERATION_TOKEN(")
    assert "'DB.AUTH.O''BRIEN'" in query
    assert json.dumps({"aud": "https:" + AUDIENCE}).replace("'", "''") in query


def test_supplier_caches_token_until_near_expiry():
    token = make_jwt({"exp": time.time() + 900})
    session = FakeSession(token)
    supplier = SnowflakeWorkloadIdentityFederationSubjectTokenSupplier(
        session,
        "DB.AUTH.WORKLOAD_IDENTITY_FEDERATION_GCP",
        "https:" + AUDIENCE,
    )

    assert supplier.get_subject_token() == token
    assert supplier.get_subject_token() == token
    assert len(session.queries) == 1


def test_supplier_reissues_token_close_to_expiry():
    token = make_jwt({"exp": time.time() + 30})
    session = FakeSession(token)
    supplier = SnowflakeWorkloadIdentityFederationSubjectTokenSupplier(
        session,
        "DB.AUTH.WORKLOAD_IDENTITY_FEDERATION_GCP",
        "https:" + AUDIENCE,
    )

    supplier.get_subject_token()
    supplier.get_subject_token()
    assert len(session.queries) == 2


def test_supplier_rejects_empty_token():
    session = FakeSession(None)
    supplier = SnowflakeWorkloadIdentityFederationSubjectTokenSupplier(
        session,
        "DB.AUTH.WORKLOAD_IDENTITY_FEDERATION_GCP",
        "https:" + AUDIENCE,
    )

    with pytest.raises(SourceError, match="empty token"):
        supplier.get_subject_token()


def test_supplier_wraps_sql_failures():
    class FailingSession:
        def sql(self, query):
            del query
            raise RuntimeError("insufficient privileges")

    supplier = SnowflakeWorkloadIdentityFederationSubjectTokenSupplier(
        FailingSession(),
        "DB.AUTH.WORKLOAD_IDENTITY_FEDERATION_GCP",
        "https:" + AUDIENCE,
    )

    with pytest.raises(SourceError, match="USAGE on the secret"):
        supplier.get_subject_token()


def test_build_workload_identity_federation_credentials_configures_identity_pool():
    from google.auth import identity_pool

    session = FakeSession(make_jwt({"exp": time.time() + 900}))
    credentials = build_gcp_credentials(
        session,
        workload_identity_federation_deployment(),
        secret_reader=lambda alias: pytest.fail(
            f"workload identity federation must not read generic secrets: {alias}"
        ),
    )

    assert isinstance(credentials, identity_pool.Credentials)
    assert credentials._audience == AUDIENCE
    assert credentials._subject_token_type == gcp_auth.SUBJECT_TOKEN_TYPE_JWT
    assert credentials._service_account_impersonation_url is None
    assert isinstance(
        credentials._subject_token_supplier,
        SnowflakeWorkloadIdentityFederationSubjectTokenSupplier,
    )


def test_build_workload_identity_federation_credentials_with_impersonation():
    session = FakeSession(make_jwt({"exp": time.time() + 900}))
    credentials = build_gcp_credentials(
        session,
        workload_identity_federation_deployment(
            gcp_service_account_impersonation="sync@example-project.iam.gserviceaccount.com"
        ),
        secret_reader=lambda alias: pytest.fail(
            f"workload identity federation must not read generic secrets: {alias}"
        ),
    )

    assert credentials._service_account_impersonation_url == (
        "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/"
        "sync@example-project.iam.gserviceaccount.com:generateAccessToken"
    )


def test_build_gcp_credentials_defaults_to_service_account_key(monkeypatch):
    seen = {}

    def fake_builder(service_account_json):
        seen["json"] = service_account_json
        return "sa-credentials"

    monkeypatch.setattr(gcp_auth, "build_service_account_credentials", fake_builder)
    deployment = DeploymentConfig(google_cloud_service_account_secret_alias="my_alias")

    credentials = build_gcp_credentials(
        session=None,
        deployment=deployment,
        secret_reader=lambda alias: f"secret-for-{alias}",
    )

    assert credentials == "sa-credentials"
    assert seen["json"] == "secret-for-my_alias"


def test_build_gcp_credentials_requires_session_for_workload_identity_federation():
    with pytest.raises(SourceError, match="Snowpark session"):
        build_gcp_credentials(
            session=None,
            deployment=workload_identity_federation_deployment(),
            secret_reader=lambda alias: alias,
        )
