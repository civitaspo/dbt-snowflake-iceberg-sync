"""Google Cloud credential helpers for the Snowflake procedure."""

from __future__ import annotations

import base64
import json
import time
from collections.abc import Callable
from typing import Any

from .config import DeploymentConfig
from .errors import ConfigError, SourceError
from .utils import sql_string

GOOGLE_CLOUD_SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]
STS_TOKEN_URL = "https://sts.googleapis.com/v1/token"
SUBJECT_TOKEN_TYPE_JWT = "urn:ietf:params:oauth:token-type:jwt"
_SUBJECT_TOKEN_REFRESH_MARGIN_SECONDS = 120


def decode_jwt_expiry(token: str) -> float | None:
    parts = token.split(".")
    if len(parts) != 3:
        return None
    payload = parts[1] + "=" * (-len(parts[1]) % 4)
    try:
        claims = json.loads(base64.urlsafe_b64decode(payload.encode("ascii")))
    except (TypeError, ValueError):
        return None
    exp = claims.get("exp") if isinstance(claims, dict) else None
    if isinstance(exp, (int, float)) and not isinstance(exp, bool):
        return float(exp)
    return None


def normalize_workload_identity_federation_audience(value: str) -> tuple[str, str]:
    audience = (value or "").strip()
    if audience.startswith("https://iam.googleapis.com/"):
        return audience[len("https:") :], audience
    if audience.startswith("//iam.googleapis.com/"):
        return audience, "https:" + audience
    raise ConfigError(
        "google_cloud_workload_identity_federation_audience must be a workload "
        "identity provider resource in the form "
        "//iam.googleapis.com/projects/<project_number>/locations/global/"
        "workloadIdentityPools/<pool_id>/providers/<provider_id>"
    )


class SnowflakeWorkloadIdentityFederationSubjectTokenSupplier:
    """Issue Snowflake workload identity federation JWTs on demand for google-auth refreshes."""

    def __init__(self, session: Any, secret_fqdn: str, jwt_audience: str) -> None:
        self._session = session
        self._secret_fqdn = secret_fqdn
        self._jwt_audience = jwt_audience
        self._cached_token: str | None = None
        self._cached_expiry: float | None = None

    def get_subject_token(self, context: Any = None, request: Any = None) -> str:
        del context, request
        if (
            self._cached_token is not None
            and self._cached_expiry is not None
            and time.time() < self._cached_expiry - _SUBJECT_TOKEN_REFRESH_MARGIN_SECONDS
        ):
            return self._cached_token
        token = self._issue_token()
        expiry = decode_jwt_expiry(token)
        self._cached_token = token if expiry is not None else None
        self._cached_expiry = expiry
        return token

    def _issue_token(self) -> str:
        claims = json.dumps({"aud": self._jwt_audience})
        query = (
            "SELECT SYSTEM$ISSUE_WORKLOAD_IDENTITY_FEDERATION_TOKEN("
            f"{sql_string(self._secret_fqdn)}, {sql_string(claims)})"
        )
        try:
            rows = self._session.sql(query).collect()
        except Exception as exc:  # pragma: no cover - exercised via fakes
            raise SourceError(
                "Failed to issue a Snowflake workload identity federation token for "
                f"{self._secret_fqdn}. The calling role needs USAGE on the secret."
            ) from exc
        token = rows[0][0] if rows and len(rows[0]) > 0 else None
        if not token:
            raise SourceError(
                "SYSTEM$ISSUE_WORKLOAD_IDENTITY_FEDERATION_TOKEN returned an empty token."
            )
        return str(token)


def build_google_cloud_credentials(
    session: Any,
    deployment: DeploymentConfig,
    secret_reader: Callable[[str], str],
) -> Any:
    if deployment.google_cloud_auth_method == "workload_identity_federation":
        if session is None:
            raise SourceError(
                "A Snowpark session is required for "
                "google_cloud_auth_method='workload_identity_federation'"
            )
        return _build_workload_identity_federation_credentials(session, deployment)

    alias = deployment.google_cloud_service_account_secret_alias
    if not alias:
        raise SourceError(
            "google_cloud_service_account_secret_alias is required to call the BigQuery API"
        )
    return build_service_account_credentials(secret_reader(alias))


def build_service_account_credentials(service_account_json: str) -> Any:
    try:
        from google.oauth2 import service_account
    except ImportError as exc:  # pragma: no cover - import availability is environment-specific
        raise SourceError("google-auth is required inside the Snowflake procedure.") from exc

    info = json.loads(service_account_json, strict=False)
    return service_account.Credentials.from_service_account_info(info, scopes=GOOGLE_CLOUD_SCOPES)


def _build_workload_identity_federation_credentials(
    session: Any,
    deployment: DeploymentConfig,
) -> Any:
    try:
        from google.auth import identity_pool
    except ImportError as exc:  # pragma: no cover - import availability is environment-specific
        raise SourceError("google-auth is required inside the Snowflake procedure.") from exc

    secret_fqdn = deployment.google_cloud_workload_identity_federation_secret_fqdn or ""
    sts_audience, jwt_audience = normalize_workload_identity_federation_audience(
        deployment.google_cloud_workload_identity_federation_audience or ""
    )
    supplier = SnowflakeWorkloadIdentityFederationSubjectTokenSupplier(
        session,
        secret_fqdn,
        jwt_audience,
    )
    kwargs: dict[str, Any] = {
        "audience": sts_audience,
        "subject_token_type": SUBJECT_TOKEN_TYPE_JWT,
        "token_url": STS_TOKEN_URL,
        "subject_token_supplier": supplier,
        "scopes": GOOGLE_CLOUD_SCOPES,
    }
    if deployment.google_cloud_service_account_impersonation:
        kwargs["service_account_impersonation_url"] = (
            "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/"
            f"{deployment.google_cloud_service_account_impersonation}:generateAccessToken"
        )
    try:
        return identity_pool.Credentials(**kwargs)
    except TypeError as exc:
        raise SourceError(
            "google-auth >= 2.29.0 is required for workload identity federation "
            "(subject_token_supplier support)"
        ) from exc
