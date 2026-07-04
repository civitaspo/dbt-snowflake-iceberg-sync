"""GCP credential construction for the iceberg_sync procedure.

Two authentication methods are supported:

- ``service_account_key``: a static service account key JSON stored in a
  Snowflake generic secret and read through
  ``_snowflake.get_generic_secret_string``.
- ``workload_identity_federation``: Snowflake outbound workload identity
  federation. The procedure issues a short-lived Snowflake JWT with
  ``SYSTEM$ISSUE_WORKLOAD_IDENTITY_FEDERATION_TOKEN`` through the Snowpark
  session, exchanges it at Google STS via
  ``google.auth.identity_pool.Credentials``, and optionally impersonates a
  service account. No static key material is involved.

Security note: WIF secrets cannot be read with
``_snowflake.get_generic_secret_string``. The subject token only ever exists
in memory and is never logged; only the token-issuing SQL text (which contains
no credential material) may appear in error messages.
"""

from __future__ import annotations

import base64
import json
import time
from typing import Any, Callable

from .config import DeploymentConfig
from .errors import ConfigError, SourceError
from .sql import string_literal

GCP_SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]
STS_TOKEN_URL = "https://sts.googleapis.com/v1/token"
SUBJECT_TOKEN_TYPE_JWT = "urn:ietf:params:oauth:token-type:jwt"

# Snowflake WIF tokens live 15 minutes; refresh a bit before expiry so a token
# is never presented to STS moments before it lapses.
_SUBJECT_TOKEN_REFRESH_MARGIN_SECONDS = 120


def decode_jwt_expiry(token: str) -> float | None:
    """Best-effort read of the ``exp`` claim from a JWT, without verification.

    The token was just issued by Snowflake over a trusted channel; the claim is
    only used to schedule client-side refresh, so skipping signature checks is
    safe here.
    """
    parts = token.split(".")
    if len(parts) != 3:
        return None
    payload = parts[1] + "=" * (-len(parts[1]) % 4)
    try:
        claims = json.loads(base64.urlsafe_b64decode(payload.encode("ascii")))
    except (ValueError, TypeError):
        return None
    exp = claims.get("exp") if isinstance(claims, dict) else None
    if isinstance(exp, (int, float)) and not isinstance(exp, bool):
        return float(exp)
    return None


def normalize_wif_audience(value: str) -> tuple[str, str]:
    """Return ``(sts_audience, jwt_audience)`` for a workload identity provider.

    GCP uses two spellings of the same resource name: the STS exchange request
    takes ``//iam.googleapis.com/...`` while the default allowed JWT audience
    is ``https://iam.googleapis.com/...``. Accept either form in config and
    derive both.
    """
    audience = (value or "").strip()
    if audience.startswith("https://iam.googleapis.com/"):
        return audience[len("https:") :], audience
    if audience.startswith("//iam.googleapis.com/"):
        return audience, "https:" + audience
    raise ConfigError(
        "gcp_wif_audience must be a workload identity provider resource name in the form "
        "//iam.googleapis.com/projects/<project_number>/locations/global/"
        "workloadIdentityPools/<pool_id>/providers/<provider_id>."
    )


class SnowflakeWifSubjectTokenSupplier:
    """Issues Snowflake workload identity federation JWTs on demand.

    Implements the ``google.auth.identity_pool.SubjectTokenSupplier`` protocol
    (duck-typed) so it can be plugged into ``identity_pool.Credentials``.
    google-auth calls ``get_subject_token`` again on every credential refresh,
    so cached tokens are re-issued shortly before their ``exp``.
    """

    def __init__(self, session: Any, secret_fqdn: str, jwt_audience: str) -> None:
        self._session = session
        self._secret_fqdn = secret_fqdn
        self._jwt_audience = jwt_audience
        self._cached_token: str | None = None
        self._cached_expiry: float | None = None

    def get_subject_token(self, context: Any = None, request: Any = None) -> str:
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
            f"{string_literal(self._secret_fqdn)}, {string_literal(claims)})"
        )
        try:
            rows = self._session.sql(query).collect()
        except Exception as exc:
            raise SourceError(
                "Failed to issue a Snowflake workload identity federation token for secret "
                f"{self._secret_fqdn}. The calling role needs USAGE on the secret."
            ) from exc
        token = rows[0][0] if rows and len(rows[0]) > 0 else None
        if not token:
            raise SourceError(
                "SYSTEM$ISSUE_WORKLOAD_IDENTITY_FEDERATION_TOKEN returned an empty token for "
                f"secret {self._secret_fqdn}."
            )
        return str(token)


def build_gcp_credentials(
    session: Any,
    deployment: DeploymentConfig,
    secret_reader: Callable[[str], str],
) -> Any:
    """Build google-auth credentials according to the deployment auth method."""
    if deployment.gcp_auth_method == "workload_identity_federation":
        return _build_wif_credentials(session, deployment)
    return build_service_account_credentials(secret_reader(deployment.gcp_sa_secret_alias))


def build_service_account_credentials(service_account_json: str) -> Any:
    try:
        from google.oauth2 import service_account
    except ImportError as exc:
        raise SourceError("google-auth is required inside the Snowflake procedure.") from exc

    info = json.loads(service_account_json)
    return service_account.Credentials.from_service_account_info(info, scopes=GCP_SCOPES)


def _build_wif_credentials(session: Any, deployment: DeploymentConfig) -> Any:
    try:
        from google.auth import identity_pool
    except ImportError as exc:
        raise SourceError("google-auth is required inside the Snowflake procedure.") from exc

    sts_audience, jwt_audience = normalize_wif_audience(deployment.gcp_wif_audience or "")
    supplier = SnowflakeWifSubjectTokenSupplier(
        session,
        deployment.gcp_wif_secret_fqdn or "",
        jwt_audience,
    )
    kwargs: dict[str, Any] = {
        "audience": sts_audience,
        "subject_token_type": SUBJECT_TOKEN_TYPE_JWT,
        "token_url": STS_TOKEN_URL,
        "subject_token_supplier": supplier,
        "scopes": GCP_SCOPES,
    }
    if deployment.gcp_service_account_impersonation:
        kwargs["service_account_impersonation_url"] = (
            "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/"
            f"{deployment.gcp_service_account_impersonation}:generateAccessToken"
        )
    try:
        return identity_pool.Credentials(**kwargs)
    except TypeError as exc:
        raise SourceError(
            "google-auth >= 2.29.0 is required for workload identity federation "
            "(subject_token_supplier support). Update the google-auth package available "
            "to the Snowflake procedure."
        ) from exc
