"""
Secrets manager: loads auction site credentials from Google Secret Manager.
Config modules import get_auction_credentials() to get username/password per site.

GCP project: Set GCP_SECRETS_PROJECT env var or config.config.GCP_SECRETS_PROJECT.
GCP authentication: uses Application Default Credentials (ADC). When
GOOGLE_APPLICATION_CREDENTIALS is unset, the client uses the default path, e.g.:
  C:\\Users\\<user>\\AppData\\Roaming\\gcloud\\application_default_credentials.json
(created by: gcloud auth application-default login)
"""

import json
import os
from functools import lru_cache

# Secret name used in Google Secret Manager
AUCTION_CREDENTIALS_SECRET_ID = "auction-credentials"


def _get_project_id(project_id: str | None = None) -> str:
    """Resolve GCP project ID: arg > env GCP_SECRETS_PROJECT > config default."""
    if project_id:
        return project_id
    if os.environ.get("GCP_SECRETS_PROJECT"):
        return os.environ["GCP_SECRETS_PROJECT"]
    try:
        from config.config import GCP_SECRETS_PROJECT
        return GCP_SECRETS_PROJECT
    except ImportError:
        return os.environ.get("GOOGLE_CLOUD_PROJECT", "email-automation-490620")


def _fetch_secret_payload(secret_id: str, project_id: str | None = None) -> str:
    """Fetch secret value from Google Secret Manager."""
    try:
        from google.cloud import secretmanager
    except ImportError:
        raise ImportError(
            "google-cloud-secret-manager is required. Install with: pip install google-cloud-secret-manager"
        ) from None

    client = secretmanager.SecretManagerServiceClient()
    proj = _get_project_id(project_id)
    name = f"projects/{proj}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("utf-8")


def _normalize_credentials(data) -> dict:
    """Convert secret payload to dict keyed by site name with lowercase keys."""
    if isinstance(data, dict) and "auction_sites" in data:
        data = data["auction_sites"]
    if isinstance(data, list):
        return {
            item.get("Site", item.get("site", "")): {
                "username": item.get("Username", item.get("username", "")),
                "password": item.get("Password", item.get("password", "")),
            }
            for item in data
        }
    if isinstance(data, dict):
        return {k: {"username": v.get("username", ""), "password": v.get("password", "")} for k, v in data.items()}
    return {}


@lru_cache(maxsize=4)
def _get_auction_credentials_impl(project_id: str) -> dict:
    """Internal: fetch credentials for a specific project. Cached per project."""
    raw = _fetch_secret_payload(AUCTION_CREDENTIALS_SECRET_ID, project_id=project_id)
    data = json.loads(raw)
    return _normalize_credentials(data)


def get_auction_credentials(project_id: str | None = None) -> dict:
    """
    Return credentials for all auction sites from Google Secret Manager.

    Project: GCP_SECRETS_PROJECT env, or config.config.GCP_SECRETS_PROJECT.
    Returns dict keyed by site name, each value: {"username": "...", "password": "..."}
    Cached per project after first call.
    """
    proj = _get_project_id(project_id)
    return _get_auction_credentials_impl(proj)


def get_credentials_for_site(site_name: str, project_id: str | None = None) -> dict:
    """Return username and password for a single auction site."""
    credentials = get_auction_credentials(project_id=project_id)
    return credentials.get(site_name, {})
