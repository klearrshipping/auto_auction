"""
Secrets manager: loads auction site credentials from Google Secret Manager.
Config modules import get_auction_credentials() to get username/password per site.

GCP authentication: uses Application Default Credentials (ADC). When
GOOGLE_APPLICATION_CREDENTIALS is unset, the client uses the default path, e.g.:
  C:\\Users\\<user>\\AppData\\Roaming\\gcloud\\application_default_credentials.json
(created by: gcloud auth application-default login)
"""

import json
import os
from functools import lru_cache

# Secret name used in Google Secret Manager (project: secrets-476114)
AUCTION_CREDENTIALS_SECRET_ID = "auction-sites-credentials"


def _fetch_secret_payload(secret_id: str, project_id: str | None = None) -> str:
    """Fetch secret value from Google Secret Manager."""
    try:
        from google.cloud import secretmanager
    except ImportError:
        raise ImportError(
            "google-cloud-secret-manager is required. Install with: pip install google-cloud-secret-manager"
        ) from None

    client = secretmanager.SecretManagerServiceClient()
    proj = project_id or os.environ.get("GOOGLE_CLOUD_PROJECT", "secrets-476114")
    name = f"projects/{proj}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("utf-8")


@lru_cache(maxsize=1)
def get_auction_credentials(project_id: str | None = None) -> dict:
    """
    Return credentials for all auction sites from Google Secret Manager.

    Returns dict keyed by site name, each value: {"username": "...", "password": "..."}
    Cached after first call. Pass project_id only on first call if needed.
    """
    raw = _fetch_secret_payload(AUCTION_CREDENTIALS_SECRET_ID, project_id=project_id)
    data = json.loads(raw)
    return data.get("auction_sites", data)


def get_credentials_for_site(site_name: str, project_id: str | None = None) -> dict:
    """Return username and password for a single auction site."""
    credentials = get_auction_credentials(project_id=project_id)
    return credentials.get(site_name, {})
