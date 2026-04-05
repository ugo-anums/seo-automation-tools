"""OAuth 2.0 authentication for Google Search Console API."""

import os
import json
from pathlib import Path

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]
TOKEN_FILE = "token.json"
CREDENTIALS_FILE = "credentials.json"


def get_credentials(credentials_path: str = CREDENTIALS_FILE, token_path: str = TOKEN_FILE) -> Credentials:
    """Load or create OAuth credentials with token refresh support."""
    creds = None

    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
        _save_token(creds, token_path)
    elif not creds or not creds.valid:
        if not os.path.exists(credentials_path):
            raise FileNotFoundError(
                f"Missing {credentials_path}. Download your OAuth client credentials from "
                "Google Cloud Console → APIs & Services → Credentials → OAuth 2.0 Client ID "
                "(Desktop app type) and save the JSON file as 'credentials.json' in the project root."
            )
        flow = InstalledAppFlow.from_client_secrets_file(credentials_path, SCOPES)
        creds = flow.run_local_server(port=0)
        _save_token(creds, token_path)

    return creds


def _save_token(creds: Credentials, token_path: str):
    """Persist token to disk for reuse across sessions."""
    with open(token_path, "w") as f:
        f.write(creds.to_json())


def build_gsc_service(credentials_path: str = CREDENTIALS_FILE, token_path: str = TOKEN_FILE):
    """Build and return an authenticated Search Console service client."""
    creds = get_credentials(credentials_path, token_path)
    return build("searchconsole", "v1", credentials=creds)


def list_sites(service) -> list[dict]:
    """List all verified sites in the authenticated GSC account."""
    site_list = service.sites().list().execute()
    return site_list.get("siteEntry", [])
