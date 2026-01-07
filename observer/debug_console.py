import requests
import sys
import urllib3
import google.auth
from google.auth.transport.requests import Request
from config import Config

# Suppress warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_token():
    try:
        creds, project = google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
        if not creds.valid:
            creds.refresh(Request())
        return creds.token, project
    except Exception as e:
        print(f"Auth Error: {e}")
        return None, None

def test_endpoint(base_url, path, token):
    url = f"{base_url.rstrip('/')}{path}"
    print(f"\nTesting: {url}")
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    try:
        resp = requests.get(url, headers=headers, verify=False, timeout=10)
        print(f"Status: {resp.status_code}")
        print(f"Response: {resp.text[:200]}...") # Truncate
        return resp.status_code == 200
    except Exception as e:
        print(f"Error: {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python debug_console.py <BASE_URL>")
        # Try to load from Config if not provided
        base_url = Config.MGMT_CONSOLE_ENDPOINT
        if not base_url:
            sys.exit(1)
    else:
        base_url = sys.argv[1]

    print(f"Authenticating...")
    token, project = get_token()
    if not token:
        print("Could not get Google Auth Token. Are you running in GCP or with ADC?")
        # Fallback for manual token if needed?
        sys.exit(1)

    paths = [
        "/actifio/api/jobstatus",
        "/actifio/api/info",
        "/actifio/api/login",
        "/api/jobstatus",
        "/api/info",
        "/jobstatus",
        "/act/api/jobstatus"
    ]

    print(f"Probing {base_url}...")
    for path in paths:
        if test_endpoint(base_url, path, token):
            print(f"--> SUCCESS found at {path}")
