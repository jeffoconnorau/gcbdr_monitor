import requests
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))
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

def create_session(base_url, token):
    url = f"{base_url.rstrip('/')}/actifio/session"
    print(f"\nCreating Session at: {url}")
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Length": "0"
    }
    try:
        resp = requests.post(url, headers=headers, verify=False, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            session_id = data.get('session_id')
            print(f"Session Created! ID: {session_id}")
            return session_id
        else:
            print(f"Session Creation Failed: {resp.status_code} - {resp.text}")
            return None
    except Exception as e:
        print(f"Session Creation Error: {e}")
        return None

def test_endpoint(base_url, path, token, session_id=None):
    url = f"{base_url.rstrip('/')}{path}"
    print(f"\nTesting: {url}")
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }
    if session_id:
        headers["backupdr-management-session"] = f"Actifio {session_id}"
    
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
        sys.exit(1)

    # 1. Create Session
    session_id = create_session(base_url, token)
    if not session_id:
        print("WARNING: Proceeding without session_id (expect failures)...")

    paths = [
        # Likely Correct Path (based on docs)
        "/actifio/jobstatus",
        
        # Variations
        "/actifio/api/jobstatus",
        "/actifio/api/v1/jobstatus",
        "/actifio/jobs",
        "/actifio/api/jobs",
        "/actifio/api/v1/jobs",
        
        # AGM / GCBDR specific
        "/agm/api/jobs",
        "/agm/jobstatus",
        
        # Info/Login
        "/actifio/info",
        "/actifio/api/info",
        "/actifio/login",
    ]

    print(f"Probing {base_url}...")
    for path in paths:
        if test_endpoint(base_url, path, token, session_id):
            print(f"--> SUCCESS found at {path}")
