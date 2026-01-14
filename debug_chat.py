import os
import json
import logging
import urllib.request
import google.auth
import google.auth.transport.requests

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_chat_auth():
    space_id = os.environ.get('GOOGLE_CHAT_SPACE_ID')
    if not space_id:
        print("Error: Please set GOOGLE_CHAT_SPACE_ID environment variable.")
        return

    # Detect Mode
    if space_id.startswith('http'):
        print(f"Detected Webhook URL mode.")
        test_webhook(space_id)
        return

    # Normalize Space ID
    if not space_id.startswith('spaces/'):
        space_name = f"spaces/{space_id}"
    else:
        space_name = space_id

    print(f"Testing auth for Space: {space_name} (API Mode)")

    try:
        # 1. Get Credentials
        print("1. Acquiring Application Default Credentials (ADC)...")
        # Try Bot scope first, but if acting as user, we might need 'chat.messages'. 
        
        scopes = ['https://www.googleapis.com/auth/chat.bot']
        
        # Check if we should try User scope (simple toggle for now)
        if os.environ.get('USE_USER_SCOPE'):
             print("   [INFO] USE_USER_SCOPE set. Switching to 'chat.messages' scope.")
             scopes = ['https://www.googleapis.com/auth/chat.messages']
        
        creds, project = google.auth.default(scopes=scopes)
        print(f"   Success! Project: {project}")
        print(f"   Service Account: {creds.service_account_email if hasattr(creds, 'service_account_email') else 'Unknown (User account?)'}")
        print(f"   Scopes: {scopes}")

        # 2. Refresh Token
        print("2. Refreshing access token...")
        auth_req = google.auth.transport.requests.Request()
        creds.refresh(auth_req)
        print("   Success! Token acquired.")

        # 3. Send Message
        print("3. Sending test message to Chat API...")
        url = f"https://chat.googleapis.com/v1/{space_name}/messages"
        
        # Simple text payload
        payload = {"text": "Hello! This is a test message from the GCBDR Monitor debugger (API Mode)."}
        
        req = urllib.request.Request(
            url,
            data=json.dumps(payload).encode('utf-8'),
            headers={
                'Content-Type': 'application/json',
                'Authorization': f'Bearer {creds.token}'
            }
        )

        with urllib.request.urlopen(req) as response:
            print(f"   Success! HTTP Status: {response.status}")
            print(f"   Response: {response.read().decode('utf-8')}")

    except Exception as e:
        print(f"\n❌ FAILED: {e}")

def test_webhook(url):
    print("Testing Webhook...")
    try:
        payload = {"text": "Hello! This is a test message from the GCBDR Monitor debugger (Webhook Mode)."}
        req = urllib.request.Request(
            url,
            data=json.dumps(payload).encode('utf-8'),
            headers={'Content-Type': 'application/json'}
        )
        
        with urllib.request.urlopen(req) as response:
            print(f"   Success! HTTP Status: {response.status}")
            print(f"   Response: {response.read().decode('utf-8')}")
    except Exception as e:
        print(f"\n❌ FAILED: {e}")
        if "403" in str(e):
            print("\nPossible Causes for 403 Forbidden:")
            print("1. The Service Account is NOT explicitly added to the Chat Space.")
            print("   -> Go to the Space > Apps & integrations > Add apps > Search for your Service Account.")
            print("2. The Chat App is not enabled or configured in Google Cloud Console.")
            print("   -> Enable 'Google Chat API' and configure the 'App' status in the API settings.")

if __name__ == "__main__":
    test_chat_auth()
