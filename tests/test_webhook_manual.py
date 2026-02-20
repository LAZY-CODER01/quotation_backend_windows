import sys
import os
import json
import base64
from unittest.mock import MagicMock
import threading

# Add the project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Mock dependencies that might be missing or require setup
sys.modules['flask_socketio'] = MagicMock()
sys.modules['google.auth.transport.requests'] = MagicMock()
sys.modules['google.oauth2.credentials'] = MagicMock()
sys.modules['google_auth_oauthlib.flow'] = MagicMock()
sys.modules['googleapiclient.discovery'] = MagicMock()
sys.modules['googleapiclient.errors'] = MagicMock()
sys.modules['pymongo'] = MagicMock()
sys.modules['app.services.semantic_search_service'] = MagicMock()
sys.modules['app.services.duckdb_service'] = MagicMock()

# Mock Config before importing backend_app or it might fail on missing env vars
from config.settings import Config
# We can just leave Config as is, or patch it if needed.

# Import app creator
from backend_app import create_flask_app
import backend_app

# Prevent auto-start from messing up our mock injection
backend_app.start_company_gmail_monitoring = MagicMock()

def test_webhook():
    print("Testing Webhook Endpoint...")
    
    # Create Flask app
    app = create_flask_app()
    client = app.test_client()
    
    # Mock the company_gmail_service
    mock_service = MagicMock()
    mock_service.check_for_new_emails = MagicMock()
    
    # Inject into backend_app global
    backend_app.company_gmail_service = mock_service
    
    # Constuct Pub/Sub Payload
    data = {
        "emailAddress": "user@example.com",
        "historyId": 123456
    }
    data_json = json.dumps(data)
    data_b64 = base64.b64encode(data_json.encode('utf-8')).decode('utf-8')
    
    payload = {
        "message": {
            "data": data_b64,
            "messageId": "msg_123"
        },
        "subscription": "projects/myproject/subscriptions/mysub"
    }
    
    # Send POST request
    response = client.post('/api/gmail/webhook', json=payload)
    
    print(f"Response Status: {response.status_code}")
    print(f"Response Body: {response.data.decode('utf-8')}")
    
    if response.status_code == 200:
        print("✅ Endpoint returned 200 OK")
    else:
        print("❌ Endpoint returned error")
        
    # Verify processing triggered
    # Logic in endpoint spawns a thread. We need to wait a bit or just check if it was called?
    # Since we mocked the method, we can check calls.
    # Threading might make it tricky to check immediately if it hasn't started.
    # But checking if thread was started is hard.
    # We can check if `check_for_new_emails` was called.
    # Actually, verify that it was passed to threading.Thread target.
    # But since we are running this script, the thread should start.
    
    import time
    time.sleep(1) # Wait for thread to run
    
    if mock_service.check_for_new_emails.called:
        print("✅ check_for_new_emails was called")
    else:
        print("❌ check_for_new_emails was NOT called")
        
if __name__ == "__main__":
    test_webhook()
