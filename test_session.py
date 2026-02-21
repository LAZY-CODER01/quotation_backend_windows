#!/usr/bin/env python3
"""
Test script to check if Flask sessions are working properly.
"""

import requests
import json

BASE_URL = "http://localhost:8000"

def test_session():
    """Test session creation and persistence."""
    session = requests.Session()
    
    print("=" * 60)
    print("Testing Flask Session")
    print("=" * 60)
    
    # Test 1: Check auth status (should create session)
    print("\n1. Testing /api/auth/status (should create session)...")
    response = session.get(f"{BASE_URL}/api/auth/status")
    print(f"   Status Code: {response.status_code}")
    print(f"   Response: {json.dumps(response.json(), indent=2)}")
    print(f"   Cookies received: {list(response.cookies.keys())}")
    if response.cookies:
        for cookie in response.cookies:
            print(f"   Cookie: {cookie.name} = {cookie.value[:50]}...")
            print(f"   Domain: {cookie.domain}, Path: {cookie.path}, Secure: {cookie.secure}, SameSite: {cookie._rest.get('SameSite', 'Not set')}")
    
    # Test 2: Check debug endpoint
    print("\n2. Testing /api/auth/debug...")
    response = session.get(f"{BASE_URL}/api/auth/debug")
    print(f"   Status Code: {response.status_code}")
    print(f"   Response: {json.dumps(response.json(), indent=2)}")
    
    # Test 3: Check auth status again (should have session now)
    print("\n3. Testing /api/auth/status again (with session cookie)...")
    response = session.get(f"{BASE_URL}/api/auth/status")
    print(f"   Status Code: {response.status_code}")
    print(f"   Response: {json.dumps(response.json(), indent=2)}")
    
    # Test 4: Test login endpoint
    print("\n4. Testing /api/auth/login (should use existing session)...")
    response = session.get(f"{BASE_URL}/api/auth/login")
    print(f"   Status Code: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        print(f"   Success: {data.get('success')}")
        if data.get('authorization_url'):
            print(f"   Auth URL: {data.get('authorization_url')[:100]}...")
    
    print("\n" + "=" * 60)
    print("Test Complete")
    print("=" * 60)
    
    # Summary
    print("\nSummary:")
    print("- If cookies are being set, you should see 'session' cookie above")
    print("- If session persists, the second /api/auth/status should show the same user_id")
    print("- If frontend has issues, check that it uses 'credentials: include' in fetch requests")

if __name__ == '__main__':
    try:
        test_session()
    except requests.exceptions.ConnectionError:
        print("  Error: Could not connect to backend server.")
        print("   Make sure the backend is running on http://localhost:8000")
    except Exception as e:
        print(f"  Error: {e}")
        import traceback
        traceback.print_exc()

