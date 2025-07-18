#setup_calendar_webhooks.py
#!/usr/bin/env python3
"""
Simple setup script for Microsoft 365 Calendar Webhooks
This creates the initial webhook subscriptions with Microsoft Graph API
"""

import os
import requests
import uuid
from datetime import datetime, timedelta
from dotenv import load_dotenv
from calendar_integration import get_valid_access_token

# Load environment variables
load_dotenv()

# Configuration
BASE_URL = os.getenv('BASE_URL')  # https://dashboard.allkins.com
PRIMARY_USER_EMAIL = os.getenv('PRIMARY_USER_EMAIL')
SECONDARY_USER_EMAIL = os.getenv('SECONDARY_USER_EMAIL')

# Microsoft Graph API
SUBSCRIPTION_ENDPOINT = "https://graph.microsoft.com/v1.0/subscriptions"

def create_subscription(user_email):
    """Create a webhook subscription for a user"""
    print(f"Setting up webhook for {user_email}...")
    
    access_token = get_valid_access_token(user_email)
    if not access_token:
        print(f"❌ No valid access token for {user_email}")
        return False
    
    # Subscription data
    subscription_data = {
        "changeType": "created,updated,deleted",
        "notificationUrl": f"{BASE_URL}/webhook/calendar",
        "resource": "/me/events",
        "expirationDateTime": (datetime.utcnow() + timedelta(days=3)).strftime('%Y-%m-%dT%H:%M:%S.000Z'),
        "clientState": f"calendar_webhook_{user_email}_{str(uuid.uuid4())}"
    }
    
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    
    try:
        response = requests.post(SUBSCRIPTION_ENDPOINT, headers=headers, json=subscription_data)
        response.raise_for_status()
        
        subscription = response.json()
        print(f"✅ Successfully created webhook for {user_email}")
        print(f"   Subscription ID: {subscription['id']}")
        print(f"   Expires: {subscription['expirationDateTime']}")
        return True
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to create webhook for {user_email}: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"   Response: {e.response.text}")
        return False

def main():
    """Set up webhooks for all users"""
    print("🔧 Setting up Microsoft 365 Calendar Webhooks")
    print(f"📍 Webhook URL: {BASE_URL}/webhook/calendar")
    print()
    
    users = [PRIMARY_USER_EMAIL, SECONDARY_USER_EMAIL]
    success_count = 0
    
    for user_email in users:
        if user_email:
            if create_subscription(user_email):
                success_count += 1
            print()
    
    print(f"🎉 Setup complete: {success_count}/{len([u for u in users if u])} webhooks created")
    print("Your calendar webhooks are now active and will send real-time notifications!")

if __name__ == "__main__":
    main()