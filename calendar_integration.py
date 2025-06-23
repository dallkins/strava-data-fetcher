#!/usr/bin/env python3
"""
Microsoft 365 Calendar Integration for Dashboard
Fetches calendar events for Dominic and Clare
Supports both scheduled fetching and webhook notifications
"""

import os
import requests
import mysql.connector
from datetime import datetime, timedelta, timezone
import logging
from dotenv import load_dotenv
import json
import base64
from urllib.parse import urlencode

# Load environment variables
load_dotenv()

# Set up logging
log_file = os.getenv('LOG_FILE', '/var/log/calendar_sync.log')
log_level = getattr(logging, os.getenv('LOG_LEVEL', 'INFO'))

logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

# Microsoft Graph API configuration
MICROSOFT_CLIENT_ID = os.getenv('MICROSOFT_CLIENT_ID')
MICROSOFT_CLIENT_SECRET = os.getenv('MICROSOFT_CLIENT_SECRET')
MICROSOFT_TENANT_ID = os.getenv('MICROSOFT_TENANT_ID', 'common')
MICROSOFT_REDIRECT_URI = os.getenv('MICROSOFT_REDIRECT_URI')

# User configuration
PRIMARY_USER_EMAIL = os.getenv('PRIMARY_USER_EMAIL')  # dominic@allkins.com
SECONDARY_USER_EMAIL = os.getenv('SECONDARY_USER_EMAIL')  # clare@allkins.com

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME'),
    'charset': 'utf8mb4'
}

# Microsoft Graph API endpoints
GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"
TOKEN_URL = "https://login.microsoftonline.com/{}/oauth2/v2.0/token".format(MICROSOFT_TENANT_ID)
AUTHORIZATION_URL = "https://login.microsoftonline.com/{}/oauth2/v2.0/authorize".format(MICROSOFT_TENANT_ID)

def get_db_connection():
    """Get database connection"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except mysql.connector.Error as e:
        logging.error(f"Database connection failed: {e}")
        return None

def create_calendar_tables():
    """Create database tables for calendar data"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # Table for storing user access tokens
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS calendar_tokens (
                id INT PRIMARY KEY AUTO_INCREMENT,
                user_email VARCHAR(255) UNIQUE,
                access_token TEXT,
                refresh_token TEXT,
                token_expires DATETIME,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
        """)
        
        # Table for storing calendar events
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS calendar_events (
                id INT PRIMARY KEY AUTO_INCREMENT,
                user_email VARCHAR(255),
                event_id VARCHAR(255),
                subject VARCHAR(500),
                start_time DATETIME,
                end_time DATETIME,
                location VARCHAR(500),
                is_all_day BOOLEAN DEFAULT FALSE,
                organizer_email VARCHAR(255),
                organizer_name VARCHAR(255),
                attendees JSON,
                body_preview TEXT,
                importance VARCHAR(50) DEFAULT 'normal',
                show_as VARCHAR(50) DEFAULT 'busy',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY unique_event (user_email, event_id),
                INDEX idx_user_time (user_email, start_time),
                INDEX idx_start_time (start_time)
            )
        """)
        
        # Table for webhook subscriptions
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS calendar_webhooks (
                id INT PRIMARY KEY AUTO_INCREMENT,
                user_email VARCHAR(255),
                subscription_id VARCHAR(255),
                resource VARCHAR(255),
                notification_url VARCHAR(500),
                expires_datetime DATETIME,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY unique_subscription (user_email, resource)
            )
        """)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logging.info("Calendar database tables created successfully")
        return True
        
    except mysql.connector.Error as e:
        logging.error(f"Error creating calendar tables: {e}")
        return False

def get_authorization_url():
    """Generate Microsoft 365 authorization URL"""
    params = {
        'client_id': MICROSOFT_CLIENT_ID,
        'response_type': 'code',
        'redirect_uri': MICROSOFT_REDIRECT_URI,
        'scope': 'https://graph.microsoft.com/Calendars.Read https://graph.microsoft.com/User.Read offline_access',
        'response_mode': 'query'
    }
    
    auth_url = f"{AUTHORIZATION_URL}?{urlencode(params)}"
    return auth_url

def exchange_code_for_token(authorization_code):
    """Exchange authorization code for access token"""
    data = {
        'client_id': MICROSOFT_CLIENT_ID,
        'client_secret': MICROSOFT_CLIENT_SECRET,
        'code': authorization_code,
        'grant_type': 'authorization_code',
        'redirect_uri': MICROSOFT_REDIRECT_URI,
        'scope': 'https://graph.microsoft.com/Calendars.Read https://graph.microsoft.com/User.Read offline_access'
    }
    
    try:
        response = requests.post(TOKEN_URL, data=data)
        
        # Log the response for debugging
        logging.info(f"Token exchange response status: {response.status_code}")
        logging.info(f"Token exchange response: {response.text}")
        
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Token exchange failed: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logging.error(f"Response content: {e.response.text}")
        return None

def refresh_access_token(refresh_token):
    """Refresh access token using refresh token"""
    data = {
        'client_id': MICROSOFT_CLIENT_ID,
        'client_secret': MICROSOFT_CLIENT_SECRET,
        'refresh_token': refresh_token,
        'grant_type': 'refresh_token',
        'scope': 'https://graph.microsoft.com/Calendars.Read https://graph.microsoft.com/User.Read offline_access'
    }
    
    try:
        response = requests.post(TOKEN_URL, data=data)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Token refresh failed: {e}")
        return None

def save_user_tokens(user_email, token_data):
    """Save or update user tokens in database"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # Calculate token expiration time
        expires_in = token_data.get('expires_in', 3600)
        token_expires = datetime.now() + timedelta(seconds=expires_in)
        
        # Insert or update tokens
        query = """
            INSERT INTO calendar_tokens (user_email, access_token, refresh_token, token_expires)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            access_token = VALUES(access_token),
            refresh_token = VALUES(refresh_token),
            token_expires = VALUES(token_expires),
            updated_at = CURRENT_TIMESTAMP
        """
        
        values = (
            user_email,
            token_data.get('access_token'),
            token_data.get('refresh_token'),
            token_expires
        )
        
        cursor.execute(query, values)
        conn.commit()
        cursor.close()
        conn.close()
        
        logging.info(f"Saved tokens for user: {user_email}")
        return True
        
    except mysql.connector.Error as e:
        logging.error(f"Error saving tokens: {e}")
        return False

def get_valid_access_token(user_email):
    """Get a valid access token for user, refreshing if necessary"""
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT access_token, refresh_token, token_expires 
            FROM calendar_tokens 
            WHERE user_email = %s
        """, (user_email,))
        
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if not result:
            logging.warning(f"No tokens found for user: {user_email}")
            return None
        
        access_token, refresh_token, token_expires = result
        
        # Check if token is still valid (with 5-minute buffer)
        if datetime.now() < (token_expires - timedelta(minutes=5)):
            return access_token
        
        # Token expired, try to refresh
        logging.info(f"Refreshing token for user: {user_email}")
        token_data = refresh_access_token(refresh_token)
        
        if token_data and save_user_tokens(user_email, token_data):
            return token_data.get('access_token')
        else:
            logging.error(f"Failed to refresh token for user: {user_email}")
            return None
            
    except mysql.connector.Error as e:
        logging.error(f"Error getting access token: {e}")
        return None

def fetch_calendar_events(user_email, days_ahead=5):
    """Fetch calendar events for a user"""
    access_token = get_valid_access_token(user_email)
    if not access_token:
        logging.error(f"No valid access token for {user_email}")
        return None
    
    # Calculate date range (today + next X days)
    start_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    end_time = start_time + timedelta(days=days_ahead + 1)
    
    # Format for Microsoft Graph API
    start_str = start_time.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    end_str = end_time.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    
    # Microsoft Graph API call
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    
    params = {
        'startDateTime': start_str,
        'endDateTime': end_str,
        '$select': 'id,subject,start,end,location,isAllDay,organizer,attendees,bodyPreview,importance,showAs',
        '$orderby': 'start/dateTime'
    }
    
    url = f"{GRAPH_BASE_URL}/me/calendarView"
    
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        
        events_data = response.json()
        return events_data.get('value', [])
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching calendar events for {user_email}: {e}")
        return None

def parse_event_datetime(dt_info):
    """Parse Microsoft Graph datetime info"""
    if not dt_info:
        return None
    
    dt_str = dt_info['dateTime']
    timezone_str = dt_info.get('timeZone', 'UTC')
    
    # Parse the datetime
    try:
        # Remove timezone info from string and parse
        if dt_str.endswith('Z'):
            dt = datetime.fromisoformat(dt_str[:-1]).replace(tzinfo=timezone.utc)
        else:
            dt = datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
        
        # Convert to local time (you might want to adjust this based on your needs)
        return dt.replace(tzinfo=None)
        
    except ValueError as e:
        logging.warning(f"Could not parse datetime: {dt_str}, error: {e}")
        return None

def save_calendar_events(user_email, events):
    """Save calendar events to database"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # Clear existing events for this user in the date range
        cursor.execute("""
            DELETE FROM calendar_events 
            WHERE user_email = %s 
            AND start_time >= CURDATE() 
            AND start_time <= DATE_ADD(CURDATE(), INTERVAL 6 DAY)
        """, (user_email,))
        
        # Insert new events
        for event in events:
            start_time = parse_event_datetime(event.get('start'))
            end_time = parse_event_datetime(event.get('end'))
            
            if not start_time or not end_time:
                continue
            
            organizer = event.get('organizer', {})
            attendees = event.get('attendees', [])
            
            insert_query = """
                INSERT INTO calendar_events 
                (user_email, event_id, subject, start_time, end_time, location, is_all_day,
                 organizer_email, organizer_name, attendees, body_preview, importance, show_as)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                subject = VALUES(subject),
                start_time = VALUES(start_time),
                end_time = VALUES(end_time),
                location = VALUES(location),
                is_all_day = VALUES(is_all_day),
                organizer_email = VALUES(organizer_email),
                organizer_name = VALUES(organizer_name),
                attendees = VALUES(attendees),
                body_preview = VALUES(body_preview),
                importance = VALUES(importance),
                show_as = VALUES(show_as),
                updated_at = CURRENT_TIMESTAMP
            """
            
            values = (
                user_email,
                event.get('id'),
                event.get('subject', 'No Subject'),
                start_time,
                end_time,
                event.get('location', {}).get('displayName', ''),
                event.get('isAllDay', False),
                organizer.get('emailAddress', {}).get('address', ''),
                organizer.get('emailAddress', {}).get('name', ''),
                json.dumps(attendees) if attendees else None,
                event.get('bodyPreview', '')[:500],  # Limit preview length
                event.get('importance', 'normal'),
                event.get('showAs', 'busy')
            )
            
            cursor.execute(insert_query, values)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logging.info(f"Saved {len(events)} events for {user_email}")
        return True
        
    except mysql.connector.Error as e:
        logging.error(f"Error saving calendar events: {e}")
        return False

def sync_all_calendars():
    """Sync calendars for all configured users"""
    users = [PRIMARY_USER_EMAIL, SECONDARY_USER_EMAIL]
    success_count = 0
    
    for user_email in users:
        if not user_email:
            continue
            
        logging.info(f"Syncing calendar for {user_email}")
        
        events = fetch_calendar_events(user_email)
        if events is not None:
            if save_calendar_events(user_email, events):
                success_count += 1
                logging.info(f"Successfully synced {len(events)} events for {user_email}")
            else:
                logging.error(f"Failed to save events for {user_email}")
        else:
            logging.error(f"Failed to fetch events for {user_email}")
    
    logging.info(f"Calendar sync completed. {success_count}/{len([u for u in users if u])} users synced successfully")
    return success_count

def get_today_events(user_email=None):
    """Get today's events for a user or all users"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        
        if user_email:
            query = """
                SELECT user_email, subject, start_time, end_time, location, is_all_day, organizer_name
                FROM calendar_events 
                WHERE user_email = %s 
                AND DATE(start_time) = CURDATE()
                ORDER BY start_time
            """
            cursor.execute(query, (user_email,))
        else:
            query = """
                SELECT user_email, subject, start_time, end_time, location, is_all_day, organizer_name
                FROM calendar_events 
                WHERE DATE(start_time) = CURDATE()
                ORDER BY start_time
            """
            cursor.execute(query)
        
        events = []
        for row in cursor.fetchall():
            events.append({
                'user_email': row[0],
                'subject': row[1],
                'start_time': row[2],
                'end_time': row[3],
                'location': row[4],
                'is_all_day': bool(row[5]),
                'organizer_name': row[6]
            })
        
        cursor.close()
        conn.close()
        return events
        
    except mysql.connector.Error as e:
        logging.error(f"Error getting today's events: {e}")
        return []

def get_upcoming_events(days=5, user_email=None):
    """Get upcoming events for next X days"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        
        if user_email:
            query = """
                SELECT user_email, subject, start_time, end_time, location, is_all_day, organizer_name
                FROM calendar_events 
                WHERE user_email = %s 
                AND start_time >= CURDATE()
                AND start_time <= DATE_ADD(CURDATE(), INTERVAL %s DAY)
                ORDER BY start_time
            """
            cursor.execute(query, (user_email, days))
        else:
            query = """
                SELECT user_email, subject, start_time, end_time, location, is_all_day, organizer_name
                FROM calendar_events 
                WHERE start_time >= CURDATE()
                AND start_time <= DATE_ADD(CURDATE(), INTERVAL %s DAY)
                ORDER BY start_time
            """
            cursor.execute(query, (days,))
        
        events = []
        for row in cursor.fetchall():
            events.append({
                'user_email': row[0],
                'subject': row[1],
                'start_time': row[2],
                'end_time': row[3],
                'location': row[4],
                'is_all_day': bool(row[5]),
                'organizer_name': row[6]
            })
        
        cursor.close()
        conn.close()
        return events
        
    except mysql.connector.Error as e:
        logging.error(f"Error getting upcoming events: {e}")
        return []

# Command line interface
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python3 calendar_integration.py setup          # Create database tables")
        print("  python3 calendar_integration.py auth           # Get authorization URL")
        print("  python3 calendar_integration.py token <code>   # Exchange code for token")
        print("  python3 calendar_integration.py sync           # Sync all calendars")
        print("  python3 calendar_integration.py today          # Show today's events")
        print("  python3 calendar_integration.py upcoming       # Show upcoming events")
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == "setup":
        if create_calendar_tables():
            print("✅ Calendar database tables created successfully")
        else:
            print("❌ Failed to create calendar database tables")
    
    elif command == "auth":
        auth_url = get_authorization_url()
        print("🔗 Authorization URL:")
        print(auth_url)
        print("\nOpen this URL in your browser and authorize the application.")
        print("After authorization, copy the 'code' parameter from the redirect URL.")
    
    elif command == "token":
        if len(sys.argv) < 4:
            print("Usage: python3 calendar_integration.py token <user_email> <authorization_code>")
            print("Example: python3 calendar_integration.py token dominic@allkins.com ABC123...")
            sys.exit(1)
        
        user_email = sys.argv[2]
        code = sys.argv[3]
        
        print(f"🔄 Exchanging code for tokens for {user_email}...")
        token_data = exchange_code_for_token(code)
        
        if token_data:
            if save_user_tokens(user_email, token_data):
                print(f"✅ Successfully saved tokens for {user_email}")
                
                # Test the token by fetching user info
                access_token = token_data.get('access_token')
                headers = {'Authorization': f'Bearer {access_token}'}
                
                try:
                    response = requests.get(f"{GRAPH_BASE_URL}/me", headers=headers)
                    if response.status_code == 200:
                        user_info = response.json()
                        print(f"📧 Verified access for: {user_info.get('displayName')} ({user_info.get('mail')})")
                    else:
                        print("⚠️  Token saved but could not verify user info")
                except:
                    print("⚠️  Token saved but could not verify user info")
            else:
                print(f"❌ Failed to save tokens for {user_email}")
        else:
            print("❌ Token exchange failed")
    
    elif command == "sync":
        success_count = sync_all_calendars()
        print(f"✅ Synced calendars for {success_count} users")
    
    elif command == "today":
        events = get_today_events()
        if events:
            print("📅 Today's Events:")
            for event in events:
                time_str = event['start_time'].strftime('%H:%M') if not event['is_all_day'] else 'All Day'
                print(f"  {time_str} - {event['subject']} ({event['user_email']})")
        else:
            print("No events today")
    
    elif command == "upcoming":
        events = get_upcoming_events()
        if events:
            print("📅 Upcoming Events:")
            for event in events:
                date_str = event['start_time'].strftime('%Y-%m-%d')
                time_str = event['start_time'].strftime('%H:%M') if not event['is_all_day'] else 'All Day'
                print(f"  {date_str} {time_str} - {event['subject']} ({event['user_email']})")
        else:
            print("No upcoming events")
    
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)