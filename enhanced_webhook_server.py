#enhanced_webhook_server.py
#!/usr/bin/env python3
"""
Enhanced Strava Webhook Server with Activity Update Tracking and Email Notifications
Handles create, update events and schedules delayed refreshes
Sends immediate, weekly, monthly, and annual email notifications

Author: AI Assistant (Claude)
Version: 2.0 - Production Ready
"""

import os
import logging
import asyncio
import json
import time
import threading
import calendar
from datetime import datetime, timedelta
from dataclasses import asdict
import psutil
import gc

# Third-party imports
import mysql.connector
from mysql.connector import Error
import aiohttp
import requests
from flask import Flask, request, jsonify
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

#for calendar webhook
from calendar_integration import get_valid_access_token, fetch_calendar_events, save_calendar_events
import uuid

# Local imports
from strava_main import StravaDataFetcher, Config, parse_strava_activity

# Load environment variables
load_dotenv()

# Initialize Flask app
app = Flask(__name__)

# ===== CONFIGURATION SECTION =====
class WebhookConfig:
    """Centralized configuration management"""
    
    def __init__(self):
        # Core webhook settings
        self.webhook_verify_token = os.getenv('WEBHOOK_VERIFY_TOKEN', 'your_verify_token_here')
        self.webhook_port = int(os.getenv('WEBHOOK_PORT', 5000))
        
        # Database configuration
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'database': os.getenv('DB_NAME'),
            'charset': 'utf8mb4',
            'autocommit': True,
            'use_pure': True,  # Add this line
            'raise_on_warnings': False  # Add this line
        }
        
        # Athlete configuration
        self.athlete_mappings = {
            int(os.getenv('ATHLETE_ID_DOMINIC', 0)): 'Dominic',
            int(os.getenv('ATHLETE_ID_CLARE', 0)): 'Clare'
        }
        
        # Email configuration
        self.brevo_api_key = os.getenv("BREVO_API_KEY")
        self.brevo_from_email = os.getenv("BREVO_FROM_EMAIL", "dominic.allkins@gmail.com")
        self.brevo_from_name = os.getenv("BREVO_FROM_NAME", "Strava Webhook Server")
        
        # Notification email addresses
        self.notification_emails = {
            'dominic': os.getenv('DOMINIC_EMAIL'),
            'clare': os.getenv('CLARE_EMAIL', 'clare@allkins.com')
        }
        
        # Strava API configuration
        self.strava_client_id = os.getenv('STRAVA_CLIENT_ID')
        self.strava_client_secret = os.getenv('STRAVA_CLIENT_SECRET')
        
        # Token configuration for athletes
        self.athlete_tokens = {
            'dominic': {
                'access_token': os.getenv('DOMINIC_ACCESS_TOKEN'),
                'refresh_token': os.getenv('DOMINIC_REFRESH_TOKEN'),
                'expires_at': int(os.getenv('DOMINIC_TOKEN_EXPIRES', '0'))
            },
            'clare': {
                'access_token': os.getenv('CLARE_ACCESS_TOKEN'),
                'refresh_token': os.getenv('CLARE_REFRESH_TOKEN'),
                'expires_at': int(os.getenv('CLARE_TOKEN_EXPIRES', '0'))
            }
        }
    
    def validate_config(self):
        """Validate essential configuration"""
        errors = []
        
        if not self.webhook_verify_token or self.webhook_verify_token == 'your_verify_token_here':
            errors.append("WEBHOOK_VERIFY_TOKEN not set")
        
        if not self.db_config['user'] or not self.db_config['password']:
            errors.append("Database credentials not configured")
        
        if not self.brevo_api_key:
            errors.append("BREVO_API_KEY not set")
        
        if not self.strava_client_id or not self.strava_client_secret:
            errors.append("Strava client credentials not set")
        
        if errors:
            raise ValueError(f"Configuration errors: {', '.join(errors)}")
        
        return True

# Initialize configuration
webhook_config = WebhookConfig()

# Setup logging with better formatting
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('webhook.log'),
        logging.StreamHandler()
    ]
)

# Create logger
logger = logging.getLogger(__name__)

# Initialize Strava components
config = Config.from_env()
fetcher = StravaDataFetcher(config)

# ===== FIX 1: ImprovedCacheManager (NEW CLASS) =====
class ImprovedCacheManager:
    """Memory-efficient cache with automatic cleanup"""
    
    def __init__(self, max_size=1000, cleanup_interval=300):  # 5 minutes
        self.cache = {}
        self.max_size = max_size
        self.cleanup_interval = cleanup_interval
        self.last_cleanup = time.time()
    
    def get(self, key):
        """Get item from cache"""
        self._cleanup_if_needed()
        item = self.cache.get(key)
        if item and item['expires'] > time.time():
            return item['value']
        elif item:
            # Item expired, remove it
            del self.cache[key]
        return None
    
    def set(self, key, value, ttl=1800):  # 30 minutes default TTL
        """Set item in cache with TTL"""
        self._cleanup_if_needed()
        
        # Prevent cache from growing too large
        if len(self.cache) >= self.max_size:
            self._force_cleanup()
        
        self.cache[key] = {
            'value': value,
            'expires': time.time() + ttl
        }
    
    def _cleanup_if_needed(self):
        """Cleanup expired items if enough time has passed"""
        current_time = time.time()
        if current_time - self.last_cleanup > self.cleanup_interval:
            self._force_cleanup()
    
    def _force_cleanup(self):
        """Force cleanup of expired items"""
        current_time = time.time()
        expired_keys = [
            key for key, data in self.cache.items()
            if data['expires'] < current_time
        ]
        
        for key in expired_keys:
            del self.cache[key]
        
        self.last_cleanup = current_time
        
        if expired_keys:
            logger.debug(f"Cache cleanup: removed {len(expired_keys)} expired items")
    
    def clear(self):
        """Clear all cache entries"""
        self.cache.clear()
        self.last_cleanup = time.time()
    
    def size(self):
        """Get current cache size"""
        return len(self.cache)

# ===== EMAIL NOTIFICATION SYSTEM =====
class EmailNotificationService:
    """Enhanced email notifications for webhook events with deduplication"""
    
    def __init__(self, config: WebhookConfig):
        self.config = config
        self.api_url = "https://api.brevo.com/v3/smtp/email"
        
        # Use improved cache manager instead of simple dict
        self.email_cache = ImprovedCacheManager(max_size=500, cleanup_interval=300)
        
        # Setup athlete email mapping
        self.athlete_emails = {
            'dominic': self.config.notification_emails['dominic'],
            'clare': self.config.notification_emails['clare']
        }
        
        # Admin emails for summaries
        self.admin_emails = [
            email for email in self.config.notification_emails.values() 
            if email is not None
        ]
        
        logger.info(f"Email service initialized with {len(self.admin_emails)} admin emails")
    
    def format_number(self, number):
        """Format numbers with commas for readability"""
        if number is None:
            return "0"
        return f"{number:,.0f}" if isinstance(number, (int, float)) else str(number)
    
    def get_calorie_equivalents(self, total_calories):
        """Generate fun food/drink equivalents for burned calories"""
        if not total_calories or total_calories <= 0:
            return ""
        
        # Food and drink equivalents with emojis
        equivalents = {
            'foods': [
                {"name": "Big Macs", "emoji": "🍔", "calories": 550},
                {"name": "Bacon Sandwiches", "emoji": "🥓", "calories": 500},
                {"name": "Victoria Sponge Slices", "emoji": "🍰", "calories": 325},
                {"name": "Croissants", "emoji": "🥐", "calories": 240},
                {"name": "Jam Doughnuts", "emoji": "🍩", "calories": 220},
            ],
            'drinks': [
                {"name": "Bottles of Bollinger", "emoji": "🍾", "calories": 485},
                {"name": "Bottles of Coke", "emoji": "🥤", "calories": 197},
                {"name": "Bottles of Peroni", "emoji": "🍺", "calories": 142},
                {"name": "Skinny Flat Whites", "emoji": "☕️", "calories": 87},
                {"name": "Glasses of OJ", "emoji": "🍊", "calories": 45},
            ]
        }
        
        html_sections = []
        
        for category, items in equivalents.items():
            category_name = "🍽️ Food" if category == 'foods' else "🥤 Drinks"
            item_html = []
            
            for item in items:
                count = int(total_calories / item["calories"])
                if count > 0:
                    item_html.append(f"""
                    <div style="display: flex; align-items: center; padding: 8px; 
                                background: white; border-radius: 6px; margin: 4px 0;">
                        <span style="font-size: 24px; margin-right: 10px;">{item["emoji"]}</span>
                        <span><strong>{self.format_number(count)}</strong> {item["name"]}</span>
                    </div>
                    """)
            
            if item_html:
                html_sections.append(f"""
                <h4 style="color: #f57c00; margin: 15px 0 10px 0;">{category_name}</h4>
                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); 
                           gap: 10px; margin-bottom: 20px;">
                    {''.join(item_html)}
                </div>
                """)
        
        if html_sections:
            return f"""
            <div style="background: #fff8e1; padding: 20px; border-radius: 8px; margin: 20px 0;">
                <h3 style="margin-top: 0; color: #f57c00;">🍔 Calorie Equivalents</h3>
                <p style="margin-bottom: 15px; color: #666;">You burned enough calories for:</p>
                {''.join(html_sections)}
            </div>
            """
        return ""
    
    def send_email(self, subject: str, body: str, recipient_emails=None):
        """Send email using Brevo API with error handling"""
        if not self.config.brevo_api_key:
            logger.error("No Brevo API key configured, skipping email")
            return False
        
        # Determine recipients
        if recipient_emails is None:
            recipient_emails = self.admin_emails
        elif isinstance(recipient_emails, str):
            recipient_emails = [recipient_emails]
        
        # Filter valid emails
        recipient_emails = [email for email in recipient_emails if email and '@' in email]
        
        if not recipient_emails:
            logger.error("No valid recipient emails found")
            return False
        
        try:
            headers = {
                "accept": "application/json",
                "content-type": "application/json",
                "api-key": self.config.brevo_api_key
            }
            
            payload = {
                "sender": {
                    "name": self.config.brevo_from_name, 
                    "email": self.config.brevo_from_email
                },
                "to": [{"email": email} for email in recipient_emails],
                "subject": subject,
                "htmlContent": body
            }
            
            logger.info(f"Sending email: '{subject}' to {len(recipient_emails)} recipients")
            
            response = requests.post(self.api_url, json=payload, headers=headers, timeout=30)
            
            if response.status_code == 201:
                logger.info(f"Email sent successfully: {subject}")
                return True
            else:
                logger.error(f"Email send failed: {response.status_code} - {response.text}")
                return False
                
        except requests.RequestException as e:
            logger.error(f"Email request error: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected email error: {e}")
            return False
    
    def should_send_email(self, activity_id, event_type):
        """Check if email should be sent with improved deduplication"""
        email_key = f"{activity_id}_{event_type}"
        
        # Check if we recently sent this email
        recent_send = self.email_cache.get(email_key)
        if recent_send:
            logger.info(f"Skipping duplicate email for {email_key}")
            return False
        
        # Record this email (30 minute TTL)
        self.email_cache.set(email_key, time.time(), ttl=1800)
        return True
    
    def clear_email_cache(self):
        """Clear email deduplication cache"""
        self.email_cache.clear()
        logger.info("Email deduplication cache cleared")

    def get_activity_stats(self, athlete_name=None, start_date=None, end_date=None):
        """Get cycling activity statistics with proper connection management"""
        try:
            db_manager = DatabaseManager(self.config)  # Create fresh instance
            
            # Build WHERE conditions
            where_conditions = ["sport_type IN ('Ride', 'VirtualRide')"]
            params = []
            
            if athlete_name:
                where_conditions.append("LOWER(athlete_name) = LOWER(%s)")
                params.append(athlete_name)
            
            if start_date:
                where_conditions.append("DATE(start_date_local) >= %s")
                params.append(start_date)
            
            if end_date:
                where_conditions.append("DATE(start_date_local) <= %s")
                params.append(end_date)
            
            where_clause = " AND ".join(where_conditions)
            
            query = f'''
                SELECT 
                    athlete_name,
                    COUNT(*) as ride_count,
                    COALESCE(SUM(distance), 0) as total_distance,
                    COALESCE(SUM(total_elevation_gain), 0) as total_elevation,
                    COALESCE(SUM(calories), 0) as total_calories,
                    COALESCE(SUM(moving_time), 0) as total_moving_time
                FROM strava_activities
                WHERE {where_clause}
                GROUP BY LOWER(athlete_name)
                ORDER BY athlete_name
            '''
            
            results = db_manager.execute_query(query, params, fetch_all=True)
            
            # Process results
            stats = {}
            for row in results:
                athlete, rides, distance, elevation, calories, moving_time = row
                stats[athlete.lower()] = {
                    'rides': rides,
                    'distance_km': round((distance or 0) / 1000, 1),
                    'elevation_m': round(elevation or 0),
                    'calories': round(calories or 0),
                    'moving_time_hours': round((moving_time or 0) / 3600, 1)
                }
            
            return stats
        
        except Error as e:
            logger.error(f"Error getting activity stats: {e}")
            return {}
    
    def get_activity_details(self, activity_id):
        """Get detailed information for a specific activity with proper connection management"""
        try:
            db_manager = DatabaseManager(self.config)
            
            result = db_manager.execute_query('''
                SELECT athlete_name, name, start_date_local, distance, 
                       total_elevation_gain, calories, type, sport_type
                FROM strava_activities 
                WHERE id = %s
            ''', (activity_id,), fetch_one=True)
            
            if result:
                return {
                    'athlete_name': result[0],
                    'name': result[1],
                    'date': result[2],
                    'distance_km': round((result[3] or 0) / 1000, 1),
                    'elevation_m': round(result[4] or 0),
                    'calories': round(result[5] or 0),
                    'type': result[6],
                    'sport_type': result[7]
                }
            return None
            
        except Error as e:
            logger.error(f"Error getting activity details: {e}")
            return None

# ===== DATABASE MANAGEMENT =====
class DatabaseManager:
    """Enhanced database manager with connection pooling and better cleanup"""
    
    def __init__(self, config: WebhookConfig):
        self.config = config
        self._max_connections = 5
    
    def get_connection(self):
        """Get database connection with automatic cleanup"""
        try:
            conn = mysql.connector.connect(**self.config.db_config)
            # Set connection timeout to prevent hanging connections
            conn.autocommit = True
            return conn
        except Error as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def execute_query(self, query, params=None, fetch_one=False, fetch_all=False):
        """Execute query with automatic connection management"""
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor(buffered=True)
            
            cursor.execute(query, params or ())
            
            if fetch_one:
                result = cursor.fetchone()
            elif fetch_all:
                result = cursor.fetchall()
            else:
                result = cursor.rowcount
            
            return result
            
        except Exception as e:
            logger.error(f"Database query failed: {e}")
            raise
        finally:
            # CRITICAL: Always close resources
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
            if conn:
                try:
                    conn.close()
                except:
                    pass
    
    def test_connection(self):
        """Test database connectivity with proper cleanup"""
        try:
            result = self.execute_query('SELECT 1', fetch_one=True)
            return result is not None
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False
    
    def activity_exists(self, activity_id, athlete_id):
        """Check if activity exists with proper connection management"""
        try:
            result = self.execute_query(
                "SELECT COUNT(*) FROM strava_activities WHERE id = %s AND athlete_id = %s",
                (activity_id, athlete_id),
                fetch_one=True
            )
            return (result[0] if result else 0) > 0
        except Exception as e:
            logger.error(f"Error checking if activity exists: {e}")
            return False
    
    def add_webhook_event(self, activity_id, athlete_id, event_type, aspect_type, raw_data):
        """Log webhook event for audit trail"""
        try:
            self.execute_query('''
                INSERT INTO webhook_events 
                (activity_id, athlete_id, event_type, aspect_type, raw_data, received_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
            ''', (activity_id, athlete_id, event_type, aspect_type, json.dumps(raw_data)))
            
            logger.debug(f"Logged webhook event: {event_type}/{aspect_type} for activity {activity_id}")
            
        except Error as e:
            logger.error(f"Error logging webhook event: {e}")

# ===== STRAVA API MANAGEMENT =====
class StravaAPIManager:
    """Handles Strava API interactions with token management"""
    
    def __init__(self, config: WebhookConfig):
        self.config = config
    
    async def refresh_access_token(self, athlete_name):
        """Refresh access token for an athlete"""
        athlete_key = athlete_name.lower()
        
        if athlete_key not in self.config.athlete_tokens:
            logger.error(f"No token configuration for athlete: {athlete_name}")
            return False
        
        athlete_config = self.config.athlete_tokens[athlete_key]
        refresh_token = athlete_config['refresh_token']
        
        if not refresh_token:
            logger.error(f"No refresh token for {athlete_name}")
            return False
        
        try:
            # Prepare token refresh request
            data = {
                'client_id': self.config.strava_client_id,
                'client_secret': self.config.strava_client_secret,
                'refresh_token': refresh_token,
                'grant_type': 'refresh_token'
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post('https://www.strava.com/oauth/token', data=data) as response:
                    if response.status == 200:
                        token_data = await response.json()
                        
                        # Update environment variables (in-memory)
                        env_prefix = athlete_name.upper()
                        os.environ[f'{env_prefix}_ACCESS_TOKEN'] = token_data['access_token']
                        os.environ[f'{env_prefix}_REFRESH_TOKEN'] = token_data['refresh_token']
                        os.environ[f'{env_prefix}_TOKEN_EXPIRES'] = str(token_data['expires_at'])
                        
                        # Update local configuration
                        athlete_config.update({
                            'access_token': token_data['access_token'],
                            'refresh_token': token_data['refresh_token'],
                            'expires_at': token_data['expires_at']
                        })
                        
                        logger.info(f"Successfully refreshed token for {athlete_name}")
                        return True
                    else:
                        logger.error(f"Token refresh failed for {athlete_name}: {response.status}")
                        return False
                        
        except Exception as e:
            logger.error(f"Error refreshing token for {athlete_name}: {e}")
            return False
    
    def get_athlete_tokens(self, athlete_name):
        """Get current tokens for an athlete"""
        athlete_key = athlete_name.lower()
        return self.config.athlete_tokens.get(athlete_key, {})
        # ===== ACTIVITY PROCESSING SYSTEM =====
class ActivityProcessor:
    """Handles activity fetching, processing, and database updates"""
    
    def __init__(self, config: WebhookConfig):
        self.config = config
        self.db_manager = DatabaseManager(config)
        self.api_manager = StravaAPIManager(config)
    
    async def fetch_and_save_activity(self, activity_id, athlete_id, athlete_name):
        """Fetch activity from Strava API and save to database"""
        try:
            logger.info(f"Fetching activity {activity_id} for {athlete_name} (ID: {athlete_id})")
            
            # Get and validate token
            tokens = self.api_manager.get_athlete_tokens(athlete_name)
            access_token = tokens.get('access_token')
            expires_at = tokens.get('expires_at', 0)
            
            if not access_token:
                logger.error(f"No access token for {athlete_name}")
                return False
            
            # Check token expiration (refresh 1 hour early)
            current_time = int(time.time())
            if current_time >= (expires_at - 3600):
                logger.info(f"Refreshing token for {athlete_name}")
                success = await self.api_manager.refresh_access_token(athlete_name)
                if not success:
                    return False
                
                # Get updated token
                tokens = self.api_manager.get_athlete_tokens(athlete_name)
                access_token = tokens.get('access_token')
            
            # Fetch activity data
            activity_data = await self._fetch_activity_from_api(activity_id, access_token)
            if not activity_data:
                return False
            
            # Parse and save activity
            return await self._save_activity_to_database(activity_data, athlete_name, athlete_id)
            
        except Exception as e:
            logger.error(f"Error processing activity {activity_id}: {e}")
            return False
    
    async def _fetch_activity_from_api(self, activity_id, access_token):
        """Fetch activity data from Strava API"""
        headers = {'Authorization': f'Bearer {access_token}'}
        url = f"https://www.strava.com/api/v3/activities/{activity_id}"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        logger.info(f"Successfully fetched activity: {data.get('name', 'Unknown')}")
                        return data
                    elif response.status == 429:
                        logger.warning("Rate limited by Strava API")
                        return None
                    else:
                        logger.error(f"Strava API error: {response.status}")
                        return None
                        
        except Exception as e:
            logger.error(f"API request failed: {e}")
            return None
    
    async def _save_activity_to_database(self, activity_data, athlete_name, athlete_id):
        """Parse activity data and save to database"""
        try:
            # Parse activity using existing parser
            activity = parse_strava_activity(activity_data, athlete_name, athlete_id)
            
            # Convert to dictionary for database insertion
            activity_dict = asdict(activity)
            
            # Format datetime fields for MySQL
            for date_field in ['start_date_local', 'start_date']:
                if activity_dict[date_field] and activity_dict[date_field].endswith('Z'):
                    activity_dict[date_field] = activity_dict[date_field][:-1].replace('T', ' ')
            
            # Handle JSON fields
            start_latlng_json = json.dumps(activity_dict['start_latlng']) if activity_dict['start_latlng'] else None
            end_latlng_json = json.dumps(activity_dict['end_latlng']) if activity_dict['end_latlng'] else None
            
            # Database insertion
            conn = self.db_manager.get_connection()
            cursor = conn.cursor()
            
            insert_sql = '''
                INSERT INTO strava_activities 
                (id, athlete_id, athlete_name, name, start_date_local, start_date, utc_offset,
                gear_id, gear_name, distance, elapsed_time, moving_time, calories,
                average_heartrate, max_heartrate, average_watts, max_watts,
                average_speed, max_speed, type, sport_type, total_elevation_gain,
                kudos_count, weighted_average_watts, average_cadence, trainer,
                map_polyline, device_name, timezone, start_latlng, end_latlng,
                data_loaded_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON DUPLICATE KEY UPDATE
                    name = VALUES(name),
                    gear_name = VALUES(gear_name),
                    calories = VALUES(calories),
                    kudos_count = VALUES(kudos_count),
                    distance = VALUES(distance),
                    moving_time = VALUES(moving_time),
                    total_elevation_gain = VALUES(total_elevation_gain),
                    updated_at = NOW()
            '''
            
            values = (
                activity_dict['id'], activity_dict['athlete_id'], activity_dict['athlete_name'], 
                activity_dict['name'], activity_dict['start_date_local'], activity_dict['start_date'], 
                activity_dict['utc_offset'], activity_dict['gear_id'], activity_dict['gear_name'], 
                activity_dict['distance'], activity_dict['elapsed_time'], activity_dict['moving_time'], 
                activity_dict['calories'], activity_dict['average_heartrate'], activity_dict['max_heartrate'], 
                activity_dict['average_watts'], activity_dict['max_watts'], activity_dict['average_speed'], 
                activity_dict['max_speed'], activity_dict['type'], activity_dict['sport_type'], 
                activity_dict['total_elevation_gain'], activity_dict['kudos_count'], 
                activity_dict['weighted_average_watts'], activity_dict['average_cadence'], 
                activity_dict['trainer'], activity_dict['map_polyline'], activity_dict['device_name'], 
                activity_dict['timezone'], start_latlng_json, end_latlng_json
            )
            
            cursor.execute(insert_sql, values)
            affected_rows = cursor.rowcount
            
            cursor.close()
            conn.close()
            
            logger.info(f"Saved activity to database. Affected rows: {affected_rows}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving activity to database: {e}")
            return False

# ===== WEBHOOK PROCESSING =====
class WebhookProcessor:
    """Enhanced webhook processor with better memory management"""
    
    def __init__(self, config: WebhookConfig, email_service: EmailNotificationService):
        self.config = config
        self.email_service = email_service
        self.db_manager = DatabaseManager(config)
        self.activity_processor = ActivityProcessor(config)
        
        # Use improved cache for webhook deduplication
        self.webhook_cache = ImprovedCacheManager(max_size=1000, cleanup_interval=600)  # 10 minutes
    
    def is_duplicate_webhook(self, event_data):
        """Check if this webhook event is a duplicate using improved cache"""
        event_time = event_data.get('event_time', int(time.time()))
        activity_id = event_data.get('object_id')
        aspect_type = event_data.get('aspect_type')
        
        event_key = f"{activity_id}_{aspect_type}_{event_time}"
        
        # Check for recent duplicates
        recent_webhook = self.webhook_cache.get(event_key)
        if recent_webhook:
            logger.info(f"Duplicate webhook ignored: {event_key}")
            return True
        
        # Record this webhook (10 minute TTL)
        self.webhook_cache.set(event_key, time.time(), ttl=600)
        return False
    
    def get_athlete_name_from_id(self, owner_id):
        """Get athlete name from owner_id"""
        return self.config.athlete_mappings.get(int(owner_id))
    
    async def process_webhook_event(self, event_data):
        """Process webhook event with better error handling"""
        try:
            # Extract event details
            object_type = event_data.get('object_type')
            aspect_type = event_data.get('aspect_type')
            activity_id = event_data.get('object_id')
            owner_id = event_data.get('owner_id')
            
            # Log the webhook event using improved database method
            self.db_manager.add_webhook_event(
                activity_id, owner_id, object_type, aspect_type, event_data
            )
            
            # Only process activity events
            if object_type != 'activity' or aspect_type not in ['create', 'update']:
                logger.info(f"Ignoring non-activity event: {object_type}/{aspect_type}")
                return True
            
            # Get athlete name
            athlete_name = self.get_athlete_name_from_id(owner_id)
            if not athlete_name:
                logger.warning(f"Unknown athlete ID: {owner_id}")
                return True
            
            logger.info(f"Processing {aspect_type} event for activity {activity_id} ({athlete_name})")
            
            # Check if activity exists using improved method
            activity_exists = self.db_manager.activity_exists(activity_id, owner_id)
            
            # Handle create events
            if aspect_type == 'create':
                if not activity_exists:
                    success = await self.activity_processor.fetch_and_save_activity(
                        activity_id, owner_id, athlete_name
                    )
                    if success:
                        await self._send_activity_notification(activity_id, 'create')
                else:
                    # Activity already exists, just send notification
                    await self._send_activity_notification(activity_id, 'create')
            
            # Handle update events
            elif aspect_type == 'update':
                success = await self.activity_processor.fetch_and_save_activity(
                    activity_id, owner_id, athlete_name
                )
                if success:
                    await self._send_activity_notification(activity_id, 'update')
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing webhook event: {e}")
            return False
    
    async def _send_activity_notification(self, activity_id, event_type):
        """Send immediate email notification for activity"""
        try:
            if not self.email_service.should_send_email(activity_id, event_type):
                return
            
            # Get activity details
            activity = self.email_service.get_activity_details(activity_id)
            if not activity:
                logger.warning(f"Could not get activity details for notification: {activity_id}")
                return
            
            # Get athlete email
            athlete_name = activity['athlete_name'].lower()
            athlete_email = self.email_service.athlete_emails.get(athlete_name)
            
            if not athlete_email:
                logger.warning(f"No email configured for athlete: {activity['athlete_name']}")
                return
            
            # Generate email content
            subject, body = self._generate_activity_email(activity, event_type)
            
            # Send email
            success = self.email_service.send_email(subject, body, athlete_email)
            if success:
                logger.info(f"Sent {event_type} notification for activity {activity_id}")
            
        except Exception as e:
            logger.error(f"Error sending activity notification: {e}")
    
    def _generate_activity_email(self, activity, event_type):
        """Generate email content for activity notification"""
        # Get statistics
        today = datetime.now().date()
        seven_days_ago = today - timedelta(days=6)
        this_monday = today - timedelta(days=today.weekday())
        
        # Get 7-day and week-to-date stats
        week_stats = self.email_service.get_activity_stats(
            athlete_name=activity['athlete_name'],
            start_date=seven_days_ago,
            end_date=today
        )
        
        wtd_stats = self.email_service.get_activity_stats(
            athlete_name=activity['athlete_name'],
            start_date=this_monday,
            end_date=today
        )
        
        # Get athlete-specific stats
        athlete_key = activity['athlete_name'].lower()
        athlete_week_stats = week_stats.get(athlete_key, {
            'rides': 0, 'distance_km': 0, 'elevation_m': 0, 'calories': 0, 'moving_time_hours': 0
        })
        athlete_wtd_stats = wtd_stats.get(athlete_key, {
            'rides': 0, 'distance_km': 0, 'elevation_m': 0, 'calories': 0, 'moving_time_hours': 0
        })
        
        # Determine event details
        event_emoji = "🆕" if event_type == "create" else "🔄"
        event_title = "New Ride" if event_type == "create" else "Updated Ride"
        
        subject = f"{event_emoji} {activity['name']}"
        
        body = f"""
        <html>
        <body style="font-family: Arial, sans-serif; color: #333; line-height: 1.6;">
            <h2 style="color: #FC4C02;">🚴‍♂️ {event_title} - {activity['athlete_name']}!</h2>
            
            <div style="background: #f8f9fa; padding: 20px; border-radius: 8px; margin: 20px 0;">
                <h3 style="margin-top: 0; color: #FC4C02;">📍 Activity Details</h3>
                <p><strong>Activity:</strong> {activity['name']}</p>
                <p><strong>Date:</strong> {activity['date'].strftime('%A, %B %d, %Y at %I:%M %p') if activity['date'] else 'Unknown'}</p>
                <p><strong>Distance:</strong> {self.email_service.format_number(activity['distance_km'])} km</p>
                <p><strong>Elevation:</strong> {self.email_service.format_number(activity['elevation_m'])} m</p>
                <p><strong>Calories:</strong> {self.email_service.format_number(activity['calories'])}</p>
            </div>
            
            <div style="background: #e3f2fd; padding: 20px; border-radius: 8px; margin: 20px 0;">
                <h3 style="margin-top: 0; color: #1976d2;">📊 Your Last 7 Days</h3>
                <p><strong>Total Rides:</strong> {self.email_service.format_number(athlete_week_stats['rides'])}</p>
                <p><strong>Total Distance:</strong> {self.email_service.format_number(athlete_week_stats['distance_km'])} km</p>
                <p><strong>Total Elevation:</strong> {self.email_service.format_number(athlete_week_stats['elevation_m'])} m</p>
                <p><strong>Total Calories:</strong> {self.email_service.format_number(athlete_week_stats['calories'])}</p>
                
                {self.email_service.get_calorie_equivalents(athlete_week_stats['calories'])}
            </div>
            
            <div style="background: #f3e5f5; padding: 20px; border-radius: 8px; margin: 20px 0;">
                <h3 style="margin-top: 0; color: #7b1fa2;">📅 Week to Date ({this_monday.strftime('%b %d')} - {today.strftime('%b %d')})</h3>
                <p><strong>Rides This Week:</strong> {self.email_service.format_number(athlete_wtd_stats['rides'])}</p>
                <p><strong>Distance This Week:</strong> {self.email_service.format_number(athlete_wtd_stats['distance_km'])} km</p>
                <p><strong>Elevation This Week:</strong> {self.email_service.format_number(athlete_wtd_stats['elevation_m'])} m</p>
                <p><strong>Calories This Week:</strong> {self.email_service.format_number(athlete_wtd_stats['calories'])}</p>
                
                {self.email_service.get_calorie_equivalents(athlete_wtd_stats['calories'])}
            </div>
            
            <p style="color: #666; font-size: 12px; margin: 30px 0;">
                <em>🤖 Strava Webhook Server - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</em><br>
                <em>📊 Data refreshed from Strava API at {datetime.now().strftime('%H:%M')}</em>
            </p>
        </body>
        </html>
        """
        
        return subject, body

# ===== EMAIL SUMMARY SYSTEM =====
class EmailSummaryService:
    """Handles periodic email summaries (weekly, monthly, annual)"""
    
    def __init__(self, email_service: EmailNotificationService):
        self.email_service = email_service
    
    def send_weekly_summary(self):
        """Send weekly cycling summary every Monday"""
        try:
            logger.info("Generating weekly summary")
            
            # Calculate last week's date range (Monday to Sunday)
            today = datetime.now().date()
            days_since_monday = today.weekday()
            last_monday = today - timedelta(days=days_since_monday + 7)
            last_sunday = last_monday + timedelta(days=6)
            
            # Get weekly stats
            week_stats = self.email_service.get_activity_stats(
                start_date=last_monday,
                end_date=last_sunday
            )
            
            if not week_stats:
                logger.info("No cycling activities for weekly summary")
                return
            
            subject = f"🚴‍♂️ Weekly Cycling Summary - {last_monday.strftime('%b %d')} to {last_sunday.strftime('%b %d, %Y')}"
            body = self._generate_weekly_summary_html(week_stats, last_monday, last_sunday)
            
            # Send to admin emails
            success = self.email_service.send_email(subject, body)
            if success:
                logger.info(f"Sent weekly summary for {last_monday} to {last_sunday}")
            
        except Exception as e:
            logger.error(f"Error sending weekly summary: {e}")
    
    def send_monthly_summary(self):
        """Send monthly cycling summary on 1st of each month"""
        try:
            logger.info("Generating monthly summary")
            
            # Calculate last month's date range
            today = datetime.now().date()
            first_of_this_month = today.replace(day=1)
            last_month_end = first_of_this_month - timedelta(days=1)
            last_month_start = last_month_end.replace(day=1)
            
            # Get monthly stats
            month_stats = self.email_service.get_activity_stats(
                start_date=last_month_start,
                end_date=last_month_end
            )
            
            if not month_stats:
                logger.info("No cycling activities for monthly summary")
                return
            
            month_name = last_month_end.strftime('%B %Y')
            subject = f"🚴‍♂️ Monthly Cycling Summary - {month_name}"
            body = self._generate_monthly_summary_html(month_stats, month_name)
            
            # Send to admin emails
            success = self.email_service.send_email(subject, body)
            if success:
                logger.info(f"Sent monthly summary for {month_name}")
            
        except Exception as e:
            logger.error(f"Error sending monthly summary: {e}")
    
    def send_annual_summary(self):
        """Send annual cycling summary on January 1st"""
        try:
            logger.info("Generating annual summary")
            
            # Calculate last year's date range
            today = datetime.now().date()
            last_year = today.year - 1
            year_start = datetime(last_year, 1, 1).date()
            year_end = datetime(last_year, 12, 31).date()
            
            # Get yearly stats
            year_stats = self.email_service.get_activity_stats(
                start_date=year_start,
                end_date=year_end
            )
            
            if not year_stats:
                logger.info("No cycling activities for annual summary")
                return
            
            subject = f"🚴‍♂️ Annual Cycling Summary - {last_year}"
            body = self._generate_annual_summary_html(year_stats, last_year)
            
            # Send to admin emails
            success = self.email_service.send_email(subject, body)
            if success:
                logger.info(f"Sent annual summary for {last_year}")
            
        except Exception as e:
            logger.error(f"Error sending annual summary: {e}")

    def _generate_weekly_summary_html(self, week_stats, last_monday, last_sunday):
        """Generate HTML content for weekly summary"""
        # Calculate totals
        total_rides = sum(stats['rides'] for stats in week_stats.values())
        total_distance = sum(stats['distance_km'] for stats in week_stats.values())
        total_elevation = sum(stats['elevation_m'] for stats in week_stats.values())
        total_calories = sum(stats['calories'] for stats in week_stats.values())
        
        # Generate athlete sections
        athlete_sections = ""
        for athlete, stats in week_stats.items():
            athlete_sections += f"""
            <div style="background: #f8f9fa; padding: 15px; border-radius: 8px; margin: 10px 0;">
                <h4 style="margin-top: 0; color: #FC4C02;">🚴‍♂️ {athlete.title()}</h4>
                <p><strong>Rides:</strong> {self.email_service.format_number(stats['rides'])}</p>
                <p><strong>Distance:</strong> {self.email_service.format_number(stats['distance_km'])} km</p>
                <p><strong>Elevation:</strong> {self.email_service.format_number(stats['elevation_m'])} m</p>
                <p><strong>Calories:</strong> {self.email_service.format_number(stats['calories'])}</p>
                <p><strong>Time:</strong> {self.email_service.format_number(stats['moving_time_hours'])} hours</p>
                
                {self.email_service.get_calorie_equivalents(stats['calories'])}
            </div>
            """
        
        return f"""
        <html>
        <body style="font-family: Arial, sans-serif; color: #333; line-height: 1.6;">
            <h2 style="color: #FC4C02;">🚴‍♂️ Weekly Cycling Summary</h2>
            <p><strong>Week of:</strong> {last_monday.strftime('%B %d')} - {last_sunday.strftime('%B %d, %Y')}</p>
            
            <div style="background: #e8f5e8; padding: 20px; border-radius: 8px; margin: 20px 0;">
                <h3 style="margin-top: 0; color: #2e7d32;">🏆 Combined Totals</h3>
                <p><strong>Total Rides:</strong> {self.email_service.format_number(total_rides)}</p>
                <p><strong>Total Distance:</strong> {self.email_service.format_number(total_distance)} km</p>
                <p><strong>Total Elevation:</strong> {self.email_service.format_number(total_elevation)} m</p>
                <p><strong>Total Calories:</strong> {self.email_service.format_number(total_calories)}</p>
            </div>
            
            <h3 style="color: #FC4C02;">👥 Individual Performance</h3>
            {athlete_sections}
            
            <p style="color: #666; font-size: 12px; margin-top: 30px;">
                <em>🤖 Weekly Summary - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</em>
            </p>
        </body>
        </html>
        """
    
    def _generate_monthly_summary_html(self, month_stats, month_name):
        """Generate HTML content for monthly summary"""
        # Calculate totals
        total_rides = sum(stats['rides'] for stats in month_stats.values())
        total_distance = sum(stats['distance_km'] for stats in month_stats.values())
        total_elevation = sum(stats['elevation_m'] for stats in month_stats.values())
        total_calories = sum(stats['calories'] for stats in month_stats.values())
        
        # Generate athlete sections
        athlete_sections = ""
        for athlete, stats in month_stats.items():
            avg_distance = stats['distance_km'] / stats['rides'] if stats['rides'] > 0 else 0
            
            athlete_sections += f"""
            <div style="background: #f8f9fa; padding: 15px; border-radius: 8px; margin: 10px 0;">
                <h4 style="margin-top: 0; color: #FC4C02;">🚴‍♂️ {athlete.title()}</h4>
                <p><strong>Rides:</strong> {self.email_service.format_number(stats['rides'])}</p>
                <p><strong>Distance:</strong> {self.email_service.format_number(stats['distance_km'])} km</p>
                <p><strong>Avg Distance:</strong> {self.email_service.format_number(avg_distance)} km</p>
                <p><strong>Elevation:</strong> {self.email_service.format_number(stats['elevation_m'])} m</p>
                <p><strong>Calories:</strong> {self.email_service.format_number(stats['calories'])}</p>
                <p><strong>Time:</strong> {self.email_service.format_number(stats['moving_time_hours'])} hours</p>
                
                {self.email_service.get_calorie_equivalents(stats['calories'])}
            </div>
            """
        
        return f"""
        <html>
        <body style="font-family: Arial, sans-serif; color: #333; line-height: 1.6;">
            <h2 style="color: #FC4C02;">🚴‍♂️ Monthly Cycling Summary</h2>
            <p><strong>Month:</strong> {month_name}</p>
            
            <div style="background: #e3f2fd; padding: 20px; border-radius: 8px; margin: 20px 0;">
                <h3 style="margin-top: 0; color: #1976d2;">🏆 Combined Monthly Totals</h3>
                <p><strong>Total Rides:</strong> {self.email_service.format_number(total_rides)}</p>
                <p><strong>Total Distance:</strong> {self.email_service.format_number(total_distance)} km</p>
                <p><strong>Total Elevation:</strong> {self.email_service.format_number(total_elevation)} m</p>
                <p><strong>Total Calories:</strong> {self.email_service.format_number(total_calories)}</p>
                <p><strong>Average per Ride:</strong> {self.email_service.format_number(total_distance / total_rides if total_rides > 0 else 0)} km</p>
            </div>
            
            <h3 style="color: #FC4C02;">👥 Individual Performance</h3>
            {athlete_sections}
            
            <p style="color: #666; font-size: 12px; margin-top: 30px;">
                <em>🤖 Monthly Summary - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</em>
            </p>
        </body>
        </html>
        """
    
    def _generate_annual_summary_html(self, year_stats, last_year):
        """Generate HTML content for annual summary"""
        # Calculate totals
        total_rides = sum(stats['rides'] for stats in year_stats.values())
        total_distance = sum(stats['distance_km'] for stats in year_stats.values())
        total_elevation = sum(stats['elevation_m'] for stats in year_stats.values())
        total_calories = sum(stats['calories'] for stats in year_stats.values())
        total_time = sum(stats['moving_time_hours'] for stats in year_stats.values())
        
        # Fun comparisons
        distance_around_earth = total_distance / 40075  # Earth's circumference
        everest_climbs = total_elevation / 8849  # Height of Everest
        
        # Generate athlete sections
        athlete_sections = ""
        for athlete, stats in year_stats.items():
            avg_distance = stats['distance_km'] / stats['rides'] if stats['rides'] > 0 else 0
            rides_per_week = stats['rides'] / 52  # Approximate
            
            athlete_sections += f"""
            <div style="background: #f8f9fa; padding: 20px; border-radius: 8px; margin: 15px 0;">
                <h4 style="margin-top: 0; color: #FC4C02;">🚴‍♂️ {athlete.title()}</h4>
                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 10px;">
                    <div>
                        <p><strong>Total Rides:</strong> {self.email_service.format_number(stats['rides'])}</p>
                        <p><strong>Total Distance:</strong> {self.email_service.format_number(stats['distance_km'])} km</p>
                        <p><strong>Total Elevation:</strong> {self.email_service.format_number(stats['elevation_m'])} m</p>
                    </div>
                    <div>
                        <p><strong>Avg Distance:</strong> {self.email_service.format_number(avg_distance)} km</p>
                        <p><strong>Rides/Week:</strong> {self.email_service.format_number(rides_per_week)}</p>
                        <p><strong>Total Time:</strong> {self.email_service.format_number(stats['moving_time_hours'])} hrs</p>
                    </div>
                </div>
                <p><strong>Calories Burned:</strong> {self.email_service.format_number(stats['calories'])}</p>
                
                {self.email_service.get_calorie_equivalents(stats['calories'])}
            </div>
            """
        
        return f"""
        <html>
        <body style="font-family: Arial, sans-serif; color: #333; line-height: 1.6;">
            <h2 style="color: #FC4C02;">🚴‍♂️ Annual Cycling Summary</h2>
            <p><strong>Year:</strong> {last_year}</p>
            
            <div style="background: #fff3e0; padding: 20px; border-radius: 8px; margin: 20px 0;">
                <h3 style="margin-top: 0; color: #f57c00;">🏆 Epic Year in Numbers</h3>
                <p><strong>Total Rides:</strong> {self.email_service.format_number(total_rides)}</p>
                <p><strong>Total Distance:</strong> {self.email_service.format_number(total_distance)} km</p>
                <p><strong>Total Elevation:</strong> {self.email_service.format_number(total_elevation)} m</p>
                <p><strong>Total Time:</strong> {self.email_service.format_number(total_time)} hours</p>
                <p><strong>Total Calories:</strong> {self.email_service.format_number(total_calories)}</p>
            </div>
            
            <div style="background: #e8f5e8; padding: 20px; border-radius: 8px; margin: 20px 0;">
                <h3 style="margin-top: 0; color: #2e7d32;">🌍 Fun Comparisons</h3>
                <p><strong>Around the Earth:</strong> {distance_around_earth:.1f} times</p>
                <p><strong>Mount Everest:</strong> Climbed {everest_climbs:.1f} times</p>
                <p><strong>Average per Week:</strong> {self.email_service.format_number(total_rides / 52)} rides, {self.email_service.format_number(total_distance / 52)} km</p>
            </div>
            
            <h3 style="color: #FC4C02;">👥 Individual Achievements</h3>
            {athlete_sections}
            
            <div style="background: #f3e5f5; padding: 20px; border-radius: 8px; margin: 20px 0;">
                <h3 style="margin-top: 0; color: #7b1fa2;">🎉 What an Amazing Year!</h3>
                <p>You've accomplished something incredible. Every ride, every kilometer, every climb - it all adds up to this amazing journey. Here's to an even better {datetime.now().year}!</p>
            </div>
            
            <p style="color: #666; font-size: 12px; margin-top: 30px;">
                <em>🤖 Annual Summary - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</em>
            </p>
        </body>
        </html>
        """

# ===== INITIALIZATION SECTION =====
# Initialize all services
try:
    webhook_config.validate_config()
    logger.info("Configuration validated successfully")
except ValueError as e:
    logger.error(f"Configuration validation failed: {e}")
    exit(1)

# Initialize services
email_service = EmailNotificationService(webhook_config)
webhook_processor = WebhookProcessor(webhook_config, email_service)
summary_service = EmailSummaryService(email_service)
db_manager = DatabaseManager(webhook_config)

# Test database connection
if not db_manager.test_connection():
    logger.error("Database connection test failed")
    exit(1)

logger.info("All services initialized successfully")
# ===== FLASK ROUTES AND API ENDPOINTS =====

@app.route('/webhook', methods=['GET'])
def webhook_challenge():
    """Handle Strava webhook subscription challenge"""
    challenge = request.args.get('hub.challenge')
    verify_token = request.args.get('hub.verify_token')
    
    logger.info(f"Webhook challenge received. Token match: {verify_token == webhook_config.webhook_verify_token}")
    
    if verify_token == webhook_config.webhook_verify_token:
        logger.info("Webhook challenge verified successfully")
        return jsonify({'hub.challenge': challenge})
    else:
        logger.warning("Webhook challenge failed - invalid verify token")
        return "Forbidden", 403

@app.route('/webhook', methods=['POST'])
def webhook_event():
    """Handle incoming webhook events with improved memory management"""
    try:
        event_data = request.get_json()
        
        if not event_data:
            logger.warning("Received webhook with no data")
            return "Bad Request", 400
        
        logger.info(f"Received webhook: {event_data.get('object_type')}/{event_data.get('aspect_type')} for {event_data.get('object_id')}")
        
        # Check for duplicate webhooks
        if webhook_processor.is_duplicate_webhook(event_data):
            return "OK", 200
        
        # IMPROVED: Better event loop management
        try:
            # Try to get existing event loop first
            try:
                loop = asyncio.get_event_loop()
                if loop.is_closed():
                    raise RuntimeError("Loop is closed")
            except RuntimeError:
                # Create new loop only if necessary
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            
            # Process webhook
            success = loop.run_until_complete(webhook_processor.process_webhook_event(event_data))
            
            # CRITICAL: Clean up any remaining tasks
            pending_tasks = [task for task in asyncio.all_tasks(loop) if not task.done()]
            if pending_tasks:
                logger.warning(f"Cleaning up {len(pending_tasks)} pending tasks")
                for task in pending_tasks:
                    task.cancel()
                
                # Wait for cancellation to complete
                loop.run_until_complete(asyncio.gather(*pending_tasks, return_exceptions=True))
            
            return "OK" if success else "Processing Error", 200 if success else 500
            
        except Exception as e:
            logger.error(f"Event loop error: {e}")
            return "Processing Error", 500
        
    except Exception as e:
        logger.error(f"Error processing webhook: {e}")
        return "Internal Server Error", 500

@app.route('/stats', methods=['GET'])
def webhook_stats():
    """Get webhook statistics and system metrics"""
    try:
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        
        # Get webhook event statistics
        cursor.execute('''
            SELECT COUNT(*) as total_events,
                   SUM(CASE WHEN event_type = 'create' THEN 1 ELSE 0 END) as creates,
                   SUM(CASE WHEN event_type = 'update' THEN 1 ELSE 0 END) as updates
            FROM webhook_events
            WHERE received_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
        ''')
        
        webhook_stats = cursor.fetchone()
        
        # Get recent activity count
        cursor.execute('''
            SELECT COUNT(*) FROM strava_activities 
            WHERE data_loaded_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
        ''')
        
        recent_activities = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'webhook_events_last_7_days': {
                'total': webhook_stats[0] if webhook_stats else 0,
                'creates': webhook_stats[1] if webhook_stats else 0,
                'updates': webhook_stats[2] if webhook_stats else 0
            },
            'activities_processed_last_7_days': recent_activities,
            'email_service': {
                'recent_emails_cache_size': email_service.email_cache.size(),  # ✅ CORRECT
                'configured_athletes': len(email_service.athlete_emails)
            },
            'webhook_processor': {
                'recent_webhooks_cache_size': webhook_processor.webhook_cache.size()  # ✅ CORRECT
            },
            'system': {
                'uptime_check': datetime.now().isoformat(),
                'configuration_valid': True
            }
        })
        
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        return jsonify({'error': str(e)}), 500

# ===== EMERGENCY FIX 4: Check What Went Wrong =====
@app.route('/debug/startup', methods=['GET'])
def debug_startup_status():
    """Debug what components loaded successfully"""
    status = {
        "timestamp": datetime.now().isoformat(),
        "python_modules": {},
        "global_objects": {},
        "imports": {}
    }
    
    # Check important imports
    try:
        import psutil
        status["imports"]["psutil"] = "available"
    except ImportError:
        status["imports"]["psutil"] = "missing - run: pip install psutil"
    
    try:
        import gc
        status["imports"]["gc"] = "available"
    except ImportError:
        status["imports"]["gc"] = "missing"
    
    # Check global objects
    globals_to_check = [
        'email_service', 'webhook_processor', 'db_manager', 
        'webhook_config', 'scheduler'
    ]
    
    for obj_name in globals_to_check:
        if obj_name in globals():
            obj = globals()[obj_name]
            status["global_objects"][obj_name] = {
                "exists": True,
                "type": str(type(obj)),
                "has_cache": hasattr(obj, 'email_cache') or hasattr(obj, 'webhook_cache')
            }
        else:
            status["global_objects"][obj_name] = {"exists": False}
    
    # Check class definitions
    classes_to_check = [
        'ImprovedCacheManager', 'DatabaseManager', 
        'EmailNotificationService', 'WebhookProcessor'
    ]
    
    for class_name in classes_to_check:
        if class_name in globals():
            status["python_modules"][class_name] = "defined"
        else:
            status["python_modules"][class_name] = "missing"
    
    return jsonify(status)

@app.route('/debug/memory', methods=['GET'])
def debug_memory_status():
    """Debug endpoint for memory status - safe version"""
    try:
        # Try to import psutil, fallback if not available
        try:
            import psutil
            import gc
            
            # Get process memory info
            process = psutil.Process()
            memory_info = process.memory_info()
            
            # Force garbage collection
            collected = gc.collect()
            
            memory_data = {
                "rss_mb": round(memory_info.rss / 1024 / 1024, 1),
                "vms_mb": round(memory_info.vms / 1024 / 1024, 1),
                "percent": process.memory_percent()
            }
            
            system_data = {
                "available_memory_mb": round(psutil.virtual_memory().available / 1024 / 1024, 1),
                "memory_percent": psutil.virtual_memory().percent
            }
            
        except ImportError:
            memory_data = {"error": "psutil not installed"}
            system_data = {"error": "psutil not installed"}
            collected = 0
        
        # Safely get cache sizes
        email_cache_size = 0
        webhook_cache_size = 0
        
        try:
            if 'email_service' in globals() and hasattr(email_service, 'email_cache'):
                email_cache_size = email_service.email_cache.size()
        except:
            pass
            
        try:
            if 'webhook_processor' in globals() and hasattr(webhook_processor, 'webhook_cache'):
                webhook_cache_size = webhook_processor.webhook_cache.size()
        except:
            pass
        
        return jsonify({
            "memory": memory_data,
            "system": system_data,
            "caches": {
                "email_cache_size": email_cache_size,
                "webhook_cache_size": webhook_cache_size
            },
            "garbage_collection": {
                "objects_collected": collected
            },
            "timestamp": datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Memory debug failed: {e}")
        return jsonify({
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

@app.route('/admin/clear-email-cache', methods=['POST'])
def clear_email_cache():
    """Clear email deduplication cache for testing"""
    try:
        email_service.clear_email_cache()
        return jsonify({
            "status": "success",
            "message": "Email deduplication cache cleared",
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Error clearing email cache: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/admin/clear-webhook-cache', methods=['POST'])
def clear_webhook_cache():
    """Clear webhook deduplication cache"""
    try:
        webhook_processor.webhook_cache.clear()
        return jsonify({
            "status": "success", 
            "message": "Webhook deduplication cache cleared",
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Error clearing webhook cache: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/test-email', methods=['POST'])
def test_email():
    """Send test emails for verification"""
    try:
        data = request.json if request.is_json else {}
        email_type = data.get('type', 'immediate')
        test_athlete = data.get('athlete', 'dominic')
        
        if email_type == 'weekly':
            summary_service.send_weekly_summary()
            return jsonify({
                "status": "success",
                "message": "Weekly summary test email sent",
                "recipients": "admin emails"
            })
            
        elif email_type == 'monthly':
            summary_service.send_monthly_summary()
            return jsonify({
                "status": "success",
                "message": "Monthly summary test email sent", 
                "recipients": "admin emails"
            })
            
        elif email_type == 'annual':
            summary_service.send_annual_summary()
            return jsonify({
                "status": "success",
                "message": "Annual summary test email sent",
                "recipients": "admin emails"
            })
            
        else:
            # Test immediate notification
            athlete_email = email_service.athlete_emails.get(test_athlete.lower())
            if not athlete_email:
                return jsonify({
                    "error": f"No email configured for athlete {test_athlete}",
                    "available_athletes": list(email_service.athlete_emails.keys())
                }), 400
            
            test_subject = f"🧪 Test Email - {test_athlete.title()}"
            test_body = f"""
            <html>
            <body style="font-family: Arial, sans-serif; color: #333;">
                <h2 style="color: #FC4C02;">🧪 Test Email for {test_athlete.title()}</h2>
                <p>This is a test email from the Enhanced Strava Webhook Server.</p>
                <div style="background: #f8f9fa; padding: 15px; border-radius: 8px; margin: 20px 0;">
                    <p><strong>Recipient:</strong> {athlete_email}</p>
                    <p><strong>Test Time:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                    <p><strong>Server Version:</strong> 2.0</p>
                </div>
                <p style="color: #28a745;">✅ Email system is working correctly!</p>
                <p style="color: #666; font-size: 12px; margin-top: 30px;">
                    <em>🤖 Strava Webhook Server Test</em>
                </p>
            </body>
            </html>
            """
            
            success = email_service.send_email(test_subject, test_body, athlete_email)
            
            return jsonify({
                "status": "success" if success else "failed",
                "message": f"Test email {'sent' if success else 'failed'} to {test_athlete}",
                "recipient": athlete_email
            })
        
    except Exception as e:
        logger.error(f"Error sending test email: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/refresh-activity/<int:activity_id>', methods=['POST'])
def manual_refresh_activity(activity_id):
    """Manually refresh a specific activity"""
    try:
        # Get activity details from database
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT athlete_id, athlete_name FROM strava_activities WHERE id = %s', (activity_id,))
        result = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if not result:
            return jsonify({"error": "Activity not found in database"}), 404
        
        athlete_id, athlete_name = result
        logger.info(f"Manual refresh requested for activity {activity_id} ({athlete_name})")
        
        # Process refresh asynchronously
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            activity_processor = ActivityProcessor(webhook_config)
            success = loop.run_until_complete(
                activity_processor.fetch_and_save_activity(activity_id, athlete_id, athlete_name)
            )
            
            return jsonify({
                "status": "success" if success else "failed",
                "message": f"Activity {activity_id} {'refreshed successfully' if success else 'refresh failed'}",
                "activity_id": activity_id,
                "athlete": athlete_name
            })
            
        finally:
            loop.close()
        
    except Exception as e:
        logger.error(f"Error in manual activity refresh: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/debug/activity-stats/<athlete_name>', methods=['GET'])
def debug_activity_stats(athlete_name):
    """Debug endpoint for activity statistics"""
    try:
        # Get date range parameters
        days = request.args.get('days', 7, type=int)
        today = datetime.now().date()
        start_date = today - timedelta(days=days-1)
        
        logger.info(f"Getting {days}-day stats for {athlete_name} from {start_date} to {today}")
        
        # Get statistics
        stats = email_service.get_activity_stats(
            athlete_name=athlete_name,
            start_date=start_date,
            end_date=today
        )
        
        return jsonify({
            "athlete": athlete_name,
            "date_range": {
                "start_date": start_date.isoformat(),
                "end_date": today.isoformat(),
                "days": days
            },
            "stats": stats,
            "debug_info": {
                "query_timestamp": datetime.now().isoformat(),
                "available_athletes": list(stats.keys()) if stats else []
            }
        })
        
    except Exception as e:
        logger.error(f"Error in debug stats: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/debug/database-test', methods=['GET'])
def debug_database_test():
    """Test database connectivity and basic queries"""
    try:
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        
        # Test basic connectivity
        cursor.execute('SELECT 1 as test')
        connectivity_test = cursor.fetchone()[0]
        
        # Get table information
        cursor.execute('SHOW TABLES')
        tables = [row[0] for row in cursor.fetchall()]
        
        # Get recent activity count
        cursor.execute('SELECT COUNT(*) FROM strava_activities WHERE created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)')
        recent_activities = cursor.fetchone()[0]
        
        # Get webhook events count
        cursor.execute('SELECT COUNT(*) FROM webhook_events WHERE received_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR)')
        recent_webhooks = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return jsonify({
            "database_connectivity": "success",
            "connectivity_test_result": connectivity_test,
            "available_tables": tables,
            "recent_activities_7_days": recent_activities,
            "recent_webhook_events_24h": recent_webhooks,
            "database_config": {
                "host": webhook_config.db_config['host'],
                "database": webhook_config.db_config['database'],
                "user": webhook_config.db_config['user']
            },
            "test_timestamp": datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Database test failed: {e}")
        return jsonify({
            "database_connectivity": "failed",
            "error": str(e),
            "test_timestamp": datetime.now().isoformat()
        }), 500

# ===== CALENDAR WEBHOOK ROUTES =====
@app.route('/webhook/calendar', methods=['POST', 'GET'])
def handle_calendar_webhook():
    """Handle calendar webhook with better memory management"""
    
    # Handle validation request
    validation_token = request.args.get('validationToken')
    if validation_token:
        logger.info("Calendar webhook validation request received")
        return validation_token, 200, {'Content-Type': 'text/plain'}
    
    # Handle notification (POST)
    if request.method == 'POST':
        try:
            data = request.get_json(force=True)
            
            if not data or 'value' not in data:
                return 'Invalid notification data', 400
            
            logger.info(f"Calendar webhook received {len(data['value'])} notifications")
            
            # Process notifications in a more memory-efficient way
            for notification in data['value']:
                try:
                    process_calendar_notification(notification)
                except Exception as e:
                    logger.error(f"Error processing calendar notification: {e}")
                    # Continue with other notifications
            
            # Force cleanup after processing
            import gc
            gc.collect()
            
            return '', 202  # Accepted
            
        except Exception as e:
            logger.error(f"Error processing calendar webhook notification: {e}")
            return 'Error processing notification', 500
    
    return 'Invalid request', 400

@app.route('/admin/force-cleanup', methods=['POST'])
def force_memory_cleanup():
    """Force immediate memory cleanup - safe version"""
    try:
        import gc
        
        # Force garbage collection
        collected = gc.collect()
        
        # Safely clean up caches
        cache_results = {}
        
        try:
            if 'email_service' in globals() and hasattr(email_service, 'email_cache'):
                email_service.email_cache._force_cleanup()
                cache_results['email_cache'] = 'cleaned'
            else:
                cache_results['email_cache'] = 'not available'
        except Exception as e:
            cache_results['email_cache'] = f'error: {str(e)}'
        
        try:
            if 'webhook_processor' in globals() and hasattr(webhook_processor, 'webhook_cache'):
                webhook_processor.webhook_cache._force_cleanup()
                cache_results['webhook_cache'] = 'cleaned'
            else:
                cache_results['webhook_cache'] = 'not available'
        except Exception as e:
            cache_results['webhook_cache'] = f'error: {str(e)}'
        
        return jsonify({
            "status": "success",
            "message": "Memory cleanup completed",
            "objects_collected": collected,
            "cache_results": cache_results,
            "timestamp": datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Force cleanup failed: {e}")
        return jsonify({"error": str(e)}), 500

# ===== COMPLETE FIX 6: Memory Cleanup Functions =====
def setup_memory_cleanup_scheduler():
    """Setup periodic memory cleanup - safe version"""
    def cleanup_memory():
        """Perform memory cleanup tasks - safe version"""
        try:
            import gc
            
            # Force garbage collection
            collected = gc.collect()
            
            # Safely clean up caches
            try:
                if 'email_service' in globals() and hasattr(email_service, 'email_cache'):
                    email_service.email_cache._force_cleanup()
            except Exception as e:
                logger.warning(f"Email cache cleanup failed: {e}")
            
            try:
                if 'webhook_processor' in globals() and hasattr(webhook_processor, 'webhook_cache'):
                    webhook_processor.webhook_cache._force_cleanup()
            except Exception as e:
                logger.warning(f"Webhook cache cleanup failed: {e}")
            
            logger.info(f"Memory cleanup completed: {collected} objects collected")
            
        except Exception as e:
            logger.error(f"Memory cleanup failed: {e}")
    
    # Only add memory cleanup if scheduler exists
    try:
        if 'scheduler' in globals() and scheduler:
            scheduler.add_job(
                func=cleanup_memory,
                trigger='interval',
                minutes=15,
                id='memory_cleanup',
                name='Memory Cleanup Task'
            )
            logger.info("Memory cleanup scheduler configured")
    except Exception as e:
        logger.warning(f"Could not setup memory cleanup scheduler: {e}")

def process_calendar_notification(notification):
    """Process a single calendar notification"""
    try:
        subscription_id = notification.get('subscriptionId')
        change_type = notification.get('changeType')
        resource = notification.get('resource')
        client_state = notification.get('clientState', '')
        
        logger.info(f"Processing calendar notification: {change_type} for {resource}")
        
        # Verify client state
        if not client_state.startswith('calendar_webhook_'):
            logger.warning(f"Invalid client state: {client_state}")
            return
        
        # Extract user email from client state
        parts = client_state.split('_')
        if len(parts) >= 3:
            user_email = parts[2]
        else:
            logger.warning(f"Could not extract user email from client state: {client_state}")
            return
        
        # Trigger immediate calendar sync for this user
        trigger_calendar_sync(user_email)
        
    except Exception as e:
        logger.error(f"Error processing calendar notification: {e}")

def trigger_calendar_sync(user_email):
    """Trigger a calendar sync for a specific user"""
    try:
        logger.info(f"Triggering calendar sync for {user_email}")
        
        events = fetch_calendar_events(user_email)
        if events is not None:
            if save_calendar_events(user_email, events):
                logger.info(f"Successfully synced {len(events)} events for {user_email}")
                
                # Optional: Send email notification about calendar sync
                email_service.send_email(
                    f"📅 Calendar Updated - {user_email}",
                    f"""
                    <html>
                    <body style="font-family: Arial, sans-serif;">
                        <h3>📅 Calendar Synchronized</h3>
                        <p>Calendar events have been updated for <strong>{user_email}</strong></p>
                        <p><strong>Events synced:</strong> {len(events)}</p>
                        <p><strong>Sync time:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                        <p style="color: #666; font-size: 12px;">
                            <em>🤖 Dashboard Webhook Server</em>
                        </p>
                    </body>
                    </html>
                    """,
                    [user_email]
                )
                
            else:
                logger.error(f"Failed to save events for {user_email}")
        else:
            logger.error(f"Failed to fetch events for {user_email}")
            
    except Exception as e:
        logger.error(f"Error during calendar sync for {user_email}: {e}")


# ===== SCHEDULER SETUP =====
def setup_email_scheduler():
    """Setup and start the email scheduler for periodic summaries"""
    try:
        scheduler = BackgroundScheduler(daemon=True)
        
        # Weekly summary - every Monday at 9 AM
        scheduler.add_job(
            func=summary_service.send_weekly_summary,
            trigger=CronTrigger(day_of_week=0, hour=9, minute=0),
            id='weekly_summary',
            name='Weekly Cycling Summary',
            misfire_grace_time=3600  # 1 hour grace period
        )
        
        # Monthly summary - 1st of each month at 9 AM
        scheduler.add_job(
            func=summary_service.send_monthly_summary,
            trigger=CronTrigger(day=1, hour=9, minute=0),
            id='monthly_summary',
            name='Monthly Cycling Summary',
            misfire_grace_time=3600
        )
        
        # Annual summary - January 1st at 9 AM
        scheduler.add_job(
            func=summary_service.send_annual_summary,
            trigger=CronTrigger(month=1, day=1, hour=9, minute=0),
            id='annual_summary',
            name='Annual Cycling Summary',
            misfire_grace_time=3600
        )
        
        scheduler.start()
        logger.info("Email scheduler started successfully")
        logger.info(f"Scheduled jobs: {[job.name for job in scheduler.get_jobs()]}")
        
        return scheduler
        
    except Exception as e:
        logger.error(f"Failed to setup scheduler: {e}")
        return None

# ===== APPLICATION STARTUP =====
def print_startup_banner():
    """Print startup information banner"""
    print("\n" + "="*70)
    print("🚀 Enhanced Strava Webhook Server v2.0")
    print("="*70)
    print(f"📍 Strava webhook endpoint: http://dashboard.allkins.com/webhook")
    print(f"📅 Calendar webhook: http://dashboard.allkins.com/webhook/calendar")
    print(f"🏥 Health check: http://dashboard.allkins.com/health")
    print(f"📊 Statistics: http://dashboard.allkins.com/stats")
    print(f"🧪 Test email: POST http://dashboard.allkins.com/test-email")
    print(f"⚙️  Admin tools: http://dashboard.allkins.com/admin/*")
    print("\n📧 Email Notifications:")
    print("   • Immediate: New/updated rides")
    print("   • Weekly: Every Monday at 9 AM")
    print("   • Monthly: 1st of month at 9 AM")
    print("   • Annual: January 1st at 9 AM")
    print("\n👥 Configured Athletes:")
    for athlete_id, athlete_name in webhook_config.athlete_mappings.items():
        if athlete_name and athlete_id != 0:
            email = webhook_config.notification_emails.get(athlete_name.lower(), 'Not configured')
            print(f"   • {athlete_name} (ID: {athlete_id}) → {email}")
    print("\n🔧 Configuration Status:")
    print(f"   • Database: {webhook_config.db_config['host']}/{webhook_config.db_config['database']}")
    print(f"   • Email Service: {'✅ Configured' if webhook_config.brevo_api_key else '❌ Not configured'}")
    print(f"   • Webhook Token: {'✅ Configured' if webhook_config.webhook_verify_token != 'your_verify_token_here' else '❌ Not configured'}")
    print(f"   • Strava API: {'✅ Configured' if webhook_config.strava_client_id else '❌ Not configured'}")
    print("="*70)
    print("🎯 Server starting...")

def main():
    """Minimal main function that should start successfully"""
    try:
        print("\n" + "="*50)
        print("🚀 Starting Webhook Server (Safe Mode)")
        print("="*50)
        
        # Test database first
        if not db_manager.test_connection():
            logger.error("Database connection failed!")
            print("❌ Database connection failed")
        else:
            logger.info("Database connection successful")
            print("✅ Database connection successful")
        
        # Setup email scheduler (but don't fail if it doesn't work)
        try:
            scheduler = setup_email_scheduler()
            if scheduler:
                print("✅ Email scheduler started")
                
                # Try to setup memory cleanup (but don't fail if it doesn't work)
                try:
                    setup_memory_cleanup_scheduler()
                    print("✅ Memory cleanup scheduled")
                except Exception as e:
                    print(f"⚠️  Memory cleanup setup failed: {e}")
            else:
                print("⚠️  Email scheduler failed to start")
        except Exception as e:
            print(f"⚠️  Scheduler setup failed: {e}")
        
        print(f"🎯 Starting server on port {webhook_config.webhook_port}")
        print("📍 Debug endpoints:")
        print("   - /health")
        print("   - /debug/startup") 
        print("   - /debug/memory")
        print("="*50)
        
        # Start Flask application
        app.run(
            host='0.0.0.0',
            port=webhook_config.webhook_port,
            debug=False,
            threaded=True
        )
        
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
    except Exception as e:
        logger.error(f"Server startup failed: {e}")
        print(f"❌ Server startup failed: {e}")
        # Don't exit, just log the error
    finally:
        # Safe cleanup
        try:
            if 'scheduler' in locals() and scheduler:
                scheduler.shutdown(wait=False)
                logger.info("Scheduler shutdown completed")
        except:
            pass

# ===== ERROR HANDLERS =====
@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors"""
    return jsonify({
        'error': 'Endpoint not found',
        'available_endpoints': [
            '/webhook (GET/POST)',
            '/health (GET)',
            '/stats (GET)',
            '/test-email (POST)',
            '/admin/clear-email-cache (POST)',
            '/refresh-activity/<id> (POST)',
            '/debug/activity-stats/<athlete> (GET)'
        ]
    }), 404

@app.errorhandler(500)
def internal_error(error):
    """Handle 500 errors"""
    logger.error(f"Internal server error: {error}")
    return jsonify({
        'error': 'Internal server error',
        'timestamp': datetime.now().isoformat()
    }), 500

@app.errorhandler(405)
def method_not_allowed(error):
    """Handle 405 errors"""
    return jsonify({
        'error': 'Method not allowed',
        'message': 'Check the HTTP method for this endpoint'
    }), 405

# ===== APPLICATION ENTRY POINT =====
if __name__ == '__main__':
    main()