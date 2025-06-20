#!/usr/bin/env python3
"""
Strava Data Fetcher
Main application for fetching Strava ride data for multiple athletes
"""

import os
import sys
import logging
import asyncio
import aiohttp
import csv
import json
import mysql.connector
from mysql.connector import Error
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from dataclasses import dataclass, asdict
from pathlib import Path
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import time
from urllib.parse import urlencode
import webbrowser

# ADD THESE TWO LINES:
from dotenv import load_dotenv
load_dotenv()  # Load environment variables from .env file

# Configuration
@dataclass
class Config:
    # Required fields first
    strava_client_id: str
    strava_client_secret: str
    smtp_password: str
    
    # Optional fields with defaults
    db_path: str = ""  # Not used for MariaDB, kept for compatibility
    csv_path: str = "strava_rides.csv"
    smtp_email: str = "dominic.allkins@gmail.com" 
    notification_email: str = "dominic@allkins.com"
    test_mode: bool = False
    max_test_activities: int = 5
    rate_limit_requests_per_15min: int = 100
    rate_limit_daily: int = 1000
    athletes: Dict[str, Dict] = None
    
    def __post_init__(self):
        if self.athletes is None:
            self.athletes = {}
    
    @classmethod
    def from_env(cls):
        # Build athletes dict dynamically, only including those with tokens
        athletes = {}
        
        # Dominic's config
        dominic_access = os.getenv("DOMINIC_ACCESS_TOKEN")
        dominic_refresh = os.getenv("DOMINIC_REFRESH_TOKEN")
        if dominic_access and dominic_refresh:
            athletes["dominic"] = {
                "access_token": dominic_access,
                "refresh_token": dominic_refresh,
                "expires_at": int(os.getenv("DOMINIC_TOKEN_EXPIRES", "0"))
            }
        
        # Clare's config
        clare_access = os.getenv("CLARE_ACCESS_TOKEN")
        clare_refresh = os.getenv("CLARE_REFRESH_TOKEN")
        if clare_access and clare_refresh:
            athletes["clare"] = {
                "access_token": clare_access,
                "refresh_token": clare_refresh,
                "expires_at": int(os.getenv("CLARE_TOKEN_EXPIRES", "0"))
            }
        
        return cls(
            strava_client_id=os.getenv("STRAVA_CLIENT_ID"),
            strava_client_secret=os.getenv("STRAVA_CLIENT_SECRET"),
            smtp_password=os.getenv("GMAIL_APP_PASSWORD"),
            csv_path=os.getenv("STRAVA_CSV_PATH", "strava_rides.csv"),
            smtp_email=os.getenv("STRAVA_SMTP_EMAIL", "dominic.allkins@gmail.com"),
            notification_email=os.getenv("STRAVA_NOTIFICATION_EMAIL", "dominic@allkins.com"),
            test_mode=os.getenv("TEST_MODE", "false").lower() == "true",
            max_test_activities=int(os.getenv("STRAVA_MAX_TEST_ACTIVITIES", "3")),
            athletes=athletes
        )
    
@dataclass
class StravaActivity:
    """Data class for Strava activity with all required fields"""
    id: int
    athlete_id: int  # Added athlete_id field
    name: str
    start_date_local: str
    start_date: str
    utc_offset: float
    gear_id: Optional[str]
    gear_name: Optional[str]
    distance: float
    elapsed_time: int
    moving_time: int
    calories: Optional[float]
    average_heartrate: Optional[float]
    max_heartrate: Optional[float]
    average_watts: Optional[float]
    max_watts: Optional[float]
    average_speed: float
    max_speed: float
    type: str
    sport_type: str
    total_elevation_gain: float
    kudos_count: int
    weighted_average_watts: Optional[float]
    average_cadence: Optional[float]
    trainer: bool
    map_polyline: Optional[str]
    device_name: Optional[str]
    timezone: str
    start_latlng: Optional[str]
    end_latlng: Optional[str]
    athlete_name: str  # Added to track which athlete

class StravaAPI:
    """Handles Strava API interactions with rate limiting and token refresh"""
    
    def __init__(self, config: Config):
        self.config = config
        self.base_url = "https://www.strava.com/api/v3"
        self.oauth_url = "https://www.strava.com/oauth"
        self.request_count = 0
        self.request_reset_time = time.time() + 900  # 15 minutes
        
    async def refresh_token(self, athlete_name: str) -> bool:
        """Refresh access token for athlete"""
        athlete_config = self.config.athletes[athlete_name]
        
        data = {
            'client_id': self.config.strava_client_id,
            'client_secret': self.config.strava_client_secret,
            'refresh_token': athlete_config['refresh_token'],
            'grant_type': 'refresh_token'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{self.oauth_url}/token", data=data) as response:
                if response.status == 200:
                    token_data = await response.json()
                    athlete_config['access_token'] = token_data['access_token']
                    athlete_config['refresh_token'] = token_data['refresh_token']
                    athlete_config['expires_at'] = token_data['expires_at']
                    
                    # Update environment variables (you'll need to persist these)
                    logging.info(f"Token refreshed for {athlete_name}")
                    return True
                else:
                    logging.error(f"Failed to refresh token for {athlete_name}: {response.status}")
                    return False
    
    def get_authorization_url(self, athlete_name: str) -> str:
        """Get authorization URL for initial OAuth setup"""
        params = {
            'client_id': self.config.strava_client_id,
            'response_type': 'code',
            'redirect_uri': 'http://localhost:8000/callback',
            'approval_prompt': 'force',
            'scope': 'read,activity:read_all',
            'state': athlete_name
        }
        return f"{self.oauth_url}/authorize?" + urlencode(params)
    
    async def exchange_code_for_tokens(self, code: str, athlete_name: str) -> bool:
        """Exchange authorization code for access tokens"""
        data = {
            'client_id': self.config.strava_client_id,
            'client_secret': self.config.strava_client_secret,
            'code': code,
            'grant_type': 'authorization_code'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{self.oauth_url}/token", data=data) as response:
                if response.status == 200:
                    token_data = await response.json()
                    self.config.athletes[athlete_name] = {
                        'access_token': token_data['access_token'],
                        'refresh_token': token_data['refresh_token'],
                        'expires_at': token_data['expires_at']
                    }
                    logging.info(f"Tokens obtained for {athlete_name}")
                    return True
                else:
                    logging.error(f"Failed to exchange code for {athlete_name}: {response.status}")
                    return False
    
    async def check_rate_limit(self):
        """Check and enforce rate limiting"""
        current_time = time.time()
        
        if current_time > self.request_reset_time:
            self.request_count = 0
            self.request_reset_time = current_time + 900
        
        if self.request_count >= self.config.rate_limit_requests_per_15min:
            sleep_time = self.request_reset_time - current_time
            logging.warning(f"Rate limit reached, sleeping for {sleep_time:.0f} seconds")
            await asyncio.sleep(sleep_time)
            self.request_count = 0
            self.request_reset_time = time.time() + 900
    
    async def make_api_request(self, endpoint: str, athlete_name: str, params: Dict = None) -> Optional[Dict]:
        """Make authenticated API request with rate limiting"""
        await self.check_rate_limit()
        
        athlete_config = self.config.athletes[athlete_name]
        
        # Check if token needs refresh (refresh 1 hour before expiry)
        if time.time() > (athlete_config['expires_at'] - 3600):
            logging.info(f"Token for {athlete_name} expires soon, refreshing...")
            if not await self.refresh_token(athlete_name):
                logging.error(f"Failed to refresh token for {athlete_name}")
                return None
        
        headers = {
            'Authorization': f"Bearer {athlete_config['access_token']}"
        }
        
        url = f"{self.base_url}{endpoint}"
        
        # Create timeout (30 seconds for API requests)
        timeout = aiohttp.ClientTimeout(total=30)
        
        async with aiohttp.ClientSession(timeout=timeout) as session:
            try:
                async with session.get(url, headers=headers, params=params) as response:
                    self.request_count += 1
                    
                    if response.status == 200:
                        return await response.json()
                    elif response.status == 401:  # Unauthorized - token probably expired
                        logging.warning(f"Unauthorized response for {athlete_name}, attempting token refresh")
                        if await self.refresh_token(athlete_name):
                            # Retry with new token
                            headers['Authorization'] = f"Bearer {athlete_config['access_token']}"
                            async with session.get(url, headers=headers, params=params) as retry_response:
                                if retry_response.status == 200:
                                    return await retry_response.json()
                                else:
                                    logging.error(f"API request failed after token refresh: {retry_response.status}")
                                    return None
                        else:
                            logging.error(f"Token refresh failed for {athlete_name}")
                            return None
                    elif response.status == 429:  # Rate limited
                        logging.warning("Rate limited by Strava, backing off")
                        await asyncio.sleep(60)
                        return await self.make_api_request(endpoint, athlete_name, params)
                    else:
                        logging.error(f"API request failed: {response.status} - {await response.text()}")
                        return None
            except asyncio.TimeoutError:
                logging.error(f"Timeout on API request to {endpoint}")
                return None
            except Exception as e:
                logging.error(f"Exception during API request to {endpoint}: {e}")
                return None
    
    async def get_athlete_info(self, athlete_name: str) -> Optional[Dict]:
        """Get authenticated athlete's information including ID"""
        return await self.make_api_request("/athlete", athlete_name)
    
    async def get_activities(self, athlete_name: str, after: Optional[datetime] = None, 
                           per_page: int = 200) -> List[Dict]:
        """Get activities for athlete, optionally after a specific date"""
        activities = []
        page = 1
        
        while True:
            params = {
                'per_page': per_page,
                'page': page
            }
            
            if after:
                params['after'] = int(after.timestamp())
            
            # In test mode, limit total activities
            if self.config.test_mode and len(activities) >= self.config.max_test_activities:
                break
            
            # Add progress logging for large fetches
            if page > 1:
                logging.info(f"Fetching page {page} for {athlete_name} ({len(activities)} activities so far)")
            
            try:
                batch = await self.make_api_request("/athlete/activities", athlete_name, params)
            except asyncio.TimeoutError:
                logging.error(f"Timeout fetching page {page} for {athlete_name}")
                break
            except Exception as e:
                logging.error(f"Error fetching page {page} for {athlete_name}: {e}")
                break
            
            if not batch or len(batch) == 0:
                logging.info(f"No more activities returned for {athlete_name} (page {page})")
                break
            
            activities.extend(batch)
            logging.info(f"Page {page}: Got {len(batch)} activities for {athlete_name} (total: {len(activities)})")
            
            if len(batch) < per_page:  # Last page
                logging.info(f"Reached last page for {athlete_name} (got {len(batch)} < {per_page})")
                break
                
            page += 1
            
            # In test mode, break after first batch
            if self.config.test_mode:
                break
            
            # Add a small delay between pages to be nice to the API
            await asyncio.sleep(0.5)
        
        logging.info(f"Total activities fetched for {athlete_name}: {len(activities)}")
        return activities[:self.config.max_test_activities] if self.config.test_mode else activities
    
    async def get_detailed_activity(self, activity_id: int, athlete_name: str) -> Optional[Dict]:
        """Get detailed activity data"""
        return await self.make_api_request(f"/activities/{activity_id}", athlete_name)

class DatabaseManager:
    """Handles MariaDB database operations"""
    
    def __init__(self, config):
        self.config = config
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'database': os.getenv('DB_NAME'),
            'charset': 'utf8mb4',
            'collation': 'utf8mb4_unicode_ci',
            'autocommit': True
        }
        self.init_database()
    
    def get_connection(self):
        """Get database connection"""
        return mysql.connector.connect(**self.db_config)
    
    def init_database(self):
        """Initialize database with required tables - MariaDB version"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Create strava_activities table if it doesn't exist
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS strava_activities (
                    id BIGINT PRIMARY KEY,
                    athlete_id BIGINT NOT NULL,
                    athlete_name VARCHAR(100) NOT NULL,
                    name TEXT,
                    start_date_local DATETIME,
                    start_date DATETIME,
                    utc_offset FLOAT,
                    gear_id VARCHAR(50),
                    gear_name VARCHAR(200),
                    distance FLOAT,
                    elapsed_time INT,
                    moving_time INT,
                    calories FLOAT,
                    average_heartrate FLOAT,
                    max_heartrate FLOAT,
                    average_watts FLOAT,
                    max_watts FLOAT,
                    average_speed FLOAT,
                    max_speed FLOAT,
                    type VARCHAR(50),
                    sport_type VARCHAR(50),
                    total_elevation_gain FLOAT,
                    kudos_count INT,
                    weighted_average_watts FLOAT,
                    average_cadence FLOAT,
                    trainer BOOLEAN,
                    map_polyline LONGTEXT,
                    device_name VARCHAR(200),
                    timezone VARCHAR(100),
                    start_latlng JSON,
                    end_latlng JSON,
                    last_webhook_at DATETIME NULL,
                    last_refresh_at DATETIME NULL,
                    needs_refresh BOOLEAN DEFAULT FALSE,
                    data_loaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY unique_activity (id, athlete_id),
                    INDEX idx_athlete_id (athlete_id),
                    INDEX idx_athlete_date (athlete_name, start_date_local),
                    INDEX idx_needs_refresh (needs_refresh, last_refresh_at),
                    INDEX idx_start_date (start_date_local)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            ''')
            
            # Add athlete_id column if it doesn't exist (for existing databases)
            try:
                cursor.execute('''
                    ALTER TABLE strava_activities 
                    ADD COLUMN athlete_id BIGINT AFTER id
                ''')
                logging.info("Added athlete_id column to existing table")
            except Error as e:
                if "Duplicate column name" not in str(e):
                    logging.warning(f"Could not add athlete_id column: {e}")
            
            # Add index if it doesn't exist
            try:
                cursor.execute('''
                    CREATE INDEX idx_athlete_id ON strava_activities(athlete_id)
                ''')
                logging.info("Added athlete_id index")
            except Error as e:
                if "Duplicate key name" not in str(e):
                    logging.warning(f"Could not add athlete_id index: {e}")
            
            # Create webhook and refresh tables
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS activity_refresh_queue (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    activity_id BIGINT NOT NULL,
                    athlete_name VARCHAR(100) NOT NULL,
                    athlete_id BIGINT,
                    webhook_received_at DATETIME NOT NULL,
                    scheduled_refresh_at DATETIME NOT NULL,
                    completed_at DATETIME NULL,
                    refresh_type VARCHAR(50) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY unique_refresh (activity_id, refresh_type),
                    INDEX idx_scheduled (scheduled_refresh_at, completed_at),
                    INDEX idx_athlete_id (athlete_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS webhook_events (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    activity_id BIGINT NOT NULL,
                    athlete_id BIGINT NOT NULL,
                    event_type VARCHAR(50) NOT NULL,
                    aspect_type VARCHAR(50) NOT NULL,
                    received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    processed_at DATETIME NULL,
                    raw_data JSON,
                    INDEX idx_activity (activity_id),
                    INDEX idx_athlete_id (athlete_id),
                    INDEX idx_received (received_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            ''')
            
            cursor.close()
            conn.close()
            
        except Error as e:
            logging.error(f"Database initialization error: {e}")
            raise
    
    def get_existing_activity_ids(self, athlete_name):
        """Get set of existing activity IDs for an athlete"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT id FROM strava_activities 
                WHERE athlete_name = %s
            ''', (athlete_name,))
            
            existing_ids = {row[0] for row in cursor.fetchall()}
            cursor.close()
            conn.close()
            
            logging.info(f"Found {len(existing_ids)} existing activities for {athlete_name}")
            return existing_ids
            
        except Error as e:
            logging.error(f"Error getting existing activity IDs: {e}")
            return set()
        """Get the date of the most recent activity for an athlete"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT MAX(start_date_local) 
                FROM strava_activities 
                WHERE athlete_name = %s
            ''', (athlete_name,))
            
            result = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            
            if result:
                return result  # MySQL returns datetime object directly
            return None
            
        except Error as e:
            logging.error(f"Error getting latest activity date: {e}")
            return None
    
    def save_activities(self, activities):
        """Save activities to MariaDB, return count of new activities"""
        if not activities:
            return 0
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            new_count = 0
            
            for activity in activities:
                activity_dict = asdict(activity)
                
                # Convert JSON fields
                start_latlng_json = json.dumps(activity_dict['start_latlng']) if activity_dict['start_latlng'] else None
                end_latlng_json = json.dumps(activity_dict['end_latlng']) if activity_dict['end_latlng'] else None
                
                # Prepare insert statement with athlete_id
                insert_sql = '''
                    INSERT INTO strava_activities 
                    (id, athlete_id, athlete_name, name, start_date_local, start_date, utc_offset,
                     gear_id, gear_name, distance, elapsed_time, moving_time, calories,
                     average_heartrate, max_heartrate, average_watts, max_watts,
                     average_speed, max_speed, type, sport_type, total_elevation_gain,
                     kudos_count, weighted_average_watts, average_cadence, trainer,
                     map_polyline, device_name, timezone, start_latlng, end_latlng,
                     last_refresh_at, data_loaded_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                    ON DUPLICATE KEY UPDATE
                        athlete_id = VALUES(athlete_id),
                        name = VALUES(name),
                        gear_name = VALUES(gear_name),
                        calories = VALUES(calories),
                        kudos_count = VALUES(kudos_count),
                        last_refresh_at = NOW(),
                        updated_at = NOW()
                '''
                
                values = (
                    activity_dict['id'], activity_dict['athlete_id'], activity_dict['athlete_name'], activity_dict['name'],
                    activity_dict['start_date_local'], activity_dict['start_date'], activity_dict['utc_offset'],
                    activity_dict['gear_id'], activity_dict['gear_name'], activity_dict['distance'],
                    activity_dict['elapsed_time'], activity_dict['moving_time'], activity_dict['calories'],
                    activity_dict['average_heartrate'], activity_dict['max_heartrate'], activity_dict['average_watts'],
                    activity_dict['max_watts'], activity_dict['average_speed'], activity_dict['max_speed'],
                    activity_dict['type'], activity_dict['sport_type'], activity_dict['total_elevation_gain'],
                    activity_dict['kudos_count'], activity_dict['weighted_average_watts'], activity_dict['average_cadence'],
                    activity_dict['trainer'], activity_dict['map_polyline'], activity_dict['device_name'],
                    activity_dict['timezone'], start_latlng_json, end_latlng_json
                )
                
                cursor.execute(insert_sql, values)
                
                if cursor.rowcount > 0:
                    new_count += 1
            
            cursor.close()
            conn.close()
            
            return new_count
            
        except Error as e:
            logging.error(f"Error saving activities: {e}")
            return 0
    
    def get_activity_summary(self, athlete_name=None):
        """Get summary statistics for activities"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            if athlete_name:
                cursor.execute('''
                    SELECT 
                        COUNT(*) as total_activities,
                        SUM(distance) as total_distance,
                        SUM(moving_time) as total_moving_time,
                        AVG(average_speed) as avg_speed,
                        MAX(start_date_local) as latest_activity
                    FROM strava_activities
                    WHERE athlete_name = %s
                ''', (athlete_name,))
            else:
                cursor.execute('''
                    SELECT 
                        COUNT(*) as total_activities,
                        SUM(distance) as total_distance,
                        SUM(moving_time) as total_moving_time,
                        AVG(average_speed) as avg_speed,
                        MAX(start_date_local) as latest_activity
                    FROM strava_activities
                ''')
            
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            
            return {
                'total_activities': result[0] or 0,
                'total_distance': result[1] or 0,
                'total_moving_time': result[2] or 0,
                'avg_speed': result[3] or 0,
                'latest_activity': result[4].isoformat() if result[4] else None
            }
            
        except Error as e:
            logging.error(f"Error getting activity summary: {e}")
            return {
                'total_activities': 0,
                'total_distance': 0,
                'total_moving_time': 0,
                'avg_speed': 0,
                'latest_activity': None
            }

class CSVExporter:
    """Handles CSV export functionality"""
    
    def __init__(self, csv_path: str):
        self.csv_path = csv_path
    
    def export_activities(self, activities: List[StravaActivity]):
        """Export activities to CSV, avoiding duplicates"""
        if not activities:
            return
        
        csv_path = Path(self.csv_path)
        existing_ids = set()
        
        # Read existing IDs if file exists
        if csv_path.exists():
            try:
                with open(csv_path, 'r', encoding='utf-8') as csvfile:
                    reader = csv.DictReader(csvfile)
                    for row in reader:
                        if 'id' in row and 'athlete_id' in row:
                            existing_ids.add((row['id'], row['athlete_id']))
            except Exception as e:
                logging.warning(f"Could not read existing CSV: {e}")
        
        # Filter out activities that already exist
        new_activities = []
        for activity in activities:
            activity_key = (str(activity.id), str(activity.athlete_id))
            if activity_key not in existing_ids:
                new_activities.append(activity)
        
        if not new_activities:
            logging.info("No new activities to add to CSV")
            return
        
        # Write new activities
        file_exists = csv_path.exists()
        with open(csv_path, 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = [field.name for field in StravaActivity.__dataclass_fields__.values()]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            if not file_exists:
                writer.writeheader()
            
            for activity in new_activities:
                row = asdict(activity)
                # Convert lists to strings for CSV
                if row['start_latlng']:
                    row['start_latlng'] = str(row['start_latlng'])
                if row['end_latlng']:
                    row['end_latlng'] = str(row['end_latlng'])
                writer.writerow(row)
        
        logging.info(f"Added {len(new_activities)} new activities to CSV")

class EmailNotifier:
    """Handles email notifications using Brevo API"""
    
    def __init__(self, config: Config):
        self.config = config
        self.api_key = os.getenv("BREVO_API_KEY")
        self.from_email = os.getenv("BREVO_FROM_EMAIL", "dominic.allkins@gmail.com")
        self.from_name = os.getenv("BREVO_FROM_NAME", "Strava Data Fetcher")
        self.api_url = "https://api.brevo.com/v3/smtp/email"
    
    def send_email(self, subject: str, body: str, attachment_path: Optional[str] = None):
        """Send email notification using Brevo API"""
        if not self.api_key:
            logging.warning("No Brevo API key configured, skipping email")
            return
        
        try:
            import requests
            import base64
            
            headers = {
                "accept": "application/json",
                "content-type": "application/json",
                "api-key": self.api_key
            }
            
            data = {
                "sender": {"name": self.from_name, "email": self.from_email},
                "to": [{"email": self.config.notification_email}],
                "subject": subject,
                "htmlContent": body
            }
            
            # Add attachment if provided
            if attachment_path and Path(attachment_path).exists():
                with open(attachment_path, "rb") as f:
                    attachment_content = base64.b64encode(f.read()).decode()
                
                data["attachment"] = [{
                    "content": attachment_content,
                    "name": Path(attachment_path).name
                }]
            
            response = requests.post(self.api_url, json=data, headers=headers)
            
            if response.status_code == 201:
                logging.info(f"Email sent successfully: {subject}")
            else:
                logging.error(f"Failed to send email: {response.status_code} - {response.text}")
                
        except Exception as e:
            logging.error(f"Error sending email: {e}")
    
    def send_daily_summary(self, summary_data: Dict):
        """Send daily summary email"""
        subject = f"Strava Data Fetch Summary - {datetime.now().strftime('%Y-%m-%d')}"
        
        body = f"""
        <html>
        <body>
            <h2>🚴‍♂️ Strava Data Fetch Summary</h2>
            <p><strong>Date:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            
            <h3>📊 Summary Statistics:</h3>
            <ul>
                <li><strong>New Activities Fetched:</strong> {summary_data.get('new_activities', 0)}</li>
                <li><strong>Total Activities in Database:</strong> {summary_data.get('total_activities', 0)}</li>
                <li><strong>Athletes Processed:</strong> {', '.join(summary_data.get('athletes', []))}</li>
                <li><strong>API Requests Made:</strong> {summary_data.get('api_requests', 0)}</li>
            </ul>
            
            <h3>🏃‍♂️ Per-Athlete Summary:</h3>
            <ul>
        """
        
        for athlete, stats in summary_data.get('athlete_stats', {}).items():
            distance_km = stats.get('total_distance', 0) / 1000
            body += f"""
                <li><strong>{athlete.title()}:</strong> 
                    {stats.get('total_activities', 0)} total activities, 
                    {distance_km:.1f}km total distance
                </li>
            """
        
        body += f"""
            </ul>
            
            <p><strong>📁 CSV Download:</strong> The latest data export is attached to this email.</p>
            
            <p><em>🤖 Strava Data Fetcher - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</em></p>
        </body>
        </html>
        """
        
        self.send_email(subject, body, self.config.csv_path)
    
    def send_error_notification(self, error_message: str):
        """Send error notification email"""
        subject = f"⚠️ Strava Data Fetch Error - {datetime.now().strftime('%Y-%m-%d')}"
        
        body = f"""
        <html>
        <body>
            <h2>⚠️ Strava Data Fetch Error</h2>
            <p><strong>Date:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            
            <h3>Error Details:</h3>
            <pre style="background-color: #f4f4f4; padding: 10px; border-radius: 5px;">{error_message}</pre>
            
            <p>Please check the application logs for more details.</p>
            
            <p><em>🤖 Strava Data Fetcher - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</em></p>
        </body>
        </html>
        """
        
        self.send_email(subject, body)

def parse_strava_activity(activity_data: Dict, athlete_name: str, athlete_id: int) -> StravaActivity:
    """Parse Strava API response into StravaActivity object"""

    # Add debug logging to see what fields we have
    logging.debug(f"Activity {activity_data.get('id')} fields: {list(activity_data.keys())}")
    
    # Check specifically for calories-related fields
    calories_fields = [k for k in activity_data.keys() if 'calor' in k.lower() or 'energy' in k.lower() or 'kilojoule' in k.lower()]
    if calories_fields:
        logging.debug(f"Calories-related fields: {calories_fields}")

    # Handle gear information
    gear_id = None
    gear_name = None
    if activity_data.get('gear'):
        gear_info = activity_data['gear']
        gear_id = gear_info.get('id')
        gear_name = gear_info.get('name')
    elif activity_data.get('gear_id'):
        gear_id = activity_data.get('gear_id')
    
    # Handle map data
    map_polyline = None
    if activity_data.get('map') and activity_data['map'].get('summary_polyline'):
        map_polyline = activity_data['map']['summary_polyline']
    
    # Handle coordinates
    start_latlng = activity_data.get('start_latlng')
    end_latlng = activity_data.get('end_latlng')
    
    # Handle datetime conversion - remove 'Z' suffix for MySQL compatibility
    def clean_datetime(dt_str):
        if dt_str and dt_str.endswith('Z'):
            return dt_str[:-1]  # Remove the 'Z'
        return dt_str
    
    return StravaActivity(
        id=activity_data['id'],
        athlete_id=athlete_id,  # Added athlete_id parameter
        name=activity_data.get('name', ''),
        start_date_local=clean_datetime(activity_data.get('start_date_local', '')),
        start_date=clean_datetime(activity_data.get('start_date', '')),
        utc_offset=activity_data.get('utc_offset', 0),
        gear_id=gear_id,
        gear_name=gear_name,
        distance=activity_data.get('distance', 0),
        elapsed_time=activity_data.get('elapsed_time', 0),
        moving_time=activity_data.get('moving_time', 0),
        calories=activity_data.get('calories'),
        average_heartrate=activity_data.get('average_heartrate'),
        max_heartrate=activity_data.get('max_heartrate'),
        average_watts=activity_data.get('average_watts'),
        max_watts=activity_data.get('max_watts'),
        average_speed=activity_data.get('average_speed', 0),
        max_speed=activity_data.get('max_speed', 0),
        type=activity_data.get('type', ''),
        sport_type=activity_data.get('sport_type', ''),
        total_elevation_gain=activity_data.get('total_elevation_gain', 0),
        kudos_count=activity_data.get('kudos_count', 0),
        weighted_average_watts=activity_data.get('weighted_average_watts'),
        average_cadence=activity_data.get('average_cadence'),
        trainer=activity_data.get('trainer', False),
        map_polyline=map_polyline,
        device_name=activity_data.get('device_name'),
        timezone=activity_data.get('timezone', ''),
        start_latlng=start_latlng,
        end_latlng=end_latlng,
        athlete_name=athlete_name
    )

class StravaDataFetcher:
    """Main application class"""

    def send_progress_email(self, progress_data: Dict):
        """Send hourly progress email during big fetch"""
        subject = f"🚴‍♂️ Strava Fetch Progress - {progress_data['percentage']:.1f}% Complete"
        
        body = f"""
        <html>
        <body>
            <h2>🚴‍♂️ Strava Historical Fetch Progress</h2>
            <p><strong>Time:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            
            <h3>📊 Progress Update:</h3>
            <ul>
                <li><strong>Athlete:</strong> {progress_data['athlete'].title()}</li>
                <li><strong>Progress:</strong> {progress_data['processed']} of {progress_data['total_estimated']} activities</li>
                <li><strong>Completion:</strong> {progress_data['percentage']:.1f}%</li>
                <li><strong>Latest Activity:</strong> {progress_data['latest_activity']}</li>
                <li><strong>Activity Date:</strong> {progress_data['latest_date']}</li>
            </ul>
            
            <p>📈 <strong>Estimated time remaining:</strong> {int((100 - progress_data['percentage']) / progress_data['percentage'] * 60) if progress_data['percentage'] > 0 else 'Calculating...'} minutes</p>
            
            <p><em>🤖 Fetching your complete cycling history since 2013...</em></p>
        </body>
        </html>
        """
        
        self.notifier.send_email(subject, body)

    def __init__(self, config: Config):
        self.config = config
        self.api = StravaAPI(config)
        self.db = DatabaseManager(config)
        self.csv_exporter = CSVExporter(config.csv_path)
        self.notifier = EmailNotifier(config)
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('strava_fetcher.log'),
                logging.StreamHandler(sys.stdout)
            ]
        )
    
    async def setup_oauth(self, athlete_name: str):
        """Setup OAuth for a new athlete"""
        auth_url = self.api.get_authorization_url(athlete_name)
        print(f"\nPlease visit this URL to authorize access for {athlete_name}:")
        print(auth_url)
        
        webbrowser.open(auth_url)
        
        code = input(f"\nEnter the authorization code for {athlete_name}: ")
        
        if await self.api.exchange_code_for_tokens(code, athlete_name):
            print(f"✅ Successfully authorized {athlete_name}")
            # Here you would save the tokens to your environment/config
            athlete_config = self.config.athletes[athlete_name]
            print(f"Save these to your .env file:")
            print(f"{athlete_name.upper()}_ACCESS_TOKEN={athlete_config['access_token']}")
            print(f"{athlete_name.upper()}_REFRESH_TOKEN={athlete_config['refresh_token']}")
            print(f"{athlete_name.upper()}_TOKEN_EXPIRES={athlete_config['expires_at']}")
        else:
            print(f"❌ Failed to authorize {athlete_name}")
    
    async def fetch_athlete_data(self, athlete_name: str, full_fetch: bool = False) -> Dict:
        """Fetch data for a single athlete"""
        logging.info(f"Fetching data for {athlete_name} (full_fetch={full_fetch})")
        
        # Get athlete info to get their ID
        athlete_info = await self.api.get_athlete_info(athlete_name)
        if not athlete_info:
            logging.error(f"Could not get athlete info for {athlete_name}")
            return {
                'athlete': athlete_name,
                'new_activities': 0,
                'total_activities': 0,
                'api_requests': self.api.request_count
            }
        
        athlete_id = athlete_info['id']
        logging.info(f"Athlete {athlete_name} has ID: {athlete_id}")
        
        # Get the date of the most recent activity in our database
        if full_fetch:
            after_date = None
            logging.info(f"Fetching ALL activities for {athlete_name} (full historical fetch)")
        else:
            latest_date = self.db.get_latest_activity_date(athlete_name)
            
            if latest_date:
                # Add a small buffer to avoid missing activities
                after_date = latest_date - timedelta(days=1)
                logging.info(f"Fetching activities for {athlete_name} after {after_date}")
            else:
                after_date = None
                logging.info(f"Fetching all activities for {athlete_name} (first run)")
        
        # Get activities from Strava
        activities_data = await self.api.get_activities(athlete_name, after_date)
        
        if not activities_data:
            logging.warning(f"No activities returned for {athlete_name}")
            return {
                'athlete': athlete_name,
                'new_activities': 0,
                'total_activities': 0,
                'api_requests': self.api.request_count
            }
        
        # Get existing activity IDs to avoid duplicates in full fetch
        existing_ids = set()
        if full_fetch:
            existing_ids = self.db.get_existing_activity_ids(athlete_name)
            logging.info(f"Will skip {len(existing_ids)} existing activities for {athlete_name}")
        
        # Parse activities - filter for cycling only
        activities = []
        skipped_existing = 0
        skipped_non_cycling = 0
        last_progress_email = time.time()
        progress_email_interval = 3600  # 1 hour in seconds

        for i, activity_data in enumerate(activities_data):
            try:
                activity_id = activity_data['id']
                
                # Skip if activity already exists (for full fetch)
                if full_fetch and activity_id in existing_ids:
                    skipped_existing += 1
                    continue
                
                # Filter for cycling activities only
                activity_type = activity_data.get('type', '').lower()
                sport_type = activity_data.get('sport_type', '').lower()
                
                # Only process rides and virtual rides
                if activity_type not in ['ride', 'virtualride'] and sport_type not in ['ride', 'virtualride']:
                    logging.debug(f"Skipping non-cycling activity {activity_id}: type={activity_type}, sport_type={sport_type}")
                    skipped_non_cycling += 1
                    continue
                
                # Get detailed activity data for calories and other detailed fields
                detailed_data = await self.api.get_detailed_activity(activity_id, athlete_name)
                if detailed_data:
                    activity = parse_strava_activity(detailed_data, athlete_name, athlete_id)
                    activities.append(activity)
                    logging.info(f"Added cycling activity: {activity.name} ({activity.type})")
                else:
                    # Fallback to basic data if detailed fetch fails
                    logging.warning(f"Could not get detailed data for activity {activity_id}, using basic data")
                    activity = parse_strava_activity(activity_data, athlete_name, athlete_id)
                    activities.append(activity)
                
                # Send progress email every hour
                current_time = time.time()
                if current_time - last_progress_email >= progress_email_interval:
                    progress_data = {
                        'athlete': athlete_name,
                        'processed': i + 1,
                        'total_estimated': len(activities_data),
                        'percentage': ((i + 1) / len(activities_data)) * 100,
                        'latest_activity': activity.name if 'activity' in locals() else 'Unknown',
                        'latest_date': activity.start_date_local if 'activity' in locals() else 'Unknown'
                    }
                    
                    # Send progress email
                    try:
                        self.send_progress_email(progress_data)
                        last_progress_email = current_time
                        logging.info(f"Progress email sent: {i+1}/{len(activities_data)} activities processed")
                    except Exception as e:
                        logging.error(f"Failed to send progress email: {e}")
                        
            except Exception as e:
                logging.error(f"Error parsing activity {activity_data.get('id', 'unknown')}: {e}")
        
        # Log summary of what was processed
        logging.info(f"Processing summary for {athlete_name}:")
        logging.info(f"  - Total activities from Strava: {len(activities_data)}")
        logging.info(f"  - Skipped (already in DB): {skipped_existing}")
        logging.info(f"  - Skipped (non-cycling): {skipped_non_cycling}")
        logging.info(f"  - New cycling activities to save: {len(activities)}")
        
        # Save to database
        new_count = self.db.save_activities(activities)
        
        # Export to CSV
        if activities:
            self.csv_exporter.export_activities(activities)
        
        # Get summary stats
        summary = self.db.get_activity_summary(athlete_name)
        
        logging.info(f"Processed {len(activities)} activities for {athlete_name}, {new_count} new")
        
        return {
            'athlete': athlete_name,
            'new_activities': new_count,
            'total_activities': summary['total_activities'],
            'total_distance': summary['total_distance'],
            'api_requests': self.api.request_count
        }
    
    async def run_data_fetch(self, full_fetch: bool = False):
        """Main data fetching routine"""
        try:
            fetch_type = "full historical" if full_fetch else "incremental"
            logging.info(f"Starting Strava data fetch ({fetch_type})")
            
            # Check if we have any athletes configured
            if not self.config.athletes:
                logging.warning("No athletes configured with valid tokens")
                print("❌ No athletes configured with valid tokens.")
                print("Available athletes to configure:")
                print("- Dominic: Set DOMINIC_ACCESS_TOKEN and DOMINIC_REFRESH_TOKEN")
                print("- Clare: Set CLARE_ACCESS_TOKEN and CLARE_REFRESH_TOKEN")
                return
            
            logging.info(f"Processing {len(self.config.athletes)} athletes: {list(self.config.athletes.keys())}")
            
            summary_data = {
                'new_activities': 0,
                'total_activities': 0,
                'athletes': [],
                'api_requests': 0,
                'athlete_stats': {}
            }
            
            # Process each athlete
            for athlete_name in self.config.athletes.keys():
                if not self.config.athletes[athlete_name].get('access_token'):
                    logging.warning(f"No access token for {athlete_name}, skipping")
                    continue
                
                try:
                    result = await self.fetch_athlete_data(athlete_name, full_fetch)
                    
                    summary_data['new_activities'] += result['new_activities']
                    summary_data['athletes'].append(athlete_name)
                    summary_data['athlete_stats'][athlete_name] = {
                        'total_activities': result['total_activities'],
                        'total_distance': result['total_distance'],
                        'new_activities': result['new_activities']
                    }
                    
                except Exception as e:
                    logging.error(f"Error processing {athlete_name}: {e}")
                    self.notifier.send_error_notification(f"Error processing {athlete_name}: {str(e)}")
            
            # Get overall summary
            overall_summary = self.db.get_activity_summary()
            summary_data['total_activities'] = overall_summary['total_activities']
            summary_data['api_requests'] = self.api.request_count
            
            # Send summary email
            self.notifier.send_daily_summary(summary_data)
            
            logging.info("Data fetch completed successfully")
            
        except Exception as e:
            error_msg = f"Critical error in data fetch: {str(e)}"
            logging.error(error_msg)
            self.notifier.send_error_notification(error_msg)
            raise

async def main():
    """Main entry point"""
    config = Config.from_env()
    
    # Validate configuration
    if not config.strava_client_id or not config.strava_client_secret:
        print("❌ Missing Strava client ID or secret. Please check your .env file.")
        return
    
    fetcher = StravaDataFetcher(config)
    
    # Check command line arguments
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "setup":
            # Setup OAuth for athletes
            if len(sys.argv) > 2:
                athlete_name = sys.argv[2]
                if athlete_name in ["dominic", "clare"]:  # Allow setup for new athletes
                    await fetcher.setup_oauth(athlete_name)
                else:
                    print(f"❌ Unknown athlete: {athlete_name}")
                    print("Available athletes: dominic, clare")
            else:
                print("Usage: python strava_main.py setup <athlete_name>")
                print("Available athletes: dominic, clare")
        
        elif command == "fetch":
            # Run data fetch
            await fetcher.run_data_fetch()
        
        elif command == "fetch-all":
            # Run full historical fetch
            print("🏃‍♂️ Running full historical fetch (all activities)")
            await fetcher.run_data_fetch(full_fetch=True)
        
        elif command == "test":
            # Run in test mode
            original_test_mode = config.test_mode
            config.test_mode = True
            print("🧪 Running in test mode (max 5 activities per athlete)")
            await fetcher.run_data_fetch()
            config.test_mode = original_test_mode
        
        elif command == "status":
            # Show status
            print("\n📊 Strava Data Fetcher Status")
            print("=" * 40)
            
            for athlete_name in ["dominic", "clare"]:
                if athlete_name in config.athletes and config.athletes[athlete_name].get('access_token'):
                    summary = fetcher.db.get_activity_summary(athlete_name)
                    print(f"\n{athlete_name.title()}:")
                    print(f"  Total activities: {summary['total_activities']}")
                    print(f"  Total distance: {summary['total_distance']:.2f}m")
                    print(f"  Latest activity: {summary['latest_activity'] or 'None'}")
                else:
                    print(f"\n{athlete_name.title()}: ❌ Not authenticated")
            
            overall = fetcher.db.get_activity_summary()
            print(f"\nOverall:")
            print(f"  Total activities: {overall['total_activities']}")
            print(f"  CSV export: {config.csv_path}")
            print(f"  Test mode: {'ON' if config.test_mode else 'OFF'}")
        
        else:
            print(f"❌ Unknown command: {command}")
            print("Available commands: setup, fetch, fetch-all, test, status")
    
    else:
        # Default: run data fetch
        await fetcher.run_data_fetch()

if __name__ == "__main__":
    asyncio.run(main())