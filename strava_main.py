#strava_main.py
#!/usr/bin/env python3
"""
strava_main
Strava Data Fetcher
Main application for fetching Strava ride data for multiple athletes

Features:
- Multi-athlete support with OAuth token management
- Incremental and full historical data fetching
- MariaDB database storage with proper indexing
- CSV export functionality
- Email notifications with progress updates
- Rate limiting and error handling
- Webhook integration support

Author: Enhanced by Claude
Version: 2.0 - Production Ready
"""

import os
import sys
import logging
import asyncio
import aiohttp
import csv
import json
import webbrowser
import mysql.connector
import requests
import base64
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

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# ===== CONFIGURATION CLASSES =====

@dataclass
class Config:
    """Configuration class for Strava Data Fetcher with environment variable loading"""
    
    # Required fields
    strava_client_id: str
    strava_client_secret: str
    smtp_password: str
    
    # Optional fields with defaults
    db_path: str = ""  # Legacy field, kept for compatibility
    csv_path: str = "strava_rides.csv"
    smtp_email: str = "dominic.allkins@gmail.com" 
    notification_email: str = "dominic@allkins.com"
    test_mode: bool = False
    max_test_activities: int = 5
    rate_limit_requests_per_15min: int = 100
    rate_limit_daily: int = 1000
    athletes: Dict[str, Dict] = None
    
    def __post_init__(self):
        """Initialize athletes dict if not provided"""
        if self.athletes is None:
            self.athletes = {}
    
    @classmethod
    def from_env(cls):
        """Create configuration from environment variables"""
        # Build athletes dict dynamically, only including those with tokens
        athletes = {}
        
        # Dominic's configuration
        dominic_access = os.getenv("DOMINIC_ACCESS_TOKEN")
        dominic_refresh = os.getenv("DOMINIC_REFRESH_TOKEN")
        if dominic_access and dominic_refresh:
            athletes["dominic"] = {
                "access_token": dominic_access,
                "refresh_token": dominic_refresh,
                "expires_at": int(os.getenv("DOMINIC_TOKEN_EXPIRES", "0"))
            }
        
        # Clare's configuration
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
            max_test_activities=int(os.getenv("STRAVA_MAX_TEST_ACTIVITIES", "5")),
            athletes=athletes
        )
    
    def validate_config(self):
        """Validate essential configuration with detailed error reporting"""
        errors = []
        warnings = []
        
        # Critical requirements
        if not self.strava_client_id:
            errors.append("STRAVA_CLIENT_ID not set")
        
        if not self.strava_client_secret:
            errors.append("STRAVA_CLIENT_SECRET not set")
        
        # Email configuration (optional but recommended)
        if not self.smtp_password:
            warnings.append("GMAIL_APP_PASSWORD not set - email notifications disabled")
        
        # Database configuration check
        required_db_vars = ['DB_HOST', 'DB_USER', 'DB_PASSWORD', 'DB_NAME']
        missing_db_vars = [var for var in required_db_vars if not os.getenv(var)]
        if missing_db_vars:
            errors.append(f"Missing database configuration: {', '.join(missing_db_vars)}")
        
        # Athlete token validation
        if not self.athletes:
            warnings.append("No athlete tokens configured - use 'setup' command to authenticate")
        
        # Report results
        if errors:
            error_msg = f"Configuration errors: {', '.join(errors)}"
            if warnings:
                error_msg += f"\nWarnings: {', '.join(warnings)}"
            raise ValueError(error_msg)
        
        if warnings:
            print("⚠️  Configuration warnings:")
            for warning in warnings:
                print(f"   • {warning}")
        
        return True

@dataclass
class StravaActivity:
    """Data class for Strava activity with all required fields for database storage"""
    
    # Core identification
    id: int
    athlete_id: int
    athlete_name: str
    
    # Basic activity info
    name: str
    type: str
    sport_type: str
    
    # Date and time
    start_date_local: str
    start_date: str
    utc_offset: float
    timezone: str
    
    # Equipment
    gear_id: Optional[str]
    gear_name: Optional[str]
    device_name: Optional[str]
    
    # Distance and time
    distance: float
    elapsed_time: int
    moving_time: int
    
    # Performance metrics
    calories: Optional[float]
    average_heartrate: Optional[float]
    max_heartrate: Optional[float]
    average_watts: Optional[float]
    max_watts: Optional[float]
    weighted_average_watts: Optional[float]
    average_cadence: Optional[float]
    average_speed: float
    max_speed: float
    total_elevation_gain: float
    
    # Social and training
    kudos_count: int
    trainer: bool
    
    # Location data
    map_polyline: Optional[str]
    start_latlng: Optional[str]
    end_latlng: Optional[str]
    # ===== STRAVA API MANAGEMENT =====

class StravaAPI:
    """Handles Strava API interactions with rate limiting, token management, and error handling"""
    
    def __init__(self, config: Config):
        self.config = config
        self.base_url = "https://www.strava.com/api/v3"
        self.oauth_url = "https://www.strava.com/oauth"
        self.request_count = 0
        self.request_reset_time = time.time() + 900  # 15 minutes
        
        # Setup logging
        self.logger = logging.getLogger(f"{__name__}.StravaAPI")
        
    async def refresh_token(self, athlete_name: str) -> bool:
        """Refresh access token for athlete with comprehensive error handling"""
        if athlete_name not in self.config.athletes:
            self.logger.error(f"No configuration found for athlete: {athlete_name}")
            return False
            
        athlete_config = self.config.athletes[athlete_name]
        refresh_token = athlete_config.get('refresh_token')
        
        if not refresh_token:
            self.logger.error(f"No refresh token available for {athlete_name}")
            return False
        
        data = {
            'client_id': self.config.strava_client_id,
            'client_secret': self.config.strava_client_secret,
            'refresh_token': refresh_token,
            'grant_type': 'refresh_token'
        }
        
        try:
            timeout = aiohttp.ClientTimeout(total=30)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(f"{self.oauth_url}/token", data=data) as response:
                    if response.status == 200:
                        token_data = await response.json()
                        
                        # Update athlete configuration
                        athlete_config.update({
                            'access_token': token_data['access_token'],
                            'refresh_token': token_data['refresh_token'],
                            'expires_at': token_data['expires_at']
                        })
                        
                        self.logger.info(f"Successfully refreshed token for {athlete_name}")
                        return True
                    else:
                        error_text = await response.text()
                        self.logger.error(f"Token refresh failed for {athlete_name}: {response.status} - {error_text}")
                        return False
                        
        except aiohttp.ClientError as e:
            self.logger.error(f"Network error refreshing token for {athlete_name}: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error refreshing token for {athlete_name}: {e}")
            return False
    
    def get_authorization_url(self, athlete_name: str) -> str:
        """Generate authorization URL for OAuth flow"""
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
        
        try:
            timeout = aiohttp.ClientTimeout(total=30)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(f"{self.oauth_url}/token", data=data) as response:
                    if response.status == 200:
                        token_data = await response.json()
                        self.config.athletes[athlete_name] = {
                            'access_token': token_data['access_token'],
                            'refresh_token': token_data['refresh_token'],
                            'expires_at': token_data['expires_at']
                        }
                        self.logger.info(f"Successfully obtained tokens for {athlete_name}")
                        return True
                    else:
                        error_text = await response.text()
                        self.logger.error(f"Token exchange failed for {athlete_name}: {response.status} - {error_text}")
                        return False
                        
        except Exception as e:
            self.logger.error(f"Error exchanging code for {athlete_name}: {e}")
            return False
    
    async def check_rate_limit(self):
        """Enhanced rate limiting with better handling"""
        current_time = time.time()
        
        # Reset counter if time window has passed
        if current_time > self.request_reset_time:
            self.request_count = 0
            self.request_reset_time = current_time + 900  # Next 15-minute window
            self.logger.debug("Rate limit window reset")
        
        # If we're approaching the limit, slow down
        if self.request_count >= (self.config.rate_limit_requests_per_15min * 0.8):
            self.logger.warning("Approaching rate limit, adding delay")
            await asyncio.sleep(2)
        
        # If we've hit the limit, wait until reset
        if self.request_count >= self.config.rate_limit_requests_per_15min:
            sleep_time = max(1, self.request_reset_time - current_time)
            self.logger.warning(f"Rate limit reached, sleeping for {sleep_time:.0f} seconds")
            await asyncio.sleep(sleep_time)
            self.request_count = 0
            self.request_reset_time = time.time() + 900
    
    async def make_api_request(self, endpoint: str, athlete_name: str, params: Dict = None) -> Optional[Dict]:
        """Make authenticated API request with comprehensive error handling and rate limiting"""
        if athlete_name not in self.config.athletes:
            self.logger.error(f"No athlete configuration for: {athlete_name}")
            return None
            
        await self.check_rate_limit()
        
        athlete_config = self.config.athletes[athlete_name]
        access_token = athlete_config.get('access_token')
        
        if not access_token:
            self.logger.error(f"No access token for {athlete_name}")
            return None
        
        # Check if token needs refresh (refresh 1 hour before expiry)
        expires_at = athlete_config.get('expires_at', 0)
        if time.time() > (expires_at - 3600):
            self.logger.info(f"Token for {athlete_name} expires soon, refreshing...")
            if not await self.refresh_token(athlete_name):
                self.logger.error(f"Failed to refresh token for {athlete_name}")
                return None
            access_token = athlete_config['access_token']  # Get updated token
        
        headers = {
            'Authorization': f"Bearer {access_token}",
            'User-Agent': 'StravaDataFetcher/2.0'
        }
        
        url = f"{self.base_url}{endpoint}"
        timeout = aiohttp.ClientTimeout(total=30)
        
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url, headers=headers, params=params) as response:
                    self.request_count += 1
                    
                    if response.status == 200:
                        return await response.json()
                    elif response.status == 401:  # Unauthorized
                        self.logger.warning(f"Unauthorized response for {athlete_name}, attempting token refresh")
                        if await self.refresh_token(athlete_name):
                            # Retry with new token
                            headers['Authorization'] = f"Bearer {athlete_config['access_token']}"
                            async with session.get(url, headers=headers, params=params) as retry_response:
                                if retry_response.status == 200:
                                    return await retry_response.json()
                                else:
                                    self.logger.error(f"API request failed after token refresh: {retry_response.status}")
                                    return None
                        else:
                            self.logger.error(f"Token refresh failed for {athlete_name}")
                            return None
                    elif response.status == 429:  # Rate limited
                        self.logger.warning("Rate limited by Strava, backing off")
                        await asyncio.sleep(60)
                        return await self.make_api_request(endpoint, athlete_name, params)
                    elif response.status == 404:
                        self.logger.warning(f"Resource not found: {endpoint}")
                        return None
                    else:
                        error_text = await response.text()
                        self.logger.error(f"API request failed: {response.status} - {error_text}")
                        return None
                        
        except asyncio.TimeoutError:
            self.logger.error(f"Timeout on API request to {endpoint}")
            return None
        except aiohttp.ClientError as e:
            self.logger.error(f"Network error on API request to {endpoint}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error on API request to {endpoint}: {e}")
            return None
    
    async def get_athlete_info(self, athlete_name: str) -> Optional[Dict]:
        """Get authenticated athlete's information including ID"""
        return await self.make_api_request("/athlete", athlete_name)
    
    async def get_activities(self, athlete_name: str, after: Optional[datetime] = None, 
                           per_page: int = 200) -> List[Dict]:
        """Get activities for athlete with pagination and progress tracking"""
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
                self.logger.info(f"Test mode: stopping at {len(activities)} activities")
                break
            
            # Progress logging for large fetches
            if page > 1:
                self.logger.info(f"Fetching page {page} for {athlete_name} ({len(activities)} activities so far)")
            
            try:
                batch = await self.make_api_request("/athlete/activities", athlete_name, params)
                
                if not batch or len(batch) == 0:
                    self.logger.info(f"No more activities returned for {athlete_name} (page {page})")
                    break
                
                activities.extend(batch)
                self.logger.info(f"Page {page}: Got {len(batch)} activities for {athlete_name} (total: {len(activities)})")
                
                # Check if this is the last page
                if len(batch) < per_page:
                    self.logger.info(f"Reached last page for {athlete_name} (got {len(batch)} < {per_page})")
                    break
                    
                page += 1
                
                # In test mode, break after first batch
                if self.config.test_mode:
                    break
                
                # Small delay between pages to be respectful to the API
                await asyncio.sleep(0.5)
                
            except Exception as e:
                self.logger.error(f"Error fetching page {page} for {athlete_name}: {e}")
                break
        
        final_count = len(activities)
        if self.config.test_mode and final_count > self.config.max_test_activities:
            activities = activities[:self.config.max_test_activities]
            final_count = len(activities)
        
        self.logger.info(f"Total activities fetched for {athlete_name}: {final_count}")
        return activities
    
    async def get_detailed_activity(self, activity_id: int, athlete_name: str) -> Optional[Dict]:
        """Get detailed activity data including calories and other metrics"""
        return await self.make_api_request(f"/activities/{activity_id}", athlete_name)
        # ===== DATABASE MANAGEMENT =====

class DatabaseManager:
    """Handles MariaDB database operations with proper error handling and connection management"""
    
    def __init__(self, config: Config):
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
        
        # Setup logging
        self.logger = logging.getLogger(f"{__name__}.DatabaseManager")
        
        # Initialize database
        self.init_database()
    
    def get_connection(self):
        """Get database connection with error handling"""
        try:
            conn = mysql.connector.connect(**self.db_config)
            return conn
        except Error as e:
            self.logger.error(f"Database connection failed: {e}")
            raise
    
    def test_connection(self):
        """Test database connectivity"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT 1')
            cursor.close()
            conn.close()
            return True
        except Exception as e:
            self.logger.error(f"Database connection test failed: {e}")
            return False
    
    def init_database(self):
        """Initialize database with required tables and indexes"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Create main activities table
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
                    INDEX idx_start_date (start_date_local),
                    INDEX idx_sport_type (sport_type)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            ''')
            
            # Create webhook events table
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
            
            # Create refresh queue table
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
            
            # Handle migration for existing databases
            self._handle_database_migration(cursor)
            
            cursor.close()
            conn.close()
            
            self.logger.info("Database initialization completed successfully")
            
        except Error as e:
            self.logger.error(f"Database initialization error: {e}")
            raise
    
    def _handle_database_migration(self, cursor):
        """Handle migration tasks for existing databases"""
        try:
            # Add athlete_id column if it doesn't exist
            cursor.execute('''
                ALTER TABLE strava_activities 
                ADD COLUMN athlete_id BIGINT AFTER id
            ''')
            self.logger.info("Added athlete_id column to existing table")
        except Error as e:
            if "Duplicate column name" not in str(e):
                self.logger.warning(f"Could not add athlete_id column: {e}")
        
        try:
            # Add sport_type index if it doesn't exist
            cursor.execute('''
                CREATE INDEX idx_sport_type ON strava_activities(sport_type)
            ''')
            self.logger.info("Added sport_type index")
        except Error as e:
            if "Duplicate key name" not in str(e):
                self.logger.warning(f"Could not add sport_type index: {e}")
    
    def get_existing_activity_ids(self, athlete_name: str) -> set:
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
            
            self.logger.info(f"Found {len(existing_ids)} existing activities for {athlete_name}")
            return existing_ids
            
        except Error as e:
            self.logger.error(f"Error getting existing activity IDs: {e}")
            return set()
    
    def get_latest_activity_date(self, athlete_name: str) -> Optional[datetime]:
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
                self.logger.debug(f"Latest activity date for {athlete_name}: {result}")
                return result  # MySQL returns datetime object directly
            return None
            
        except Error as e:
            self.logger.error(f"Error getting latest activity date: {e}")
            return None
    
    def save_activities(self, activities: List[StravaActivity]) -> int:
        """Save activities to MariaDB, return count of new activities"""
        if not activities:
            return 0
        
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            new_count = 0
            updated_count = 0
            
            for activity in activities:
                try:  # Add individual activity error handling
                    activity_dict = asdict(activity)
                    
                    # Convert JSON fields for MySQL storage
                    start_latlng_json = json.dumps(activity_dict['start_latlng']) if activity_dict['start_latlng'] else None
                    end_latlng_json = json.dumps(activity_dict['end_latlng']) if activity_dict['end_latlng'] else None
                    
                    # Handle datetime formatting for MySQL
                    start_date_local = activity_dict['start_date_local']
                    start_date = activity_dict['start_date']
                    
                    if start_date_local and start_date_local.endswith('Z'):
                        start_date_local = start_date_local[:-1].replace('T', ' ')
                    if start_date and start_date.endswith('Z'):
                        start_date = start_date[:-1].replace('T', ' ')
                    
                    # Prepare comprehensive insert statement
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
                            distance = VALUES(distance),
                            moving_time = VALUES(moving_time),
                            total_elevation_gain = VALUES(total_elevation_gain),
                            last_refresh_at = NOW(),
                            updated_at = NOW()
                    '''
                    
                    values = (
                        activity_dict['id'], activity_dict['athlete_id'], activity_dict['athlete_name'], 
                        activity_dict['name'], start_date_local, start_date, activity_dict['utc_offset'],
                        activity_dict['gear_id'], activity_dict['gear_name'], activity_dict['distance'],
                        activity_dict['elapsed_time'], activity_dict['moving_time'], activity_dict['calories'],
                        activity_dict['average_heartrate'], activity_dict['max_heartrate'], 
                        activity_dict['average_watts'], activity_dict['max_watts'], 
                        activity_dict['average_speed'], activity_dict['max_speed'],
                        activity_dict['type'], activity_dict['sport_type'], 
                        activity_dict['total_elevation_gain'], activity_dict['kudos_count'], 
                        activity_dict['weighted_average_watts'], activity_dict['average_cadence'], 
                        activity_dict['trainer'], activity_dict['map_polyline'], 
                        activity_dict['device_name'], activity_dict['timezone'], 
                        start_latlng_json, end_latlng_json
                    )
                    
                    cursor.execute(insert_sql, values)
                    
                    # Check if this was an insert or update
                    if cursor.rowcount > 0:
                        if cursor.lastrowid:  # New insert
                            new_count += 1
                        else:  # Update
                            updated_count += 1
                            
                except Error as e:
                    self.logger.error(f"Error saving individual activity {activity.id}: {e}")
                    continue  # Skip this activity and continue with others
            
            self.logger.info(f"Saved activities: {new_count} new, {updated_count} updated")
            return new_count
            
        except Error as e:
            self.logger.error(f"Error saving activities: {e}")
            return 0
        finally:
            # Ensure connections are properly closed
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    def get_activity_summary(self, athlete_name: Optional[str] = None) -> Dict[str, Any]:
        """Get comprehensive summary statistics for activities"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            base_query = '''
                SELECT 
                    COUNT(*) as total_activities,
                    COALESCE(SUM(distance), 0) as total_distance,
                    COALESCE(SUM(moving_time), 0) as total_moving_time,
                    COALESCE(AVG(average_speed), 0) as avg_speed,
                    MAX(start_date_local) as latest_activity,
                    COALESCE(SUM(total_elevation_gain), 0) as total_elevation,
                    COALESCE(SUM(calories), 0) as total_calories,
                    COUNT(DISTINCT athlete_name) as unique_athletes
                FROM strava_activities
            '''
            
            if athlete_name:
                base_query += ' WHERE athlete_name = %s'
                cursor.execute(base_query, (athlete_name,))
            else:
                cursor.execute(base_query)
            
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            
            return {
                'total_activities': result[0] or 0,
                'total_distance': result[1] or 0,
                'total_moving_time': result[2] or 0,
                'avg_speed': result[3] or 0,
                'latest_activity': result[4].isoformat() if result[4] else None,
                'total_elevation': result[5] or 0,
                'total_calories': result[6] or 0,
                'unique_athletes': result[7] or 0
            }
            
        except Error as e:
            self.logger.error(f"Error getting activity summary: {e}")
            return {
                'total_activities': 0,
                'total_distance': 0,
                'total_moving_time': 0,
                'avg_speed': 0,
                'latest_activity': None,
                'total_elevation': 0,
                'total_calories': 0,
                'unique_athletes': 0
            }

# ===== UTILITY CLASSES =====

class CSVExporter:
    """Handles CSV export functionality with duplicate detection"""
    
    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        self.logger = logging.getLogger(f"{__name__}.CSVExporter")
    
    def export_activities(self, activities: List[StravaActivity]) -> bool:
        """Export activities to CSV, avoiding duplicates"""
        if not activities:
            self.logger.info("No activities to export")
            return True
        
        try:
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
                    self.logger.warning(f"Could not read existing CSV: {e}")
            
            # Filter out activities that already exist
            new_activities = []
            for activity in activities:
                activity_key = (str(activity.id), str(activity.athlete_id))
                if activity_key not in existing_ids:
                    new_activities.append(activity)
            
            if not new_activities:
                self.logger.info("No new activities to add to CSV")
                return True
            
            # Write new activities
            file_exists = csv_path.exists()
            with open(csv_path, 'a', newline='', encoding='utf-8') as csvfile:
                fieldnames = [field.name for field in StravaActivity.__dataclass_fields__.values()]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                if not file_exists:
                    writer.writeheader()
                
                for activity in new_activities:
                    row = asdict(activity)
                    # Convert complex types to strings for CSV compatibility
                    if row['start_latlng']:
                        row['start_latlng'] = str(row['start_latlng'])
                    if row['end_latlng']:
                        row['end_latlng'] = str(row['end_latlng'])
                    writer.writerow(row)
            
            self.logger.info(f"Added {len(new_activities)} new activities to CSV: {csv_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error exporting to CSV: {e}")
            return False

class EmailNotifier:
    """Enhanced email notifications using Brevo API with comprehensive templates"""
    
    def __init__(self, config: Config):
        self.config = config
        self.api_key = os.getenv("BREVO_API_KEY")
        self.from_email = os.getenv("BREVO_FROM_EMAIL", "dominic.allkins@gmail.com")
        self.from_name = os.getenv("BREVO_FROM_NAME", "Strava Data Fetcher")
        self.api_url = "https://api.brevo.com/v3/smtp/email"
        
        # Setup logging
        self.logger = logging.getLogger(f"{__name__}.EmailNotifier")
    
    def send_email(self, subject: str, body: str, attachment_path: Optional[str] = None) -> bool:
        """Send email notification using Brevo API with error handling"""
        if not self.api_key:
            self.logger.warning("No Brevo API key configured, skipping email")
            return False
        
        try:
            headers = {
                "accept": "application/json",
                "content-type": "application/json",
                "api-key": self.api_key
            }
            
            email_data = {
                "sender": {"name": self.from_name, "email": self.from_email},
                "to": [{"email": self.config.notification_email}],
                "subject": subject,
                "htmlContent": body
            }
            
            # Add attachment if provided
            if attachment_path and Path(attachment_path).exists():
                with open(attachment_path, "rb") as f:
                    attachment_content = base64.b64encode(f.read()).decode()
                
                email_data["attachment"] = [{
                    "content": attachment_content,
                    "name": Path(attachment_path).name
                }]
            
            response = requests.post(self.api_url, json=email_data, headers=headers, timeout=30)
            
            if response.status_code == 201:
                self.logger.info(f"Email sent successfully: {subject}")
                return True
            else:
                self.logger.error(f"Failed to send email: {response.status_code} - {response.text}")
                return False
                
        except requests.RequestException as e:
            self.logger.error(f"Network error sending email: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error sending email: {e}")
            return False
    
    def send_daily_summary(self, summary_data: Dict) -> bool:
        """Send comprehensive daily summary email"""
        subject = f"🚴‍♂️ Strava Data Fetch Summary - {datetime.now().strftime('%Y-%m-%d')}"
        
        # Generate athlete performance sections
        athlete_sections = ""
        for athlete, stats in summary_data.get('athlete_stats', {}).items():
            distance_km = stats.get('total_distance', 0) / 1000
            athlete_sections += f"""
                <div style="background: #f8f9fa; padding: 15px; border-radius: 8px; margin: 10px 0;">
                    <h4 style="color: #FC4C02;">{athlete.title()}</h4>
                    <ul>
                        <li><strong>New Activities:</strong> {stats.get('new_activities', 0)}</li>
                        <li><strong>Total Activities:</strong> {stats.get('total_activities', 0)}</li>
                        <li><strong>Total Distance:</strong> {distance_km:.1f} km</li>
                    </ul>
                </div>
            """
        
        body = f"""
        <html>
        <body style="font-family: Arial, sans-serif; color: #333; line-height: 1.6;">
            <h2 style="color: #FC4C02;">🚴‍♂️ Strava Data Fetch Summary</h2>
            <p><strong>Date:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            
            <div style="background: #e8f5e8; padding: 20px; border-radius: 8px; margin: 20px 0;">
                <h3 style="margin-top: 0;">📊 Overall Statistics</h3>
                <ul>
                    <li><strong>New Activities Fetched:</strong> {summary_data.get('new_activities', 0)}</li>
                    <li><strong>Total Activities in Database:</strong> {summary_data.get('total_activities', 0)}</li>
                    <li><strong>Athletes Processed:</strong> {', '.join(summary_data.get('athletes', []))}</li>
                    <li><strong>API Requests Made:</strong> {summary_data.get('api_requests', 0)}</li>
                </ul>
            </div>
            
            <h3 style="color: #FC4C02;">👥 Per-Athlete Summary</h3>
            {athlete_sections}
            
            <p><strong>📁 CSV Export:</strong> The latest data export is attached to this email.</p>
            
            <p style="color: #666; font-size: 12px; margin-top: 30px;">
                <em>🤖 Strava Data Fetcher v2.0 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</em>
            </p>
        </body>
        </html>
        """
        
        return self.send_email(subject, body, self.config.csv_path)
    
    def send_progress_email(self, progress_data: Dict) -> bool:
        """Send progress update email during large fetches"""
        subject = f"🚴‍♂️ Strava Fetch Progress - {progress_data['percentage']:.1f}% Complete"
        
        # Calculate estimated time remaining
        percentage = progress_data['percentage']
        if percentage > 0:
            estimated_remaining = int((100 - percentage) / percentage * 60)
            time_remaining_text = f"{estimated_remaining} minutes"
        else:
            time_remaining_text = "Calculating..."
        
        body = f"""
        <html>
        <body style="font-family: Arial, sans-serif; color: #333; line-height: 1.6;">
            <h2 style="color: #FC4C02;">🚴‍♂️ Strava Historical Fetch Progress</h2>
            <p><strong>Time:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            
            <div style="background: #e3f2fd; padding: 20px; border-radius: 8px; margin: 20px 0;">
                <h3 style="margin-top: 0;">📊 Progress Update</h3>
                <ul>
                    <li><strong>Athlete:</strong> {progress_data['athlete'].title()}</li>
                    <li><strong>Progress:</strong> {progress_data['processed']} of {progress_data['total_estimated']} activities</li>
                    <li><strong>Completion:</strong> {progress_data['percentage']:.1f}%</li>
                    <li><strong>Latest Activity:</strong> {progress_data['latest_activity']}</li>
                    <li><strong>Activity Date:</strong> {progress_data['latest_date']}</li>
                </ul>
                
                <div style="background: #ddd; border-radius: 10px; margin: 15px 0;">
                    <div style="background: #4CAF50; height: 20px; border-radius: 10px; width: {progress_data['percentage']}%;"></div>
                </div>
                
                <p><strong>📈 Estimated time remaining:</strong> {time_remaining_text}</p>
            </div>
            
            <p><em>🤖 Fetching your complete cycling history...</em></p>
        </body>
        </html>
        """
        
        return self.send_email(subject, body)
    
    def send_error_notification(self, error_message: str) -> bool:
        """Send error notification email with details"""
        subject = f"⚠️ Strava Data Fetch Error - {datetime.now().strftime('%Y-%m-%d')}"
        
        body = f"""
        <html>
        <body style="font-family: Arial, sans-serif; color: #333; line-height: 1.6;">
            <h2 style="color: #e74c3c;">⚠️ Strava Data Fetch Error</h2>
            <p><strong>Date:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            
            <div style="background: #ffebee; padding: 20px; border-radius: 8px; margin: 20px 0;">
                <h3 style="margin-top: 0; color: #e74c3c;">Error Details</h3>
                <pre style="background-color: #f4f4f4; padding: 15px; border-radius: 5px; overflow-x: auto; white-space: pre-wrap;">{error_message}</pre>
            </div>
            
            <p>Please check the application logs for more detailed information.</p>
            
            <p style="color: #666; font-size: 12px; margin-top: 30px;">
                <em>🤖 Strava Data Fetcher v2.0 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</em>
            </p>
        </body>
        </html>
        """
        
        return self.send_email(subject, body)
        # ===== ACTIVITY PARSING =====

def parse_strava_activity(activity_data: Dict, athlete_name: str, athlete_id: int) -> StravaActivity:
    """
    Parse Strava API response into StravaActivity object with comprehensive field handling
    
    Args:
        activity_data: Raw activity data from Strava API
        athlete_name: Name of the athlete
        athlete_id: Strava athlete ID
        
    Returns:
        StravaActivity object with all fields properly parsed
    """
    
    # Setup logging for debugging
    logger = logging.getLogger(f"{__name__}.parse_strava_activity")
    
    # Debug logging for field analysis
    logger.debug(f"Parsing activity {activity_data.get('id')} for {athlete_name}")
    
    # Handle gear information with fallback
    gear_id = None
    gear_name = None
    if activity_data.get('gear'):
        gear_info = activity_data['gear']
        gear_id = gear_info.get('id')
        gear_name = gear_info.get('name')
    elif activity_data.get('gear_id'):
        gear_id = activity_data.get('gear_id')
    
    # Handle map polyline data
    map_polyline = None
    if activity_data.get('map') and activity_data['map'].get('summary_polyline'):
        map_polyline = activity_data['map']['summary_polyline']
    
    # Handle coordinate data
    start_latlng = activity_data.get('start_latlng')
    end_latlng = activity_data.get('end_latlng')
    
    # Handle datetime conversion for MySQL compatibility
    def clean_datetime(dt_str):
        """Clean datetime string for MySQL storage"""
        if dt_str and dt_str.endswith('Z'):
            return dt_str[:-1]  # Remove the 'Z' suffix
        return dt_str
    
    # Handle missing or invalid numeric values
    def safe_float(value, default=0.0):
        """Safely convert to float with default"""
        try:
            return float(value) if value is not None else default
        except (ValueError, TypeError):
            return default
    
    def safe_int(value, default=0):
        """Safely convert to int with default"""
        try:
            return int(value) if value is not None else default
        except (ValueError, TypeError):
            return default
    
    # Create and return StravaActivity object
    return StravaActivity(
        # Core identification
        id=activity_data['id'],
        athlete_id=athlete_id,
        athlete_name=athlete_name,
        
        # Basic activity info
        name=activity_data.get('name', ''),
        type=activity_data.get('type', ''),
        sport_type=activity_data.get('sport_type', ''),
        
        # Date and time information
        start_date_local=clean_datetime(activity_data.get('start_date_local', '')),
        start_date=clean_datetime(activity_data.get('start_date', '')),
        utc_offset=safe_float(activity_data.get('utc_offset')),
        timezone=activity_data.get('timezone', ''),
        
        # Equipment information
        gear_id=gear_id,
        gear_name=gear_name,
        device_name=activity_data.get('device_name'),
        
        # Distance and time metrics
        distance=safe_float(activity_data.get('distance')),
        elapsed_time=safe_int(activity_data.get('elapsed_time')),
        moving_time=safe_int(activity_data.get('moving_time')),
        
        # Performance metrics
        calories=safe_float(activity_data.get('calories')) if activity_data.get('calories') else None,
        average_heartrate=safe_float(activity_data.get('average_heartrate')) if activity_data.get('average_heartrate') else None,
        max_heartrate=safe_float(activity_data.get('max_heartrate')) if activity_data.get('max_heartrate') else None,
        average_watts=safe_float(activity_data.get('average_watts')) if activity_data.get('average_watts') else None,
        max_watts=safe_float(activity_data.get('max_watts')) if activity_data.get('max_watts') else None,
        weighted_average_watts=safe_float(activity_data.get('weighted_average_watts')) if activity_data.get('weighted_average_watts') else None,
        average_cadence=safe_float(activity_data.get('average_cadence')) if activity_data.get('average_cadence') else None,
        average_speed=safe_float(activity_data.get('average_speed')),
        max_speed=safe_float(activity_data.get('max_speed')),
        total_elevation_gain=safe_float(activity_data.get('total_elevation_gain')),
        
        # Social and training info
        kudos_count=safe_int(activity_data.get('kudos_count')),
        trainer=bool(activity_data.get('trainer', False)),
        
        # Location data
        map_polyline=map_polyline,
        start_latlng=start_latlng,
        end_latlng=end_latlng
    )

# ===== MAIN APPLICATION CLASS =====

class StravaDataFetcher:
    """
    Main application class for fetching and managing Strava activity data
    
    Features:
    - Multi-athlete support
    - Incremental and full historical fetching
    - Progress tracking and email notifications
    - Database storage and CSV export
    - Comprehensive error handling and logging
    """
    
    def __init__(self, config: Config):
        self.config = config
        self.api = StravaAPI(config)
        self.db = DatabaseManager(config)
        self.csv_exporter = CSVExporter(config.csv_path)
        self.notifier = EmailNotifier(config)
        
        # Setup comprehensive logging
        self.logger = logging.getLogger(f"{__name__}.StravaDataFetcher")
        self._setup_logging()
        
        self.logger.info("StravaDataFetcher initialized successfully")
    
    def _setup_logging(self):
        """Setup comprehensive logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('strava_fetcher.log'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        # Set specific log levels
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("aiohttp").setLevel(logging.WARNING)
    
    async def setup_oauth(self, athlete_name: str) -> bool:
        """Setup OAuth authentication for a new athlete"""
        if athlete_name not in ["dominic", "clare"]:
            self.logger.error(f"Unknown athlete: {athlete_name}")
            return False
        
        try:
            auth_url = self.api.get_authorization_url(athlete_name)
            print(f"\n🔗 Please visit this URL to authorize access for {athlete_name}:")
            print(f"   {auth_url}")
            
            # Try to open browser automatically
            try:
                webbrowser.open(auth_url)
                print("✅ Opened authorization URL in your default browser")
            except Exception:
                print("❌ Could not open browser automatically")
            
            code = input(f"\n📝 Enter the authorization code for {athlete_name}: ").strip()
            
            if not code:
                print("❌ No authorization code provided")
                return False
            
            success = await self.api.exchange_code_for_tokens(code, athlete_name)
            
            if success:
                athlete_config = self.config.athletes[athlete_name]
                print(f"✅ Successfully authorized {athlete_name}!")
                print(f"\n💾 Save these to your .env file:")
                print(f"{athlete_name.upper()}_ACCESS_TOKEN={athlete_config['access_token']}")
                print(f"{athlete_name.upper()}_REFRESH_TOKEN={athlete_config['refresh_token']}")
                print(f"{athlete_name.upper()}_TOKEN_EXPIRES={athlete_config['expires_at']}")
                return True
            else:
                print(f"❌ Failed to authorize {athlete_name}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error in OAuth setup for {athlete_name}: {e}")
            print(f"❌ OAuth setup failed: {e}")
            return False
    
    async def fetch_athlete_data(self, athlete_name: str, full_fetch: bool = False) -> Dict[str, Any]:
        """
        Fetch and process data for a single athlete
        
        Args:
            athlete_name: Name of the athlete to fetch data for
            full_fetch: Whether to fetch all historical data or just recent
            
        Returns:
            Dictionary with fetch results and statistics
        """
        self.logger.info(f"Starting data fetch for {athlete_name} (full_fetch={full_fetch})")
        
        try:
            # Get athlete information and ID
            athlete_info = await self.api.get_athlete_info(athlete_name)
            if not athlete_info:
                self.logger.error(f"Could not retrieve athlete info for {athlete_name}")
                return self._create_empty_result(athlete_name)
            
            athlete_id = athlete_info['id']
            self.logger.info(f"Athlete {athlete_name} has ID: {athlete_id}")
            
            # Determine date range for fetching
            after_date = None
            if not full_fetch:
                latest_date = self.db.get_latest_activity_date(athlete_name)
                if latest_date:
                    # Add buffer to avoid missing activities
                    after_date = latest_date - timedelta(days=1)
                    self.logger.info(f"Incremental fetch: getting activities after {after_date}")
                else:
                    self.logger.info(f"No existing data found, fetching all activities for {athlete_name}")
            else:
                self.logger.info(f"Full historical fetch requested for {athlete_name}")
            
            # Fetch activities from Strava
            activities_data = await self.api.get_activities(athlete_name, after_date)
            
            if not activities_data:
                self.logger.warning(f"No activities returned for {athlete_name}")
                return self._create_empty_result(athlete_name)
            
            # Process activities
            result = await self._process_activities(
                activities_data, athlete_name, athlete_id, full_fetch
            )
            
            self.logger.info(f"Completed data fetch for {athlete_name}: {result['new_activities']} new activities")
            return result
            
        except Exception as e:
            self.logger.error(f"Error fetching data for {athlete_name}: {e}")
            return self._create_empty_result(athlete_name)
    
    async def _process_activities(self, activities_data: List[Dict], athlete_name: str, 
                                athlete_id: int, full_fetch: bool) -> Dict[str, Any]:
        """Process raw activity data into database-ready format"""
        
        # Get existing activity IDs for duplicate detection in full fetch
        existing_ids = set()
        if full_fetch:
            existing_ids = self.db.get_existing_activity_ids(athlete_name)
            self.logger.info(f"Will skip {len(existing_ids)} existing activities for {athlete_name}")
        
        # Process activities with filtering and progress tracking
        processed_activities = []
        skipped_existing = 0
        skipped_non_cycling = 0
        last_progress_email = time.time()
        progress_email_interval = 3600  # 1 hour
        
        for i, activity_data in enumerate(activities_data):
            try:
                activity_id = activity_data['id']
                
                # Skip existing activities in full fetch mode
                if full_fetch and activity_id in existing_ids:
                    skipped_existing += 1
                    continue
                
                # Filter for cycling activities only
                activity_type = activity_data.get('type', '').lower()
                sport_type = activity_data.get('sport_type', '').lower()
                
                if not self._is_cycling_activity(activity_type, sport_type):
                    skipped_non_cycling += 1
                    continue
                
                # Get detailed activity data for better metrics
                detailed_data = await self.api.get_detailed_activity(activity_id, athlete_name)
                if detailed_data:
                    activity = parse_strava_activity(detailed_data, athlete_name, athlete_id)
                    self.logger.debug(f"Added detailed activity: {activity.name}")
                else:
                    # Fallback to basic data
                    self.logger.warning(f"Using basic data for activity {activity_id}")
                    activity = parse_strava_activity(activity_data, athlete_name, athlete_id)
                
                processed_activities.append(activity)
                
                # Send progress email for long-running fetches
                if self._should_send_progress_email(last_progress_email, progress_email_interval):
                    await self._send_progress_update(
                        athlete_name, i + 1, len(activities_data), activity
                    )
                    last_progress_email = time.time()
                
            except Exception as e:
                self.logger.error(f"Error processing activity {activity_data.get('id', 'unknown')}: {e}")
                continue
        
        # Log processing summary
        self._log_processing_summary(
            athlete_name, len(activities_data), skipped_existing, 
            skipped_non_cycling, len(processed_activities)
        )
        
        # Save to database and export
        new_count = self.db.save_activities(processed_activities)
        
        if processed_activities:
            csv_success = self.csv_exporter.export_activities(processed_activities)
            if not csv_success:
                self.logger.warning("CSV export failed")
        
        # Get updated summary statistics
        summary = self.db.get_activity_summary(athlete_name)
        
        return {
            'athlete': athlete_name,
            'new_activities': new_count,
            'total_activities': summary['total_activities'],
            'total_distance': summary['total_distance'],
            'total_elevation': summary['total_elevation'],
            'total_calories': summary['total_calories'],
            'api_requests': self.api.request_count
        }
    
    def _is_cycling_activity(self, activity_type: str, sport_type: str) -> bool:
        """Check if activity is a cycling activity"""
        cycling_types = {'ride', 'virtualride', 'ebikeride'}
        return activity_type in cycling_types or sport_type in cycling_types
    
    def _should_send_progress_email(self, last_email_time: float, interval: float) -> bool:
        """Check if it's time to send a progress email"""
        return time.time() - last_email_time >= interval
    
    async def _send_progress_update(self, athlete_name: str, processed: int, 
                                  total: int, latest_activity: StravaActivity):
        """Send progress update email"""
        try:
            progress_data = {
                'athlete': athlete_name,
                'processed': processed,
                'total_estimated': total,
                'percentage': (processed / total) * 100,
                'latest_activity': latest_activity.name,
                'latest_date': latest_activity.start_date_local
            }
            
            success = self.notifier.send_progress_email(progress_data)
            if success:
                self.logger.info(f"Progress email sent: {processed}/{total} activities processed")
            
        except Exception as e:
            self.logger.error(f"Failed to send progress email: {e}")
    
    def _log_processing_summary(self, athlete_name: str, total_from_api: int, 
                              skipped_existing: int, skipped_non_cycling: int, 
                              processed: int):
        """Log comprehensive processing summary"""
        self.logger.info(f"Processing summary for {athlete_name}:")
        self.logger.info(f"  - Total activities from API: {total_from_api}")
        self.logger.info(f"  - Skipped (already in DB): {skipped_existing}")
        self.logger.info(f"  - Skipped (non-cycling): {skipped_non_cycling}")
        self.logger.info(f"  - Processed cycling activities: {processed}")
    
    def _create_empty_result(self, athlete_name: str) -> Dict[str, Any]:
        """Create empty result dictionary for failed fetches"""
        return {
            'athlete': athlete_name,
            'new_activities': 0,
            'total_activities': 0,
            'total_distance': 0,
            'total_elevation': 0,
            'total_calories': 0,
            'api_requests': self.api.request_count
        }
    
    async def run_data_fetch(self, full_fetch: bool = False) -> bool:
        """
        Main data fetching routine with comprehensive error handling
        
        Args:
            full_fetch: Whether to perform full historical fetch
            
        Returns:
            True if fetch completed successfully, False otherwise
        """
        try:
            fetch_type = "full historical" if full_fetch else "incremental"
            self.logger.info(f"Starting Strava data fetch ({fetch_type})")
            
            # Validate athlete configuration
            if not self.config.athletes:
                self.logger.warning("No athletes configured with valid tokens")
                print("❌ No athletes configured with valid tokens.")
                print("\nTo configure athletes, set these environment variables:")
                print("  • Dominic: DOMINIC_ACCESS_TOKEN, DOMINIC_REFRESH_TOKEN")
                print("  • Clare: CLARE_ACCESS_TOKEN, CLARE_REFRESH_TOKEN")
                print("\nOr run: python strava_main.py setup <athlete_name>")
                return False
            
            self.logger.info(f"Processing {len(self.config.athletes)} athletes: {list(self.config.athletes.keys())}")
            
            # Initialize summary data
            summary_data = {
                'new_activities': 0,
                'total_activities': 0,
                'athletes': [],
                'api_requests': 0,
                'athlete_stats': {},
                'fetch_type': fetch_type,
                'start_time': datetime.now().isoformat()
            }
            
            # Process each configured athlete
            for athlete_name in self.config.athletes.keys():
                athlete_config = self.config.athletes[athlete_name]
                
                if not athlete_config.get('access_token'):
                    self.logger.warning(f"No access token for {athlete_name}, skipping")
                    continue
                
                try:
                    self.logger.info(f"Processing athlete: {athlete_name}")
                    result = await self.fetch_athlete_data(athlete_name, full_fetch)
                    
                    # Update summary
                    summary_data['new_activities'] += result['new_activities']
                    summary_data['athletes'].append(athlete_name)
                    summary_data['athlete_stats'][athlete_name] = {
                        'total_activities': result['total_activities'],
                        'total_distance': result['total_distance'],
                        'total_elevation': result['total_elevation'],
                        'total_calories': result['total_calories'],
                        'new_activities': result['new_activities']
                    }
                    
                    self.logger.info(f"Completed processing {athlete_name}: {result['new_activities']} new activities")
                    
                except Exception as e:
                    error_msg = f"Error processing {athlete_name}: {str(e)}"
                    self.logger.error(error_msg)
                    
                    # Send error notification
                    try:
                        self.notifier.send_error_notification(error_msg)
                    except Exception as email_error:
                        self.logger.error(f"Failed to send error email: {email_error}")
            
            # Get overall database summary
            overall_summary = self.db.get_activity_summary()
            summary_data['total_activities'] = overall_summary['total_activities']
            summary_data['api_requests'] = self.api.request_count
            summary_data['end_time'] = datetime.now().isoformat()
            
            # Send comprehensive summary email
            try:
                email_success = self.notifier.send_daily_summary(summary_data)
                if email_success:
                    self.logger.info("Summary email sent successfully")
                else:
                    self.logger.warning("Failed to send summary email")
            except Exception as e:
                self.logger.error(f"Error sending summary email: {e}")
            
            self.logger.info("Data fetch completed successfully")
            self._print_completion_summary(summary_data)
            return True
            
        except Exception as e:
            error_msg = f"Critical error in data fetch: {str(e)}"
            self.logger.error(error_msg)
            
            try:
                self.notifier.send_error_notification(error_msg)
            except Exception as email_error:
                self.logger.error(f"Failed to send critical error email: {email_error}")
            
            print(f"❌ Data fetch failed: {e}")
            return False
    
    def _print_completion_summary(self, summary_data: Dict):
        """Print completion summary to console"""
        print(f"\n✅ Data fetch completed!")
        print(f"📊 Summary:")
        print(f"  • New activities: {summary_data['new_activities']}")
        print(f"  • Total in database: {summary_data['total_activities']}")
        print(f"  • Athletes processed: {len(summary_data['athletes'])}")
        print(f"  • API requests: {summary_data['api_requests']}")
        
        if summary_data['athlete_stats']:
            print(f"\n👥 Per-athlete results:")
            for athlete, stats in summary_data['athlete_stats'].items():
                distance_km = stats['total_distance'] / 1000
                print(f"  • {athlete.title()}: {stats['new_activities']} new, {distance_km:.1f}km total")

# ===== MAIN ENTRY POINT AND CLI =====

async def main():
    """
    Main entry point with comprehensive command-line interface
    """
    try:
        # Load and validate configuration
        config = Config.from_env()
        config.validate_config()
        
    except ValueError as e:
        print(f"❌ Configuration error: {e}")
        print("\nPlease check your .env file and ensure all required variables are set.")
        return 1
    except Exception as e:
        print(f"❌ Failed to load configuration: {e}")
        return 1
    
    # Initialize application
    try:
        fetcher = StravaDataFetcher(config)
    except Exception as e:
        print(f"❌ Failed to initialize application: {e}")
        return 1
    
    # Parse command line arguments
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == "setup":
            # OAuth setup for athletes
            if len(sys.argv) > 2:
                athlete_name = sys.argv[2].lower()
                if athlete_name in ["dominic", "clare"]:
                    success = await fetcher.setup_oauth(athlete_name)
                    return 0 if success else 1
                else:
                    print(f"❌ Unknown athlete: {athlete_name}")
                    print("Available athletes: dominic, clare")
                    return 1
            else:
                print("Usage: python strava_main.py setup <athlete_name>")
                print("Available athletes: dominic, clare")
                return 1
        
        elif command == "fetch":
            # Standard incremental fetch
            print("🚴‍♂️ Running incremental data fetch...")
            success = await fetcher.run_data_fetch(full_fetch=False)
            return 0 if success else 1
        
        elif command == "fetch-all":
            # Full historical fetch
            print("🚴‍♂️ Running full historical data fetch...")
            print("⚠️  This may take a long time and use many API requests!")
            
            confirm = input("Continue? (y/N): ").lower().strip()
            if confirm in ['y', 'yes']:
                success = await fetcher.run_data_fetch(full_fetch=True)
                return 0 if success else 1
            else:
                print("❌ Cancelled by user")
                return 0
        
        elif command == "test":
            # Test mode with limited activities
            original_test_mode = config.test_mode
            original_max_activities = config.max_test_activities
            
            config.test_mode = True
            config.max_test_activities = 5
            
            print("🧪 Running in test mode (max 5 activities per athlete)")
            success = await fetcher.run_data_fetch()
            
            # Restore original settings
            config.test_mode = original_test_mode
            config.max_test_activities = original_max_activities
            
            return 0 if success else 1
        
        elif command == "status":
            # Display comprehensive status information
            print("\n📊 Strava Data Fetcher Status")
            print("=" * 50)
            
            # Database connectivity
            if fetcher.db.test_connection():
                print("✅ Database: Connected")
            else:
                print("❌ Database: Connection failed")
            
            # Athlete status
            print(f"\n👥 Athlete Configuration:")
            for athlete_name in ["dominic", "clare"]:
                if athlete_name in config.athletes and config.athletes[athlete_name].get('access_token'):
                    summary = fetcher.db.get_activity_summary(athlete_name)
                    distance_km = summary['total_distance'] / 1000
                    print(f"✅ {athlete_name.title()}:")
                    print(f"   • Total activities: {summary['total_activities']}")
                    print(f"   • Total distance: {distance_km:.1f} km")
                    print(f"   • Total elevation: {summary.get('total_elevation', 0):.0f} m")
                    print(f"   • Latest activity: {summary['latest_activity'] or 'None'}")
                else:
                    print(f"❌ {athlete_name.title()}: Not authenticated")
            
            # Overall statistics
            overall = fetcher.db.get_activity_summary()
            overall_distance_km = overall['total_distance'] / 1000
            print(f"\n📈 Overall Statistics:")
            print(f"   • Total activities: {overall['total_activities']}")
            print(f"   • Total distance: {overall_distance_km:.1f} km")
            print(f"   • Total elevation: {overall.get('total_elevation', 0):.0f} m")
            print(f"   • Unique athletes: {overall.get('unique_athletes', 0)}")
            
            # Configuration
            print(f"\n⚙️  Configuration:")
            print(f"   • CSV export: {config.csv_path}")
            print(f"   • Test mode: {'ON' if config.test_mode else 'OFF'}")
            print(f"   • Max test activities: {config.max_test_activities}")
            
            return 0
        
        elif command in ["help", "-h", "--help"]:
            # Display help information
            print("\n🚴‍♂️ Strava Data Fetcher v2.0")
            print("=" * 40)
            print("\nAvailable commands:")
            print("  setup <athlete>   - Setup OAuth for athlete (dominic|clare)")
            print("  fetch            - Run incremental data fetch")
            print("  fetch-all        - Run full historical data fetch")
            print("  test             - Run in test mode (5 activities max)")
            print("  status           - Show detailed status information")
            print("  help             - Show this help message")
            print("\nExamples:")
            print("  python strava_main.py setup dominic")
            print("  python strava_main.py fetch")
            print("  python strava_main.py status")
            
            return 0
        
        else:
            print(f"❌ Unknown command: {command}")
            print("Available commands: setup, fetch, fetch-all, test, status, help")
            print("Run 'python strava_main.py help' for more information")
            return 1
    
    else:
        # Default behavior: run incremental fetch
        print("🚴‍♂️ Running default incremental data fetch...")
        print("Use 'python strava_main.py help' to see all available commands")
        success = await fetcher.run_data_fetch()
        return 0 if success else 1

# ===== APPLICATION ENTRY POINT =====

if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user")
        sys.exit(130)  # Standard exit code for SIGINT
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        logging.getLogger(__name__).error(f"Unexpected error in main: {e}", exc_info=True)
        sys.exit(1)