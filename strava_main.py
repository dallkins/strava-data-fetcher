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
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from dataclasses import dataclass, asdict
from pathlib import Path
import sqlite3
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
    db_path: str = "strava_data.db"
    csv_path: str = "strava_rides.csv"
    smtp_email: str = "dominic.allkins@gmail.com" 
    notification_email: str = "dominic@allkins.com"
    test_mode: bool = False
    max_test_activities: int = 3
    rate_limit_requests_per_15min: int = 100
    rate_limit_daily: int = 1000
    athletes: Dict[str, Dict] = None
    
    def __post_init__(self):
        if self.athletes is None:
            self.athletes = {}
    
    @classmethod
    def from_env(cls):
        return cls(
            strava_client_id=os.getenv("STRAVA_CLIENT_ID"),
            strava_client_secret=os.getenv("STRAVA_CLIENT_SECRET"),
            smtp_password=os.getenv("GMAIL_APP_PASSWORD"),
            db_path=os.getenv("STRAVA_DB_PATH", "strava_data.db"),
            csv_path=os.getenv("STRAVA_CSV_PATH", "strava_rides.csv"),
            smtp_email=os.getenv("STRAVA_SMTP_EMAIL", "dominic.allkins@gmail.com"),
            notification_email=os.getenv("STRAVA_NOTIFICATION_EMAIL", "dominic@allkins.com"),
            test_mode=os.getenv("TEST_MODE", "false").lower() == "true",
            max_test_activities=int(os.getenv("STRAVA_MAX_TEST_ACTIVITIES", "3")),
            athletes={
                "dominic": {
                    "access_token": os.getenv("DOMINIC_ACCESS_TOKEN"),
                    "refresh_token": os.getenv("DOMINIC_REFRESH_TOKEN"),
                    "expires_at": int(os.getenv("DOMINIC_TOKEN_EXPIRES", "0"))
                }
            }
        )
    
@dataclass
class StravaActivity:
    """Data class for Strava activity with all required fields"""
    id: int
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
        
        # Check if token needs refresh
        if time.time() > athlete_config['expires_at']:
            if not await self.refresh_token(athlete_name):
                return None
        
        headers = {
            'Authorization': f"Bearer {athlete_config['access_token']}"
        }
        
        url = f"{self.base_url}{endpoint}"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params=params) as response:
                self.request_count += 1
                
                if response.status == 200:
                    return await response.json()
                elif response.status == 429:  # Rate limited
                    logging.warning("Rate limited by Strava, backing off")
                    await asyncio.sleep(60)
                    return await self.make_api_request(endpoint, athlete_name, params)
                else:
                    logging.error(f"API request failed: {response.status} - {await response.text()}")
                    return None
    
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
            
            batch = await self.make_api_request("/athlete/activities", athlete_name, params)
            
            if not batch or len(batch) == 0:
                break
            
            activities.extend(batch)
            
            if len(batch) < per_page:  # Last page
                break
                
            page += 1
            
            # In test mode, break after first batch
            if self.config.test_mode:
                break
        
        return activities[:self.config.max_test_activities] if self.config.test_mode else activities
    
    async def get_detailed_activity(self, activity_id: int, athlete_name: str) -> Optional[Dict]:
        """Get detailed activity data"""
        return await self.make_api_request(f"/activities/{activity_id}", athlete_name)

class DatabaseManager:
    """Handles database operations"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Initialize database with required tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS strava_activities (
                id INTEGER PRIMARY KEY,
                athlete_name TEXT NOT NULL,
                name TEXT,
                start_date_local TEXT,
                start_date TEXT,
                utc_offset REAL,
                gear_id TEXT,
                gear_name TEXT,
                distance REAL,
                elapsed_time INTEGER,
                moving_time INTEGER,
                calories REAL,
                average_heartrate REAL,
                max_heartrate REAL,
                average_watts REAL,
                max_watts REAL,
                average_speed REAL,
                max_speed REAL,
                type TEXT,
                sport_type TEXT,
                total_elevation_gain REAL,
                kudos_count INTEGER,
                weighted_average_watts REAL,
                average_cadence REAL,
                trainer BOOLEAN,
                map_polyline TEXT,
                device_name TEXT,
                timezone TEXT,
                start_latlng TEXT,
                end_latlng TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(id, athlete_name)
            )
        ''')
        
        # Create index on start_date for efficient querying
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_start_date 
            ON strava_activities(start_date_local, athlete_name)
        ''')
        
        conn.commit()
        conn.close()
    
    def get_latest_activity_date(self, athlete_name: str) -> Optional[datetime]:
        """Get the date of the most recent activity for an athlete"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT MAX(start_date_local) 
            FROM strava_activities 
            WHERE athlete_name = ?
        ''', (athlete_name,))
        
        result = cursor.fetchone()[0]
        conn.close()
        
        if result:
            return datetime.fromisoformat(result.replace('Z', '+00:00'))
        return None
    
    def save_activities(self, activities: List[StravaActivity]) -> int:
        """Save activities to database, return count of new activities"""
        if not activities:
            return 0
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        new_count = 0
        
        for activity in activities:
            activity_dict = asdict(activity)
            
            # Convert boolean to integer for SQLite
            activity_dict['trainer'] = int(activity_dict['trainer'])
            
            # Convert lists to JSON strings
            if activity_dict['start_latlng']:
                activity_dict['start_latlng'] = json.dumps(activity_dict['start_latlng'])
            if activity_dict['end_latlng']:
                activity_dict['end_latlng'] = json.dumps(activity_dict['end_latlng'])
            
            try:
                cursor.execute('''
                    INSERT OR REPLACE INTO strava_activities 
                    (id, athlete_name, name, start_date_local, start_date, utc_offset,
                    gear_id, gear_name, distance, elapsed_time, moving_time, calories,
                    average_heartrate, max_heartrate, average_watts, max_watts,
                    average_speed, max_speed, type, sport_type, total_elevation_gain,
                    kudos_count, weighted_average_watts, average_cadence, trainer,
                    map_polyline, device_name, timezone, start_latlng, end_latlng,
                    updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', (
                    activity_dict['id'], activity_dict['athlete_name'], activity_dict['name'],
                    activity_dict['start_date_local'], activity_dict['start_date'], activity_dict['utc_offset'],
                    activity_dict['gear_id'], activity_dict['gear_name'], activity_dict['distance'],
                    activity_dict['elapsed_time'], activity_dict['moving_time'], activity_dict['calories'],
                    activity_dict['average_heartrate'], activity_dict['max_heartrate'], activity_dict['average_watts'],
                    activity_dict['max_watts'], activity_dict['average_speed'], activity_dict['max_speed'],
                    activity_dict['type'], activity_dict['sport_type'], activity_dict['total_elevation_gain'],
                    activity_dict['kudos_count'], activity_dict['weighted_average_watts'], activity_dict['average_cadence'],
                    activity_dict['trainer'], activity_dict['map_polyline'], activity_dict['device_name'],
                    activity_dict['timezone'], activity_dict['start_latlng'], activity_dict['end_latlng']
                ))
                
                if cursor.rowcount > 0:
                    new_count += 1
                    
            except sqlite3.Error as e:
                logging.error(f"Error saving activity {activity.id}: {e}")
                        
        conn.commit()
        conn.close()
        
        return new_count
    
    def get_activity_summary(self, athlete_name: Optional[str] = None) -> Dict:
        """Get summary statistics for activities"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        where_clause = "WHERE athlete_name = ?" if athlete_name else ""
        params = (athlete_name,) if athlete_name else ()
        
        cursor.execute(f'''
            SELECT 
                COUNT(*) as total_activities,
                SUM(distance) as total_distance,
                SUM(moving_time) as total_moving_time,
                AVG(average_speed) as avg_speed,
                MAX(start_date_local) as latest_activity
            FROM strava_activities
            {where_clause}
        ''', params)
        
        result = cursor.fetchone()
        conn.close()
        
        return {
            'total_activities': result[0] or 0,
            'total_distance': result[1] or 0,
            'total_moving_time': result[2] or 0,
            'avg_speed': result[3] or 0,
            'latest_activity': result[4]
        }

class CSVExporter:
    """Handles CSV export functionality"""
    
    def __init__(self, csv_path: str):
        self.csv_path = csv_path
    
    def export_activities(self, activities: List[StravaActivity]):
        """Export activities to CSV"""
        if not activities:
            return
        
        file_exists = Path(self.csv_path).exists()
        
        with open(self.csv_path, 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = [field.name for field in StravaActivity.__dataclass_fields__.values()]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            if not file_exists:
                writer.writeheader()
            
            for activity in activities:
                row = asdict(activity)
                # Convert lists to strings for CSV
                if row['start_latlng']:
                    row['start_latlng'] = str(row['start_latlng'])
                if row['end_latlng']:
                    row['end_latlng'] = str(row['end_latlng'])
                writer.writerow(row)

class EmailNotifier:
    """Handles email notifications"""
    
    def __init__(self, config: Config):
        self.config = config
    
    def send_email(self, subject: str, body: str, attachment_path: Optional[str] = None):
        """Send email notification"""
        try:
            msg = MIMEMultipart()
            msg['From'] = self.config.smtp_email
            msg['To'] = self.config.notification_email
            msg['Subject'] = subject
            
            msg.attach(MIMEText(body, 'html'))
            
            # Add CSV attachment if provided
            if attachment_path and Path(attachment_path).exists():
                with open(attachment_path, "rb") as attachment:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(attachment.read())
                    encoders.encode_base64(part)
                    part.add_header(
                        'Content-Disposition',
                        f'attachment; filename= {Path(attachment_path).name}',
                    )
                    msg.attach(part)
            
            server = smtplib.SMTP('smtp.gmail.com', 587)
            server.starttls()
            server.login(self.config.smtp_email, self.config.smtp_password)
            text = msg.as_string()
            server.sendmail(self.config.smtp_email, self.config.notification_email, text)
            server.quit()
            
            logging.info(f"Email sent: {subject}")
            
        except Exception as e:
            logging.error(f"Failed to send email: {e}")
    
    def send_daily_summary(self, summary_data: Dict):
        """Send daily summary email"""
        subject = f"Strava Data Fetch Summary - {datetime.now().strftime('%Y-%m-%d')}"
        
        body = f"""
        <html>
        <body>
            <h2>Strava Data Fetch Summary</h2>
            <p><strong>Date:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            
            <h3>Summary Statistics:</h3>
            <ul>
                <li><strong>New Activities Fetched:</strong> {summary_data.get('new_activities', 0)}</li>
                <li><strong>Total Activities in Database:</strong> {summary_data.get('total_activities', 0)}</li>
                <li><strong>Athletes Processed:</strong> {', '.join(summary_data.get('athletes', []))}</li>
                <li><strong>API Requests Made:</strong> {summary_data.get('api_requests', 0)}</li>
            </ul>
            
            <h3>Per-Athlete Summary:</h3>
            <ul>
        """
        
        for athlete, stats in summary_data.get('athlete_stats', {}).items():
            body += f"""
                <li><strong>{athlete.title()}:</strong> 
                    {stats['total_activities']} total activities, 
                    {stats['total_distance']:.2f}m total distance
                </li>
            """
        
        body += f"""
            </ul>
            
            <p><strong>CSV Download:</strong> The latest data export is attached to this email.</p>
            
            <p><em>Strava Data Fetcher - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</em></p>
        </body>
        </html>
        """
        
        self.send_email(subject, body, self.config.csv_path)
    
    def send_error_notification(self, error_message: str):
        """Send error notification email"""
        subject = f"Strava Data Fetch Error - {datetime.now().strftime('%Y-%m-%d')}"
        
        body = f"""
        <html>
        <body>
            <h2>Strava Data Fetch Error</h2>
            <p><strong>Date:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            
            <h3>Error Details:</h3>
            <pre>{error_message}</pre>
            
            <p>Please check the application logs for more details.</p>
            
            <p><em>Strava Data Fetcher - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</em></p>
        </body>
        </html>
        """
        
        self.send_email(subject, body)

def parse_strava_activity(activity_data: Dict, athlete_name: str) -> StravaActivity:
    """Parse Strava API response into StravaActivity object"""
    
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
    
    return StravaActivity(
        id=activity_data['id'],
        name=activity_data.get('name', ''),
        start_date_local=activity_data.get('start_date_local', ''),
        start_date=activity_data.get('start_date', ''),
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
    
    def __init__(self, config: Config):
        self.config = config
        self.api = StravaAPI(config)
        self.db = DatabaseManager(config.db_path)
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
    
    async def fetch_athlete_data(self, athlete_name: str) -> Dict:
        """Fetch data for a single athlete"""
        logging.info(f"Fetching data for {athlete_name}")
        
        # Get the date of the most recent activity in our database
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
        
        # Parse activities
        activities = []
        for activity_data in activities_data:
            try:
                activity = parse_strava_activity(activity_data, athlete_name)
                activities.append(activity)
            except Exception as e:
                logging.error(f"Error parsing activity {activity_data.get('id', 'unknown')}: {e}")
        
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
    
    async def run_data_fetch(self):
        """Main data fetching routine"""
        try:
            logging.info("Starting Strava data fetch")
            
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
                    result = await self.fetch_athlete_data(athlete_name)
                    
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
                if athlete_name in config.athletes:
                    await fetcher.setup_oauth(athlete_name)
                else:
                    print(f"❌ Unknown athlete: {athlete_name}")
                    print(f"Available athletes: {', '.join(config.athletes.keys())}")
            else:
                print("Usage: python strava_fetcher.py setup <athlete_name>")
                print(f"Available athletes: {', '.join(config.athletes.keys())}")
        
        elif command == "fetch":
            # Run data fetch
            await fetcher.run_data_fetch()
        
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
            
            for athlete_name in config.athletes.keys():
                if config.athletes[athlete_name].get('access_token'):
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
            print(f"  Database: {config.db_path}")
            print(f"  CSV export: {config.csv_path}")
            print(f"  Test mode: {'ON' if config.test_mode else 'OFF'}")
        
        else:
            print(f"❌ Unknown command: {command}")
            print("Available commands: setup, fetch, test, status")
    
    else:
        # Default: run data fetch
        await fetcher.run_data_fetch()

if __name__ == "__main__":
    asyncio.run(main())