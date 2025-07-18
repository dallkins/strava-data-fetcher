#scheduled_refresh_service.py
#!/usr/bin/env python3
"""
Scheduled Refresh Service
Handles daily/weekly refresh jobs for social updates (kudos, comments)
"""

import schedule
import time
import asyncio
import logging
import sqlite3
from datetime import datetime, timedelta
from dotenv import load_dotenv

from strava_main import StravaDataFetcher, Config

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('refresh_scheduler.log'),
        logging.StreamHandler()
    ]
)

# Global config and fetcher
config = Config.from_env()
fetcher = StravaDataFetcher(config)

class RefreshScheduler:
    """Manages scheduled refresh operations"""
    
    def __init__(self, db_path):
        self.db_path = db_path
    
    def schedule_weekly_social_refresh(self):
        """Schedule refresh for recent activities to update social data (kudos, comments)"""
        try:
            logging.info("Starting weekly social refresh job")
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get activities from last 7 days
            seven_days_ago = (datetime.now() - timedelta(days=7)).isoformat()
            
            cursor.execute('''
                SELECT id, athlete_name, name, start_date_local
                FROM strava_activities
                WHERE start_date_local >= ?
                ORDER BY start_date_local DESC
            ''', (seven_days_ago,))
            
            recent_activities = cursor.fetchall()
            conn.close()
            
            logging.info(f"Found {len(recent_activities)} activities from last 7 days for social refresh")
            
            # Schedule each activity for refresh
            for activity_id, athlete_name, name, start_date in recent_activities:
                self.add_to_refresh_queue(activity_id, athlete_name, 'weekly_social')
                
            logging.info("Weekly social refresh scheduling completed")
            
        except Exception as e:
            logging.error(f"Error in weekly social refresh: {e}")
    
    def schedule_monthly_deep_refresh(self):
        """Schedule deep refresh for activities from last 30 days"""
        try:
            logging.info("Starting monthly deep refresh job")
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get activities from last 30 days
            thirty_days_ago = (datetime.now() - timedelta(days=30)).isoformat()
            
            cursor.execute('''
                SELECT id, athlete_name
                FROM strava_activities
                WHERE start_date_local >= ?
                AND (last_refresh_at IS NULL OR last_refresh_at < datetime('now', '-7 days'))
                ORDER BY start_date_local DESC
            ''', (thirty_days_ago,))
            
            activities_to_refresh = cursor.fetchall()
            conn.close()
            
            logging.info(f"Found {len(activities_to_refresh)} activities for monthly deep refresh")
            
            for activity_id, athlete_name in activities_to_refresh:
                self.add_to_refresh_queue(activity_id, athlete_name, 'monthly_deep')
                
            logging.info("Monthly deep refresh scheduling completed")
            
        except Exception as e:
            logging.error(f"Error in monthly deep refresh: {e}")
    
    def add_to_refresh_queue(self, activity_id, athlete_name, refresh_type):
        """Add activity to refresh queue"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        now = datetime.now()
        # Stagger refreshes to avoid rate limiting
        scheduled_time = now + timedelta(minutes=len(cursor.execute('SELECT id FROM activity_refresh_queue WHERE completed_at IS NULL').fetchall()) * 2)
        
        cursor.execute('''
            INSERT OR REPLACE INTO activity_refresh_queue
            (activity_id, athlete_name, webhook_received_at, scheduled_refresh_at, refresh_type)
            VALUES (?, ?, ?, ?, ?)
        ''', (activity_id, athlete_name, now, scheduled_time, refresh_type))
        
        conn.commit()
        conn.close()
    
    def process_refresh_queue(self):
        """Process activities in the refresh queue"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            now = datetime.now()
            
            # Get activities due for refresh
            cursor.execute('''
                SELECT activity_id, athlete_name, refresh_type
                FROM activity_refresh_queue
                WHERE scheduled_refresh_at <= ? AND completed_at IS NULL
                ORDER BY scheduled_refresh_at
                LIMIT 10
            ''', (now,))
            
            activities_to_refresh = cursor.fetchall()
            conn.close()
            
            if not activities_to_refresh:
                return
                
            logging.info(f"Processing {len(activities_to_refresh)} activities from refresh queue")
            
            for activity_id, athlete_name, refresh_type in activities_to_refresh:
                asyncio.run(self.refresh_activity(activity_id, athlete_name, refresh_type))
                time.sleep(2)  # Rate limiting
                
        except Exception as e:
            logging.error(f"Error processing refresh queue: {e}")
    
    async def refresh_activity(self, activity_id, athlete_name, refresh_type):
        """Refresh a single activity"""
        try:
            logging.info(f"Refreshing activity {activity_id} ({refresh_type})")
            
            # Get updated activity data
            detailed_data = await fetcher.api.get_detailed_activity(activity_id, athlete_name)
            
            if detailed_data:
                from strava_main import parse_strava_activity
                activity = parse_strava_activity(detailed_data, athlete_name)
                
                # Update in database
                fetcher.db.save_activities([activity])
                
                # Mark refresh as completed
                self.mark_refresh_completed(activity_id, refresh_type)
                
                logging.info(f"Successfully refreshed activity {activity_id}")
            else:
                logging.error(f"Could not fetch data for activity {activity_id}")
                
        except Exception as e:
            logging.error(f"Error refreshing activity {activity_id}: {e}")
    
    def mark_refresh_completed(self, activity_id, refresh_type):
        """Mark refresh as completed"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        now = datetime.now()
        
        cursor.execute('''
            UPDATE activity_refresh_queue
            SET completed_at = ?
            WHERE activity_id = ? AND refresh_type = ?
        ''', (now, activity_id, refresh_type))
        
        cursor.execute('''
            UPDATE strava_activities
            SET needs_refresh = FALSE, last_refresh_at = ?
            WHERE id = ?
        ''', (now, activity_id))
        
        conn.commit()
        conn.close()
    
    def cleanup_old_queue_entries(self):
        """Clean up old completed queue entries"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Delete completed entries older than 30 days
            thirty_days_ago = datetime.now() - timedelta(days=30)
            
            cursor.execute('''
                DELETE FROM activity_refresh_queue
                WHERE completed_at IS NOT NULL AND completed_at < ?
            ''', (thirty_days_ago,))
            
            deleted_count = cursor.rowcount
            
            # Delete old webhook events older than 90 days
            ninety_days_ago = datetime.now() - timedelta(days=90)
            
            cursor.execute('''
                DELETE FROM webhook_events
                WHERE received_at < ?
            ''', (ninety_days_ago,))
            
            webhook_deleted = cursor.rowcount
            
            conn.commit()
            conn.close()
            
            if deleted_count > 0 or webhook_deleted > 0:
                logging.info(f"Cleanup: Removed {deleted_count} old refresh entries and {webhook_deleted} old webhook events")
                
        except Exception as e:
            logging.error(f"Error in cleanup: {e}")
    
    def send_daily_refresh_summary(self):
        """Send daily summary of refresh activities"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get stats for last 24 hours
            yesterday = datetime.now() - timedelta(days=1)
            
            cursor.execute('''
                SELECT COUNT(*) as completed,
                       SUM(CASE WHEN refresh_type = 'webhook_delayed' THEN 1 ELSE 0 END) as webhook_refreshes,
                       SUM(CASE WHEN refresh_type = 'weekly_social' THEN 1 ELSE 0 END) as social_refreshes
                FROM activity_refresh_queue
                WHERE completed_at >= ?
            ''', (yesterday,))
            
            stats = cursor.fetchone()
            
            cursor.execute('''
                SELECT COUNT(*) FROM activity_refresh_queue 
                WHERE completed_at IS NULL
            ''', )
            
            pending = cursor.fetchone()[0]
            
            conn.close()
            
            if stats[0] > 0 or pending > 0:
                fetcher.notifier.send_email(
                    "📊 Daily Strava Refresh Summary",
                    f"""
                    <html><body>
                    <h2>📊 Daily Strava Refresh Summary</h2>
                    <p><strong>Date:</strong> {datetime.now().strftime('%Y-%m-%d')}</p>
                    
                    <h3>Refresh Statistics (Last 24 Hours):</h3>
                    <ul>
                        <li><strong>Total Refreshes Completed:</strong> {stats[0]}</li>
                        <li><strong>Webhook-triggered Refreshes:</strong> {stats[1]}</li>
                        <li><strong>Social Data Refreshes:</strong> {stats[2]}</li>
                        <li><strong>Pending Refreshes:</strong> {pending}</li>
                    </ul>
                    
                    <p><em>🤖 Automated refresh monitoring</em></p>
                    </body></html>
                    """
                )
                
        except Exception as e:
            logging.error(f"Error sending refresh summary: {e}")

def main():
    """Main scheduler loop"""
    logging.info("🔄 Starting Strava Refresh Scheduler")
    
    refresh_scheduler = RefreshScheduler(config.db_path)
    
    # Schedule jobs
    schedule.every().hour.do(refresh_scheduler.process_refresh_queue)
    schedule.every().day.at("02:00").do(refresh_scheduler.schedule_weekly_social_refresh)
    schedule.every().sunday.at("03:00").do(refresh_scheduler.schedule_monthly_deep_refresh)
    schedule.every().day.at("06:00").do(refresh_scheduler.cleanup_old_queue_entries)
    schedule.every().day.at("23:30").do(refresh_scheduler.send_daily_refresh_summary)
    
    # In test mode, run more frequently
    if config.test_mode:
        logging.info("Test mode: More frequent refresh processing")
        schedule.every(10).minutes.do(refresh_scheduler.process_refresh_queue)
    
    logging.info("Refresh scheduler configured. Scheduled jobs:")
    for job in schedule.jobs:
        logging.info(f"  - {job}")
    
    # Run the scheduler
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
            
        except KeyboardInterrupt:
            logging.info("Refresh scheduler stopped by user")
            break
        except Exception as e:
            logging.error(f"Scheduler error: {e}")
            time.sleep(60)

if __name__ == "__main__":
    main()