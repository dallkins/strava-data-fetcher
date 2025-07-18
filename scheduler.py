#scheduler.py
#!/usr/bin/env python3
"""
Strava Scheduler
Handles scheduled data fetches and daily summary emails
"""

import schedule
import time
import asyncio
import logging
from datetime import datetime
from dotenv import load_dotenv

from strava_fetcher import StravaDataFetcher, Config

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scheduler.log'),
        logging.StreamHandler()
    ]
)

# Global config and fetcher
config = Config.from_env()
fetcher = StravaDataFetcher(config)

def run_fetch():
    """Run the data fetch process"""
    try:
        logging.info("Scheduler: Starting scheduled data fetch")
        asyncio.run(fetcher.run_data_fetch())
        logging.info("Scheduler: Completed scheduled data fetch")
    except Exception as e:
        logging.error(f"Scheduler: Error in scheduled fetch: {e}")

def send_daily_summary():
    """Send daily summary email"""
    try:
        logging.info("Scheduler: Sending daily summary")
        
        # Get summary data
        summary_data = {
            'new_activities': 0,  # This would need to be tracked since last summary
            'total_activities': 0,
            'athletes': list(config.athletes.keys()),
            'api_requests': 0,
            'athlete_stats': {}
        }
        
        # Get stats for each athlete
        for athlete_name in config.athletes.keys():
            if config.athletes[athlete_name].get('access_token'):
                stats = fetcher.db.get_activity_summary(athlete_name)
                summary_data['athlete_stats'][athlete_name] = stats
        
        # Get overall stats
        overall_stats = fetcher.db.get_activity_summary()
        summary_data['total_activities'] = overall_stats['total_activities']
        
        # Send the summary
        fetcher.notifier.send_daily_summary(summary_data)
        
        logging.info("Scheduler: Daily summary sent successfully")
        
    except Exception as e:
        logging.error(f"Scheduler: Error sending daily summary: {e}")

def main():
    """Main scheduler loop"""
    logging.info("🕐 Starting Strava Scheduler")
    
    # Schedule regular data fetches
    # Fetch every 4 hours during the day (when people are likely to be riding)
    schedule.every().day.at("06:00").do(run_fetch)
    schedule.every().day.at("10:00").do(run_fetch)
    schedule.every().day.at("14:00").do(run_fetch)
    schedule.every().day.at("18:00").do(run_fetch)
    schedule.every().day.at("22:00").do(run_fetch)
    
    # Send daily summary at 23:00
    schedule.every().day.at("23:00").do(send_daily_summary)
    
    # In test mode, run more frequently
    if config.test_mode:
        logging.info("Test mode: Scheduling every 5 minutes")
        schedule.every(5).minutes.do(run_fetch)
        schedule.every(10).minutes.do(send_daily_summary)
    
    logging.info("Scheduler configured. Waiting for scheduled tasks...")
    logging.info("Scheduled tasks:")
    for job in schedule.jobs:
        logging.info(f"  - {job}")
    
    # Run the scheduler
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
            
        except KeyboardInterrupt:
            logging.info("Scheduler stopped by user")
            break
        except Exception as e:
            logging.error(f"Scheduler error: {e}")
            time.sleep(60)  # Wait a minute before retrying

if __name__ == "__main__":
    main()
