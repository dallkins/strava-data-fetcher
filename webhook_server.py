#!/usr/bin/env python3
"""
Enhanced Strava Webhook Server with Activity Update Tracking
Handles create, update events and schedules delayed refreshes
"""

import os
import logging
import asyncio
import sqlite3
import json
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, render_template_string
from dotenv import load_dotenv
import threading
import time

# Import your main application
from strava_main import StravaDataFetcher, Config

# Load environment variables
load_dotenv()

app = Flask(__name__)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('webhook.log'),
        logging.StreamHandler()
    ]
)

# Global config
config = Config.from_env()
fetcher = StravaDataFetcher(config)

# Webhook verification token
WEBHOOK_VERIFY_TOKEN = os.getenv('STRAVA_WEBHOOK_VERIFY_TOKEN', 'your_verify_token_here')

# Athlete ID mapping - you'll need to populate this during initial setup
ATHLETE_MAPPING = {
    # Add your actual Strava athlete IDs here after first webhook
    # 'strava_athlete_id': 'athlete_name'
}

class ActivityRefreshManager:
    """Manages activity refresh scheduling and processing"""
    
    def __init__(self, db_path):
        self.db_path = db_path
        
    def add_webhook_event(self, activity_id, athlete_id, event_type, aspect_type, raw_data):
        """Log webhook event for audit trail"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO webhook_events 
            (activity_id, athlete_id, event_type, aspect_type, raw_data)
            VALUES (?, ?, ?, ?, ?)
        ''', (activity_id, athlete_id, event_type, aspect_type, json.dumps(raw_data)))
        
        conn.commit()
        conn.close()
        
    def schedule_delayed_refresh(self, activity_id, athlete_name, refresh_type='webhook_delayed', delay_hours=1.5):
        """Schedule an activity for delayed refresh"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        now = datetime.now()
        scheduled_time = now + timedelta(hours=delay_hours)
        
        cursor.execute('''
            INSERT OR REPLACE INTO activity_refresh_queue
            (activity_id, athlete_name, webhook_received_at, scheduled_refresh_at, refresh_type)
            VALUES (?, ?, ?, ?, ?)
        ''', (activity_id, athlete_name, now, scheduled_time, refresh_type))
        
        # Mark activity as needing refresh
        cursor.execute('''
            UPDATE strava_activities 
            SET needs_refresh = TRUE, last_webhook_at = ?
            WHERE id = ? AND athlete_name = ?
        ''', (now, activity_id, athlete_name))
        
        conn.commit()
        conn.close()
        
        logging.info(f"Scheduled delayed refresh for activity {activity_id} at {scheduled_time}")
        
    def get_activities_due_for_refresh(self):
        """Get activities that are due for refresh"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        now = datetime.now()
        
        cursor.execute('''
            SELECT activity_id, athlete_name, refresh_type
            FROM activity_refresh_queue
            WHERE scheduled_refresh_at <= ? AND completed_at IS NULL
            ORDER BY scheduled_refresh_at
        ''', (now,))
        
        results = cursor.fetchall()
        conn.close()
        
        return results
        
    def mark_refresh_completed(self, activity_id, refresh_type):
        """Mark a refresh as completed"""
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
        
    def schedule_weekly_refresh(self):
        """Schedule weekly refresh for recent activities (for kudos/social updates)"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get activities from last 7 days
        seven_days_ago = datetime.now() - timedelta(days=7)
        
        cursor.execute('''
            SELECT id, athlete_name
            FROM strava_activities
            WHERE start_date_local >= ?
        ''', (seven_days_ago.isoformat(),))
        
        activities = cursor.fetchall()
        conn.close()
        
        for activity_id, athlete_name in activities:
            self.schedule_delayed_refresh(
                activity_id, 
                athlete_name, 
                refresh_type='weekly_social',
                delay_hours=0  # Immediate for weekly refresh
            )
            
        logging.info(f"Scheduled weekly refresh for {len(activities)} recent activities")

# Initialize refresh manager
refresh_manager = ActivityRefreshManager(config.db_path)

@app.route('/webhook', methods=['GET'])
def webhook_challenge():
    """Handle Strava webhook subscription challenge"""
    challenge = request.args.get('hub.challenge')
    verify_token = request.args.get('hub.verify_token')
    
    if verify_token == WEBHOOK_VERIFY_TOKEN:
        logging.info("Webhook challenge verified")
        return jsonify({'hub.challenge': challenge})
    else:
        logging.warning("Webhook challenge failed - invalid verify token")
        return "Forbidden", 403

@app.route('/webhook', methods=['POST'])
def webhook_event():
    """Handle incoming webhook events from Strava"""
    try:
        event_data = request.get_json()
        
        logging.info(f"Received webhook event: {event_data}")
        
        # Extract event details
        object_type = event_data.get('object_type')
        aspect_type = event_data.get('aspect_type')
        activity_id = event_data.get('object_id')
        owner_id = event_data.get('owner_id')
        
        # Log the webhook event
        refresh_manager.add_webhook_event(
            activity_id, owner_id, object_type, aspect_type, event_data
        )
        
        # Check if this is an activity event we care about
        if object_type == 'activity' and aspect_type in ['create', 'update']:
            
            # Determine which athlete this belongs to
            athlete_name = determine_athlete_from_owner_id(owner_id)
            
            if athlete_name:
                if aspect_type == 'create':
                    logging.info(f"New activity {activity_id} for {athlete_name}, immediate fetch")
                    # For new activities, fetch immediately
                    threading.Timer(30.0, lambda: asyncio.run(fetch_activity_immediate(activity_id, athlete_name))).start()
                    
                elif aspect_type == 'update':
                    logging.info(f"Activity {activity_id} updated for {athlete_name}, scheduling delayed refresh")
                    # For updates, schedule delayed refresh (user might still be editing)
                    refresh_manager.schedule_delayed_refresh(activity_id, athlete_name)
                    
            else:
                logging.warning(f"Unknown owner_id: {owner_id} - update ATHLETE_MAPPING")
                
        return "OK", 200
        
    except Exception as e:
        logging.error(f"Error processing webhook: {e}")
        return "Error", 500

def determine_athlete_from_owner_id(owner_id):
    """
    Determine which athlete the owner_id belongs to.
    Returns athlete name or None if unknown.
    """
    return ATHLETE_MAPPING.get(str(owner_id))

async def fetch_activity_immediate(activity_id, athlete_name):
    """Immediately fetch a single activity (for new activities)"""
    try:
        logging.info(f"Fetching new activity {activity_id} for {athlete_name}")
        
        # Get detailed activity data
        detailed_data = await fetcher.api.get_detailed_activity(activity_id, athlete_name)
        
        if detailed_data:
            from strava_main import parse_strava_activity
            activity = parse_strava_activity(detailed_data, athlete_name)
            
            # Save to database
            new_count = fetcher.db.save_activities([activity])
            
            logging.info(f"Successfully fetched and saved activity {activity_id} for {athlete_name}")
            
            # Send notification email for new activities
            if new_count > 0:
                fetcher.notifier.send_email(
                    f"🚴‍♂️ New Strava Activity: {activity.name}",
                    f"""
                    <html><body>
                    <h2>New Activity Detected!</h2>
                    <p><strong>Activity:</strong> {activity.name}</p>
                    <p><strong>Date:</strong> {activity.start_date_local}</p>
                    <p><strong>Distance:</strong> {activity.distance/1000:.1f}km</p>
                    <p><strong>Moving Time:</strong> {activity.moving_time//60}min</p>
                    </body></html>
                    """
                )
                
        else:
            logging.error(f"Could not fetch detailed data for activity {activity_id}")
            
    except Exception as e:
        logging.error(f"Error fetching activity {activity_id}: {e}")

@app.route('/refresh-activities', methods=['POST'])
def manual_refresh():
    """Manual endpoint to trigger activity refresh"""
    try:
        process_scheduled_refreshes()
        return jsonify({"status": "success", "message": "Refresh processing initiated"})
    except Exception as e:
        logging.error(f"Error in manual refresh: {e}")
        return jsonify({"error": str(e)}), 500

def process_scheduled_refreshes():
    """Process activities that are due for refresh"""
    activities_to_refresh = refresh_manager.get_activities_due_for_refresh()
    
    if not activities_to_refresh:
        logging.info("No activities due for refresh")
        return
    
    logging.info(f"Processing {len(activities_to_refresh)} activities for refresh")
    
    for activity_id, athlete_name, refresh_type in activities_to_refresh:
        try:
            # Run the refresh
            asyncio.run(refresh_single_activity(activity_id, athlete_name))
            
            # Mark as completed
            refresh_manager.mark_refresh_completed(activity_id, refresh_type)
            
            logging.info(f"Completed {refresh_type} refresh for activity {activity_id}")
            
            # Rate limiting - small delay between refreshes
            time.sleep(2)
            
        except Exception as e:
            logging.error(f"Error refreshing activity {activity_id}: {e}")

async def refresh_single_activity(activity_id, athlete_name):
    """Refresh a single activity's data"""
    try:
        # Get updated detailed activity data
        detailed_data = await fetcher.api.get_detailed_activity(activity_id, athlete_name)
        
        if detailed_data:
            from strava_main import parse_strava_activity
            activity = parse_strava_activity(detailed_data, athlete_name)
            
            # Update in database (will replace existing due to UNIQUE constraint)
            fetcher.db.save_activities([activity])
            
            logging.info(f"Successfully refreshed activity {activity_id}")
        else:
            logging.error(f"Could not fetch updated data for activity {activity_id}")
            
    except Exception as e:
        logging.error(f"Error refreshing activity {activity_id}: {e}")

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'webhook_token_configured': bool(WEBHOOK_VERIFY_TOKEN != 'your_verify_token_here'),
        'athlete_mapping_configured': bool(ATHLETE_MAPPING)
    })

@app.route('/stats', methods=['GET'])
def webhook_stats():
    """Show webhook statistics"""
    conn = sqlite3.connect(config.db_path)
    cursor = conn.cursor()
    
    # Get recent webhook events
    cursor.execute('''
        SELECT COUNT(*) as total_events,
               SUM(CASE WHEN event_type = 'create' THEN 1 ELSE 0 END) as creates,
               SUM(CASE WHEN event_type = 'update' THEN 1 ELSE 0 END) as updates
        FROM webhook_events
        WHERE received_at >= datetime('now', '-7 days')
    ''')
    
    stats = cursor.fetchone()
    
    # Get pending refreshes
    cursor.execute('''
        SELECT COUNT(*) FROM activity_refresh_queue 
        WHERE completed_at IS NULL
    ''')
    
    pending_refreshes = cursor.fetchone()[0]
    
    conn.close()
    
    return jsonify({
        'webhook_events_last_7_days': {
            'total': stats[0],
            'creates': stats[1], 
            'updates': stats[2]
        },
        'pending_refreshes': pending_refreshes,
        'timestamp': datetime.now().isoformat()
    })

# Background refresh processor
def refresh_processor():
    """Background thread to process scheduled refreshes"""
    while True:
        try:
            process_scheduled_refreshes()
            time.sleep(300)  # Check every 5 minutes
        except Exception as e:
            logging.error(f"Error in refresh processor: {e}")
            time.sleep(60)  # Wait 1 minute on error

# Start background processor
refresh_thread = threading.Thread(target=refresh_processor, daemon=True)
refresh_thread.start()

if __name__ == '__main__':
    print("🚀 Starting Enhanced Strava Webhook Server")
    print(f"Webhook endpoint: http://dashboard.allkins.com/webhook")
    print(f"Health check: http://dashboard.allkins.com/health")
    print(f"Stats: http://dashboard.allkins.com/stats")
    print("Remember to update ATHLETE_MAPPING with your Strava athlete IDs!")
    
    # Run the Flask app
    app.run(host='0.0.0.0', port=int(os.getenv('WEBHOOK_PORT', 5000)), debug=False)