#!/usr/bin/env python3
"""
Strava Webhook Server
Handles incoming webhook notifications from Strava for new activities
"""

import os
import logging
import asyncio
from datetime import datetime
from flask import Flask, request, jsonify
from dotenv import load_dotenv
import threading
import time

# Import your main application
from strava_fetcher import StravaDataFetcher, Config

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

# Webhook verification token (set this when creating the webhook)
WEBHOOK_VERIFY_TOKEN = os.getenv('STRAVA_WEBHOOK_VERIFY_TOKEN', 'your_verify_token_here')

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
        
        # Check if this is an activity event
        if (event_data.get('object_type') == 'activity' and 
            event_data.get('aspect_type') == 'create'):
            
            activity_id = event_data.get('object_id')
            owner_id = event_data.get('owner_id')
            
            # Determine which athlete this belongs to
            athlete_name = determine_athlete_from_owner_id(owner_id)
            
            if athlete_name:
                logging.info(f"New activity {activity_id} for {athlete_name}, scheduling fetch")
                
                # Schedule a fetch for this athlete (with a small delay to ensure activity is fully processed)
                threading.Timer(30.0, lambda: asyncio.run(fetch_new_activity(athlete_name))).start()
            else:
                logging.warning(f"Unknown owner_id: {owner_id}")
        
        return "OK", 200
        
    except Exception as e:
        logging.error(f"Error processing webhook: {e}")
        return "Error", 500

def determine_athlete_from_owner_id(owner_id):
    """
    Determine which athlete the owner_id belongs to.
    You'll need to implement this based on your athlete data.
    One approach is to store athlete Strava IDs when you first authenticate them.
    """
    # This is a placeholder - you'll need to implement the mapping
    # You could store this in your database or config
    athlete_mapping = {
        # Add your actual Strava athlete IDs here
        # 'strava_athlete_id': 'athlete_name'
    }
    
    return athlete_mapping.get(str(owner_id))

async def fetch_new_activity(athlete_name):
    """Fetch new activities for a specific athlete"""
    try:
        logging.info(f"Fetching new activities for {athlete_name}")
        await fetcher.fetch_athlete_data(athlete_name)
        logging.info(f"Successfully fetched new activities for {athlete_name}")
    except Exception as e:
        logging.error(f"Error fetching new activities for {athlete_name}: {e}")
        fetcher.notifier.send_error_notification(f"Webhook fetch error for {athlete_name}: {str(e)}")

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'webhook_token_configured': bool(WEBHOOK_VERIFY_TOKEN != 'your_verify_token_here')
    })

@app.route('/trigger_fetch/<athlete_name>', methods=['POST'])
def manual_trigger_fetch(athlete_name):
    """Manual endpoint to trigger a fetch for an athlete (for testing)"""
    if athlete_name not in config.athletes:
        return jsonify({'error': f'Unknown athlete: {athlete_name}'}), 400
    
    try:
        # Run fetch in background
        threading.Timer(1.0, lambda: asyncio.run(fetch_new_activity(athlete_name))).start()
        
        return jsonify({
            'message': f'Fetch triggered for {athlete_name}',
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        logging.error(f"Error triggering manual fetch: {e}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    print("🚀 Starting Strava Webhook Server")
    print(f"Webhook endpoint: http://your-domain.com/webhook")
    print(f"Health check: http://your-domain.com/health")
    
    # Run the Flask app
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 5000)), debug=False)
