#!/usr/bin/env python3
"""
Modern webhook server for Strava events using the modular architecture.

This server handles Strava webhook events and processes them using the
refactored modular components.
"""

import asyncio
import json
import os
import sys
from datetime import datetime
from typing import Dict, Any, Optional

from flask import Flask, request, jsonify

# Import our modular components
from .utils.config import Config
from .utils.logging_config import setup_logging, get_logger, PerformanceTimer
from .utils.error_handling import (
    StravaDataFetcherError, ConfigurationError, DatabaseError, APIError,
    EmailError, handle_errors, safe_execute_async, ErrorCollector
)
from .utils.security import create_security_headers, SecurityValidator
from .api.strava_client import StravaAPIClient
from .database.manager import DatabaseManager
from .notifications.email_service import EmailService


class StravaWebhookServer:
    """Modern webhook server using modular architecture"""
    
    def __init__(self, config: Config):
        """
        Initialize the webhook server.
        
        Args:
            config: Application configuration
        """
        self.config = config
        self.logger = get_logger(__name__)
        
        # Initialize components
        self.db_manager = DatabaseManager(config.database)
        self.email_service = EmailService(config.email)
        
        # Track processed events to avoid duplicates
        self.processed_events = set()
        
        # Flask app
        self.app = Flask(__name__)
        self._setup_routes()
        
        self.logger.info("StravaWebhookServer initialized")
    
    def _setup_routes(self):
        """Setup Flask routes"""
        
        @self.app.route('/webhook', methods=['GET'])
        def webhook_challenge():
            """Handle Strava webhook verification challenge"""
            try:
                challenge = request.args.get('hub.challenge')
                verify_token = request.args.get('hub.verify_token')
                
                self.logger.info(f"Webhook challenge received")
                
                # Validate inputs
                if not challenge or not verify_token:
                    self.logger.warning("Missing challenge or verify token")
                    return jsonify({'error': 'Missing required parameters'}), 400
                
                # Sanitize inputs
                challenge = SecurityValidator.sanitize_string(challenge, max_length=100)
                verify_token = SecurityValidator.sanitize_string(verify_token, max_length=100)
                
                # Verify the token matches our configuration
                expected_token = os.getenv('STRAVA_WEBHOOK_VERIFY_TOKEN')
                if not expected_token or verify_token != expected_token:
                    self.logger.warning("Invalid verify token")
                    return jsonify({'error': 'Invalid verify token'}), 403
                
                self.logger.info("Webhook challenge verified successfully")
                response = jsonify({'hub.challenge': challenge})
                
                # Add security headers
                for header, value in create_security_headers().items():
                    response.headers[header] = value
                
                return response
                
            except Exception as e:
                self.logger.error(f"Webhook challenge error: {e}")
                return jsonify({'error': 'Challenge verification failed'}), 500
        
        @self.app.route('/webhook', methods=['POST'])
        def webhook_event():
            """Handle Strava webhook events"""
            try:
                # Validate content type
                if not request.is_json:
                    self.logger.warning("Webhook received with invalid content type")
                    return jsonify({'error': 'Content-Type must be application/json'}), 400
                
                event_data = request.get_json()
                
                if not event_data:
                    self.logger.warning("Received webhook with no JSON data")
                    return jsonify({'error': 'No JSON data'}), 400
                
                # Validate required fields
                required_fields = ['object_type', 'object_id', 'aspect_type', 'owner_id']
                missing_fields = [field for field in required_fields if field not in event_data]
                if missing_fields:
                    self.logger.warning(f"Missing required fields: {missing_fields}")
                    return jsonify({'error': f'Missing required fields: {missing_fields}'}), 400
                
                # Validate field types and values
                try:
                    object_id = int(event_data['object_id'])
                    owner_id = int(event_data['owner_id'])
                    
                    if not SecurityValidator.validate_athlete_id(owner_id):
                        self.logger.warning(f"Invalid owner_id: {owner_id}")
                        return jsonify({'error': 'Invalid owner_id'}), 400
                        
                except (ValueError, TypeError):
                    self.logger.warning("Invalid data types in webhook event")
                    return jsonify({'error': 'Invalid data types'}), 400
                
                # Log the event (without sensitive data)
                safe_event_data = {
                    'object_type': event_data.get('object_type'),
                    'aspect_type': event_data.get('aspect_type'),
                    'object_id': object_id,
                    'owner_id': owner_id
                }
                self.logger.info(f"Webhook event received: {json.dumps(safe_event_data)}")
                
                # Process the event asynchronously
                asyncio.create_task(self._process_webhook_event(event_data))
                
                response = jsonify({'status': 'received'})
                
                # Add security headers
                for header, value in create_security_headers().items():
                    response.headers[header] = value
                
                return response, 200
                
            except Exception as e:
                self.logger.error(f"Webhook event error: {e}")
                return jsonify({'error': 'Event processing failed'}), 500
        
        @self.app.route('/health', methods=['GET'])
        @handle_errors(default_return=(jsonify({'status': 'error', 'error': 'Health check failed'}), 500))
        def health_check():
            """Health check endpoint"""
            try:
                # Test database connection
                db_status = self.db_manager.test_connection()
                
                status = {
                    'status': 'healthy' if db_status else 'unhealthy',
                    'timestamp': datetime.utcnow().isoformat(),
                    'database': 'connected' if db_status else 'disconnected',
                    'processed_events': len(self.processed_events)
                }
                
                response = jsonify(status)
                
                # Add security headers
                for header, value in create_security_headers().items():
                    response.headers[header] = value
                
                return response, 200 if db_status else 503
                
            except (DatabaseError, ConfigurationError) as e:
                self.logger.error(f"Health check error: {e}")
                error_response = jsonify({
                    'status': 'error',
                    'error': 'Health check failed',  # Don't expose internal error details
                    'timestamp': datetime.utcnow().isoformat()
                })
                
                # Add security headers
                for header, value in create_security_headers().items():
                    error_response.headers[header] = value
                
                return error_response, 500
        
        @self.app.route('/stats', methods=['GET'])
        def stats():
            """Get webhook server statistics"""
            try:
                # Get recent activity count
                recent_activities = self.db_manager.get_recent_activities(limit=100)
                
                stats_data = {
                    'processed_events': len(self.processed_events),
                    'recent_activities': len(recent_activities),
                    'configured_athletes': len(self.config.athletes),
                    'uptime': datetime.utcnow().isoformat()
                }
                
                return jsonify(stats_data), 200
                
            except Exception as e:
                self.logger.error(f"Stats endpoint error: {e}")
                return jsonify({'error': str(e)}), 500
    
    async def _process_webhook_event(self, event_data: Dict[str, Any]):
        """
        Process a webhook event asynchronously.
        
        Args:
            event_data: Webhook event data from Strava
        """
        with PerformanceTimer("Process webhook event"):
            try:
                # Extract event details
                object_type = event_data.get('object_type')
                object_id = event_data.get('object_id')
                aspect_type = event_data.get('aspect_type')
                owner_id = event_data.get('owner_id')
                event_time = event_data.get('event_time')
                
                # Create unique event ID to avoid duplicates
                event_id = f"{object_type}_{object_id}_{aspect_type}_{owner_id}_{event_time}"
                
                if event_id in self.processed_events:
                    self.logger.info(f"Event already processed: {event_id}")
                    return
                
                self.logger.info(f"Processing webhook event: {event_id}")
                
                # Only process activity events
                if object_type != 'activity':
                    self.logger.info(f"Ignoring non-activity event: {object_type}")
                    self.processed_events.add(event_id)
                    return
                
                # Find the athlete configuration
                athlete_config = self.config.get_athlete_by_id(owner_id)
                if not athlete_config:
                    self.logger.warning(f"No configuration found for athlete ID: {owner_id}")
                    self.processed_events.add(event_id)
                    return
                
                # Process based on aspect type
                if aspect_type == 'create':
                    await self._handle_activity_create(athlete_config, object_id)
                elif aspect_type == 'update':
                    await self._handle_activity_update(athlete_config, object_id)
                elif aspect_type == 'delete':
                    await self._handle_activity_delete(athlete_config, object_id)
                else:
                    self.logger.warning(f"Unknown aspect type: {aspect_type}")
                
                # Mark event as processed
                self.processed_events.add(event_id)
                
                # Clean up old processed events (keep last 1000)
                if len(self.processed_events) > 1000:
                    # Remove oldest 100 events
                    old_events = list(self.processed_events)[:100]
                    for old_event in old_events:
                        self.processed_events.discard(old_event)
                
                self.logger.info(f"Successfully processed webhook event: {event_id}")
                
            except (APIError, DatabaseError, EmailError) as e:
                self.logger.error(f"Error processing webhook event: {e}")
                # Don't reraise - we want to mark the event as processed to avoid retries
            except Exception as e:
                self.logger.error(f"Unexpected error processing webhook event: {e}")
                # Don't reraise - we want to mark the event as processed to avoid retries
    
    async def _handle_activity_create(self, athlete_config, activity_id: int):
        """
        Handle activity creation event.
        
        Args:
            athlete_config: Athlete configuration
            activity_id: Strava activity ID
        """
        try:
            self.logger.info(f"Handling activity create: {activity_id} for {athlete_config.name}")
            
            async with StravaAPIClient(athlete_config) as client:
                # Fetch the specific activity
                activity = await client.fetch_activity_by_id(activity_id)
                
                if activity:
                    # Save to database
                    affected_rows = self.db_manager.save_activities([activity])
                    self.logger.info(f"Saved new activity {activity_id}: {affected_rows} rows affected")
                    
                    # Send notification email if configured
                    if athlete_config.email and self.email_service:
                        try:
                            success = self.email_service.send_activity_email(
                                activity=activity,
                                to_email=athlete_config.email
                            )
                            if success:
                                self.logger.info(f"Sent activity notification email for {activity_id}")
                            else:
                                self.logger.warning(f"Failed to send activity notification email for {activity_id}")
                        except Exception as e:
                            self.logger.error(f"Error sending activity notification: {e}")
                else:
                    self.logger.warning(f"Could not fetch activity {activity_id}")
                    
        except (APIError, DatabaseError, EmailError) as e:
            self.logger.error(f"Error handling activity create {activity_id}: {e}")
            # Don't reraise to avoid webhook retry loops
        except Exception as e:
            self.logger.error(f"Unexpected error handling activity create {activity_id}: {e}")
            # Don't reraise to avoid webhook retry loops
    
    async def _handle_activity_update(self, athlete_config, activity_id: int):
        """
        Handle activity update event.
        
        Args:
            athlete_config: Athlete configuration
            activity_id: Strava activity ID
        """
        try:
            self.logger.info(f"Handling activity update: {activity_id} for {athlete_config.name}")
            
            async with StravaAPIClient(athlete_config) as client:
                # Fetch the updated activity
                activity = await client.fetch_activity_by_id(activity_id)
                
                if activity:
                    # Update in database (save_activities handles upserts)
                    affected_rows = self.db_manager.save_activities([activity])
                    self.logger.info(f"Updated activity {activity_id}: {affected_rows} rows affected")
                else:
                    self.logger.warning(f"Could not fetch updated activity {activity_id}")
                    
        except (APIError, DatabaseError) as e:
            self.logger.error(f"Error handling activity update {activity_id}: {e}")
            # Don't reraise to avoid webhook retry loops
        except Exception as e:
            self.logger.error(f"Unexpected error handling activity update {activity_id}: {e}")
            # Don't reraise to avoid webhook retry loops
    
    async def _handle_activity_delete(self, athlete_config, activity_id: int):
        """
        Handle activity deletion event.
        
        Args:
            athlete_config: Athlete configuration
            activity_id: Strava activity ID
        """
        try:
            self.logger.info(f"Handling activity delete: {activity_id} for {athlete_config.name}")
            
            # Delete from database
            success = self.db_manager.delete_activity(activity_id)
            
            if success:
                self.logger.info(f"Deleted activity {activity_id} from database")
            else:
                self.logger.warning(f"Activity {activity_id} not found in database for deletion")
                
        except DatabaseError as e:
            self.logger.error(f"Error handling activity delete {activity_id}: {e}")
            # Don't reraise to avoid webhook retry loops
        except Exception as e:
            self.logger.error(f"Unexpected error handling activity delete {activity_id}: {e}")
            # Don't reraise to avoid webhook retry loops
    
    def run(self, host: str = '0.0.0.0', port: int = 5000, debug: bool = False):
        """
        Run the webhook server.
        
        Args:
            host: Host to bind to
            port: Port to bind to
            debug: Enable debug mode
        """
        self.logger.info(f"Starting webhook server on {host}:{port}")
        
        try:
            self.app.run(host=host, port=port, debug=debug)
        except Exception as e:
            self.logger.error(f"Error running webhook server: {e}")
            raise


def create_app():
    """Create Flask app for WSGI deployment"""
    try:
        # Setup logging
        setup_logging(log_level='INFO')
        logger = get_logger(__name__)
        
        # Load configuration
        try:
            config = Config.from_env()
            config.validate()
            logger.info("Configuration loaded for webhook server")
            
            # Create webhook server
            webhook_server = StravaWebhookServer(config)
            return webhook_server.app
            
        except ConfigurationError as e:
            print(f"Configuration error: {e}")
            sys.exit(1)
        except Exception as e:
            print(f"Failed to create webhook app: {e}")
            sys.exit(1)
    
    except Exception as e:
        print(f"Setup failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Strava Webhook Server')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to')
    parser.add_argument('--port', type=int, default=5000, help='Port to bind to')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    parser.add_argument('--log-level', default='INFO', 
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                       help='Set logging level')
    
    args = parser.parse_args()
    
    try:
        # Setup logging
        setup_logging(log_level=args.log_level)
        logger = get_logger(__name__)
        
        # Load configuration
        try:
            config = Config.from_env()
            config.validate()
            logger.info("Configuration loaded and validated")
            
            # Create and run webhook server
            webhook_server = StravaWebhookServer(config)
            webhook_server.run(host=args.host, port=args.port, debug=args.debug)
            
        except ConfigurationError as e:
            logger.error(f"Configuration error: {e}")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Webhook server setup failed: {e}")
            sys.exit(1)
        
    except KeyboardInterrupt:
        logger.info("Webhook server stopped by user")
    except Exception as e:
        logger.error(f"Webhook server failed: {e}")
        sys.exit(1)