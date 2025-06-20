#!/usr/bin/env python3
"""
Enhanced Strava Webhook Server with Activity Update Tracking and Email Notifications
Handles create, update events and schedules delayed refreshes
Sends immediate, weekly, monthly, and annual email notifications
"""

import os
import logging
import asyncio
import sqlite3
import json
import mysql.connector
from mysql.connector import Error
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, render_template_string
from dotenv import load_dotenv
import threading
import time
import aiohttp
from dataclasses import asdict
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import calendar

# Import your main application
from strava_main import StravaDataFetcher, Config, parse_strava_activity

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
WEBHOOK_VERIFY_TOKEN = os.getenv('WEBHOOK_VERIFY_TOKEN', 'your_verify_token_here')

# Athlete ID mapping - you'll need to populate this during initial setup
ATHLETE_MAPPING = {
    # Add your actual Strava athlete IDs here after first webhook
    # 'strava_athlete_id': 'athlete_name'
}

class EmailNotifier:
    """Enhanced email notifications for webhook events"""
    
    def __init__(self, config):
        self.config = config
        self.api_key = os.getenv("BREVO_API_KEY")
        self.from_email = os.getenv("BREVO_FROM_EMAIL", "dominic.allkins@gmail.com")
        self.from_name = os.getenv("BREVO_FROM_NAME", "Strava Webhook Server")
        self.api_url = "https://api.brevo.com/v3/smtp/email"
        
        # Athlete-specific email addresses (case-insensitive)
        self.athlete_emails = {
            'Dominic': self.config.notification_email,  # Main email from config
            'dominic': self.config.notification_email,  # Lowercase version
            'Clare': os.getenv('CLARE_EMAIL', 'clare@allkins.com'),
            'clare': os.getenv('CLARE_EMAIL', 'clare@allkins.com')  # Lowercase version
        }
        
        # Admin email for summaries (both emails)
        self.admin_emails = [
            self.config.notification_email,
            os.getenv('CLARE_EMAIL', 'clare@allkins.com')
        ]
    
    def format_number(self, number):
        """Format numbers with commas for thousands"""
        if number is None:
            return "0"
        return f"{number:,.0f}" if isinstance(number, (int, float)) else str(number)
    
    def get_calorie_equivalents(self, total_calories):
        """Calculate fun food/drink equivalents for burned calories"""
        if not total_calories or total_calories <= 0:
            return ""
        
        # Separate food and drinks with emojis
        foods = [
            {"name": "Big Macs", "emoji": "🍔", "calories": 550},
            {"name": "Bacon Sandwiches", "emoji": "🥓", "calories": 500},
            {"name": "Victoria Sponge Slices", "emoji": "🍰", "calories": 325},
            {"name": "Jam Doughnuts", "emoji": "🍩", "calories": 220},
        ]
        
        drinks = [
            {"name": "Bottles of Bollinger", "emoji": "🍾", "calories": 485},
            {"name": "Bottles of Coke", "emoji": "🥤", "calories": 197},
            {"name": "Bottles of Peroni", "emoji": "🍺", "calories": 142},
            {"name": "Skinny Flat Whites", "emoji": "☕️", "calories": 87},
        ]
        
        equivalents_html = """
        <div style="background: #fff8e1; padding: 20px; border-radius: 8px; margin: 20px 0;">
            <h3 style="margin-top: 0; color: #f57c00;">🍔 Calorie Equivalents</h3>
            <p style="margin-bottom: 15px; color: #666;">You burned enough calories for:</p>
        """
        
        # Food section
        food_items = []
        for food in foods:
            count = int(total_calories / food["calories"])
            if count > 0:
                food_items.append(f"""
                <div style="display: flex; align-items: center; padding: 8px; background: white; border-radius: 6px;">
                    <span style="font-size: 24px; margin-right: 10px;">{food["emoji"]}</span>
                    <span><strong>{self.format_number(count)}</strong> {food["name"]}</span>
                </div>
                """)
        
        if food_items:
            equivalents_html += """
            <h4 style="color: #f57c00; margin: 15px 0 10px 0;">🍽️ Food</h4>
            <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 10px; margin-bottom: 20px;">
            """
            equivalents_html += "".join(food_items)
            equivalents_html += "</div>"
        
        # Drinks section
        drink_items = []
        for drink in drinks:
            count = int(total_calories / drink["calories"])
            if count > 0:
                drink_items.append(f"""
                <div style="display: flex; align-items: center; padding: 8px; background: white; border-radius: 6px;">
                    <span style="font-size: 24px; margin-right: 10px;">{drink["emoji"]}</span>
                    <span><strong>{self.format_number(count)}</strong> {drink["name"]}</span>
                </div>
                """)
        
        if drink_items:
            equivalents_html += """
            <h4 style="color: #f57c00; margin: 15px 0 10px 0;">🥤 Drinks</h4>
            <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 10px;">
            """
            equivalents_html += "".join(drink_items)
            equivalents_html += "</div>"
        
        equivalents_html += """
        </div>
        """
        
        return equivalents_html
    
    def send_email(self, subject: str, body: str, recipient_emails=None):
        """Send email notification using Brevo API"""
        if not self.api_key:
            logging.error("No Brevo API key configured, skipping email")
            return False
        
        # Use provided emails or default to admin emails
        if recipient_emails is None:
            recipient_emails = self.admin_emails
        elif isinstance(recipient_emails, str):
            recipient_emails = [recipient_emails]
        
        # Filter out None/empty emails
        recipient_emails = [email for email in recipient_emails if email]
        
        if not recipient_emails:
            logging.error("No valid recipient emails configured")
            return False
        
        try:
            import requests
            
            headers = {
                "accept": "application/json",
                "content-type": "application/json",
                "api-key": self.api_key
            }
            
            # Convert email addresses to Brevo format
            to_emails = [{"email": email} for email in recipient_emails]
            
            data = {
                "sender": {"name": self.from_name, "email": self.from_email},
                "to": to_emails,
                "subject": subject,
                "htmlContent": body
            }
            
            logging.info(f"Sending email to {recipient_emails} with subject: {subject}")
            logging.info(f"Email data: sender={self.from_email}, recipients={len(to_emails)}")
            
            response = requests.post(self.api_url, json=data, headers=headers)
            
            logging.info(f"Brevo API response: {response.status_code}")
            logging.info(f"Brevo API response body: {response.text}")
            
            if response.status_code == 201:
                logging.info(f"Email sent successfully to {recipient_emails}: {subject}")
                return True
            else:
                logging.error(f"Failed to send email: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logging.error(f"Error sending email: {e}")
            return False
    
    def get_activity_stats(self, athlete_name=None, start_date=None, end_date=None):
        """Get activity statistics for a date range"""
        try:
            conn = refresh_manager.get_connection()
            cursor = conn.cursor()
            
            where_conditions = ["sport_type IN ('Ride', 'VirtualRide')"]
            params = []
            
            if athlete_name:
                # Make athlete name lookup case-insensitive
                where_conditions.append("LOWER(athlete_name) = LOWER(%s)")
                params.append(athlete_name)
            
            if start_date:
                where_conditions.append("DATE(start_date_local) >= %s")
                params.append(start_date)
                logging.info(f"DEBUG: start_date filter = {start_date} (date-only)")
            
            if end_date:
                where_conditions.append("DATE(start_date_local) <= %s")
                params.append(end_date)
                logging.info(f"DEBUG: end_date filter = {end_date} (date-only)")
            
            where_clause = " AND ".join(where_conditions)
            
            # Debug query - let's see what activities are being included
            debug_query = f'''
                SELECT 
                    id,
                    athlete_name,
                    name,
                    start_date_local,
                    distance/1000 as distance_km,
                    sport_type
                FROM strava_activities
                WHERE {where_clause}
                ORDER BY start_date_local DESC
            '''
            
            logging.info(f"DEBUG: Executing query with params: {params}")
            logging.info(f"DEBUG: Full query: {debug_query}")
            cursor.execute(debug_query, params)
            debug_results = cursor.fetchall()
            
            logging.info(f"DEBUG: Found {len(debug_results)} activities for {athlete_name}:")
            for row in debug_results:
                logging.info(f"  - ID: {row[0]}, {row[2]} on {row[3]}: {row[4]:.1f}km ({row[5]})")
            
            # Main stats query - also case-insensitive
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
            
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            stats = {}
            for row in results:
                athlete, rides, distance, elevation, calories, moving_time = row
                stats[athlete.lower()] = {  # Use lowercase key for consistency
                    'rides': rides,
                    'distance_km': round((distance or 0) / 1000, 1),
                    'elevation_m': round(elevation or 0),
                    'calories': round(calories or 0),
                    'moving_time_hours': round((moving_time or 0) / 3600, 1)
                }
                logging.info(f"DEBUG: Stats for {athlete}: {rides} rides, {stats[athlete.lower()]['distance_km']}km")
            
            cursor.close()
            conn.close()
            
            return stats
            
        except Error as e:
            logging.error(f"Error getting activity stats: {e}")
            return {}
    
    def get_activity_details(self, activity_id):
        """Get details for a specific activity"""
        try:
            conn = refresh_manager.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT athlete_name, name, start_date_local, distance, 
                       total_elevation_gain, calories, type, sport_type
                FROM strava_activities 
                WHERE id = %s
            ''', (activity_id,))
            
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            
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
            logging.error(f"Error getting activity details: {e}")
            return None
    
    def send_immediate_notification(self, activity_id, event_type):
        """Send immediate notification for new/updated ride to specific athlete"""
        try:
            activity = self.get_activity_details(activity_id)
            if not activity:
                logging.warning(f"Could not get activity details for {activity_id}")
                return
            
            # Get athlete-specific email (case-insensitive lookup)
            athlete_name = activity['athlete_name']
            athlete_email = None
            
            # Try exact match first, then case-insensitive
            for name, email in self.athlete_emails.items():
                if name.lower() == athlete_name.lower():
                    athlete_email = email
                    break
            
            if not athlete_email:
                logging.warning(f"No email configured for athlete {athlete_name} (tried case-insensitive)")
                logging.info(f"Available athlete emails: {list(self.athlete_emails.keys())}")
                return
            
            # Get 7-day stats for this athlete (last 7 days including today)
            # Use date-only comparison to avoid time-of-day issues
            today = datetime.now().date()
            seven_days_ago = today - timedelta(days=6)  # 6 days ago + today = 7 days
            week_stats = self.get_activity_stats(
                athlete_name=activity['athlete_name'],
                start_date=seven_days_ago
            )
            
            # Get athlete stats with flexible lookup
            athlete_week_stats = None
            athlete_name_variations = [
                activity['athlete_name'],  # Exact match
                activity['athlete_name'].lower(),  # Lowercase
                activity['athlete_name'].capitalize(),  # Capitalized
            ]
            
            for name_variant in athlete_name_variations:
                if name_variant in week_stats:
                    athlete_week_stats = week_stats[name_variant]
                    break
            
            # Fallback to empty stats if nothing found
            if not athlete_week_stats:
                athlete_week_stats = {'rides': 0, 'distance_km': 0, 'elevation_m': 0, 'calories': 0}
            
            # Debug logging for stats lookup
            logging.info(f"DEBUG: Looking for stats for '{activity['athlete_name']}'")
            logging.info(f"DEBUG: Available stats keys: {list(week_stats.keys())}")
            logging.info(f"DEBUG: Found stats: {athlete_week_stats}")
            
            # Determine event emoji and title
            event_emoji = "🆕" if event_type == "create" else "🔄"
            event_title = "New Ride" if event_type == "create" else "Updated Ride"
            
            # Personalized subject line
            subject = f"{event_emoji} {activity['name']}"
            
            body = f"""
            <html>
            <body style="font-family: Arial, sans-serif; color: #333;">
                <h2 style="color: #FC4C02;">🚴‍♂️ {event_title} - {activity['athlete_name']}!</h2>
                
                <div style="background: #f8f9fa; padding: 20px; border-radius: 8px; margin: 20px 0;">
                    <h3 style="margin-top: 0; color: #FC4C02;">📍 Activity Details</h3>
                    <p><strong>Activity:</strong> {activity['name']}</p>
                    <p><strong>Date:</strong> {activity['date'].strftime('%A, %B %d, %Y at %I:%M %p') if activity['date'] else 'Unknown'}</p>
                    <p><strong>Distance:</strong> {self.format_number(activity['distance_km'])} km</p>
                    <p><strong>Elevation:</strong> {self.format_number(activity['elevation_m'])} m</p>
                    <p><strong>Calories:</strong> {self.format_number(activity['calories'])}</p>
                </div>
                
                <div style="background: #e3f2fd; padding: 20px; border-radius: 8px; margin: 20px 0;">
                    <h3 style="margin-top: 0; color: #1976d2;">📊 Your Last 7 Days</h3>
                    <p><strong>Total Rides:</strong> {self.format_number(athlete_week_stats['rides'])}</p>
                    <p><strong>Total Distance:</strong> {self.format_number(athlete_week_stats['distance_km'])} km</p>
                    <p><strong>Total Elevation:</strong> {self.format_number(athlete_week_stats['elevation_m'])} m</p>
                    <p><strong>Total Calories:</strong> {self.format_number(athlete_week_stats['calories'])}</p>
                </div>
                
                {self.get_calorie_equivalents(athlete_week_stats['calories'])}
                
                <p style="color: #666; font-size: 12px; margin-top: 30px;">
                    <em>🤖 Strava Webhook Server - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</em>
                </p>
            </body>
            </html>
            """
            
            # Send to specific athlete
            self.send_email(subject, body, athlete_email)
            logging.info(f"Sent immediate notification to {activity['athlete_name']} ({athlete_email}) for {event_type} of activity {activity_id}")
            
        except Exception as e:
            logging.error(f"Error sending immediate notification: {e}")
    
    def send_weekly_summary(self):
        """Send weekly summary every Monday"""
        try:
            # Get last week's date range (Monday to Sunday) - using date-only
            today = datetime.now().date()
            days_since_monday = today.weekday()
            last_monday = today - timedelta(days=days_since_monday + 7)
            last_sunday = last_monday + timedelta(days=6)
            
            logging.info(f"DEBUG Weekly: Today={today}, Last Monday={last_monday}, Last Sunday={last_sunday}")
            
            # Get stats for the week using date-only comparison
            week_stats = self.get_activity_stats(
                start_date=last_monday,
                end_date=last_sunday
            )
            
            if not week_stats:
                logging.info("No cycling activities for weekly summary")
                return
            
            subject = f"🚴‍♂️ Weekly Cycling Summary - {last_monday.strftime('%b %d')} to {last_sunday.strftime('%b %d, %Y')}"
            
            # Build athlete stats with individual calorie equivalents
            athlete_sections = ""
            total_rides = 0
            total_distance = 0
            total_elevation = 0
            total_calories = 0
            
            for athlete, stats in week_stats.items():
                total_rides += stats['rides']
                total_distance += stats['distance_km']
                total_elevation += stats['elevation_m']
                total_calories += stats['calories']
                
                athlete_sections += f"""
                <div style="background: #f8f9fa; padding: 15px; border-radius: 8px; margin: 10px 0;">
                    <h4 style="margin-top: 0; color: #FC4C02;">🚴‍♂️ {athlete.title()}</h4>
                    <p><strong>Rides:</strong> {self.format_number(stats['rides'])}</p>
                    <p><strong>Distance:</strong> {self.format_number(stats['distance_km'])} km</p>
                    <p><strong>Elevation:</strong> {self.format_number(stats['elevation_m'])} m</p>
                    <p><strong>Calories:</strong> {self.format_number(stats['calories'])}</p>
                    <p><strong>Time:</strong> {self.format_number(stats['moving_time_hours'])} hours</p>
                    
                    {self.get_calorie_equivalents(stats['calories'])}
                </div>
                """
            
            body = f"""
            <html>
            <body style="font-family: Arial, sans-serif; color: #333;">
                <h2 style="color: #FC4C02;">🚴‍♂️ Weekly Cycling Summary</h2>
                <p><strong>Week of:</strong> {last_monday.strftime('%B %d')} - {last_sunday.strftime('%B %d, %Y')}</p>
                
                <div style="background: #e8f5e8; padding: 20px; border-radius: 8px; margin: 20px 0;">
                    <h3 style="margin-top: 0; color: #2e7d32;">🏆 Combined Totals</h3>
                    <p><strong>Total Rides:</strong> {self.format_number(total_rides)}</p>
                    <p><strong>Total Distance:</strong> {self.format_number(total_distance)} km</p>
                    <p><strong>Total Elevation:</strong> {self.format_number(total_elevation)} m</p>
                    <p><strong>Total Calories:</strong> {self.format_number(total_calories)}</p>
                </div>
                
                <h3 style="color: #FC4C02;">👥 Individual Performance</h3>
                {athlete_sections}
                
                <p style="color: #666; font-size: 12px; margin-top: 30px;">
                    <em>🤖 Weekly Summary - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</em>
                </p>
            </body>
            </html>
            """
            
            # Send to both admin emails
            self.send_email(subject, body)
            logging.info(f"Sent weekly summary for {last_monday} to {last_sunday}")
            
        except Exception as e:
            logging.error(f"Error sending weekly summary: {e}")
    
    def send_monthly_summary(self):
        """Send monthly summary on 1st of each month"""
        try:
            # Get last month's date range - using date-only
            today = datetime.now().date()
            first_of_this_month = today.replace(day=1)
            last_month_end = first_of_this_month - timedelta(days=1)
            last_month_start = last_month_end.replace(day=1)
            
            logging.info(f"DEBUG Monthly: Today={today}, Last month start={last_month_start}, Last month end={last_month_end}")
            
            # Get stats for the month using date-only comparison
            month_stats = self.get_activity_stats(
                start_date=last_month_start,
                end_date=last_month_end
            )
            
            if not month_stats:
                logging.info("No cycling activities for monthly summary")
                return
            
            month_name = last_month_end.strftime('%B %Y')
            subject = f"🚴‍♂️ Monthly Cycling Summary - {month_name}"
            
            # Build athlete stats
            athlete_sections = ""
            total_rides = 0
            total_distance = 0
            total_elevation = 0
            total_calories = 0
            
            for athlete, stats in month_stats.items():
                total_rides += stats['rides']
                total_distance += stats['distance_km']
                total_elevation += stats['elevation_m']
                total_calories += stats['calories']
                
                avg_distance = stats['distance_km'] / stats['rides'] if stats['rides'] > 0 else 0
                
                athlete_sections += f"""
                <div style="background: #f8f9fa; padding: 15px; border-radius: 8px; margin: 10px 0;">
                    <h4 style="margin-top: 0; color: #FC4C02;">🚴‍♂️ {athlete.title()}</h4>
                    <p><strong>Rides:</strong> {self.format_number(stats['rides'])}</p>
                    <p><strong>Distance:</strong> {self.format_number(stats['distance_km'])} km</p>
                    <p><strong>Avg Distance:</strong> {self.format_number(avg_distance)} km</p>
                    <p><strong>Elevation:</strong> {self.format_number(stats['elevation_m'])} m</p>
                    <p><strong>Calories:</strong> {self.format_number(stats['calories'])}</p>
                    <p><strong>Time:</strong> {self.format_number(stats['moving_time_hours'])} hours</p>
                    
                    {self.get_calorie_equivalents(stats['calories'])}
                </div>
                """
            
            body = f"""
            <html>
            <body style="font-family: Arial, sans-serif; color: #333;">
                <h2 style="color: #FC4C02;">🚴‍♂️ Monthly Cycling Summary</h2>
                <p><strong>Month:</strong> {month_name}</p>
                
                <div style="background: #e3f2fd; padding: 20px; border-radius: 8px; margin: 20px 0;">
                    <h3 style="margin-top: 0; color: #1976d2;">🏆 Combined Monthly Totals</h3>
                    <p><strong>Total Rides:</strong> {self.format_number(total_rides)}</p>
                    <p><strong>Total Distance:</strong> {self.format_number(total_distance)} km</p>
                    <p><strong>Total Elevation:</strong> {self.format_number(total_elevation)} m</p>
                    <p><strong>Total Calories:</strong> {self.format_number(total_calories)}</p>
                    <p><strong>Average per Ride:</strong> {self.format_number(total_distance / total_rides if total_rides > 0 else 0)} km</p>
                </div>
                
                <h3 style="color: #FC4C02;">👥 Individual Performance</h3>
                {athlete_sections}
                
                <p style="color: #666; font-size: 12px; margin-top: 30px;">
                    <em>🤖 Monthly Summary - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</em>
                </p>
            </body>
            </html>
            """
            
            # Send to both admin emails
            self.send_email(subject, body)
            logging.info(f"Sent monthly summary for {month_name}")
            
        except Exception as e:
            logging.error(f"Error sending monthly summary: {e}")
    
    def send_annual_summary(self):
        """Send annual summary on January 1st"""
        try:
            # Get last year's date range - using date-only
            today = datetime.now().date()
            last_year = today.year - 1
            year_start = datetime(last_year, 1, 1).date()
            year_end = datetime(last_year, 12, 31).date()
            
            logging.info(f"DEBUG Annual: Today={today}, Year start={year_start}, Year end={year_end}")
            
            # Get stats for the year using date-only comparison
            year_stats = self.get_activity_stats(
                start_date=year_start,
                end_date=year_end
            )
            
            if not year_stats:
                logging.info("No cycling activities for annual summary")
                return
            
            subject = f"🚴‍♂️ Annual Cycling Summary - {last_year}"
            
            # Build athlete stats with more detailed metrics
            athlete_sections = ""
            total_rides = 0
            total_distance = 0
            total_elevation = 0
            total_calories = 0
            total_time = 0
            
            for athlete, stats in year_stats.items():
                total_rides += stats['rides']
                total_distance += stats['distance_km']
                total_elevation += stats['elevation_m']
                total_calories += stats['calories']
                total_time += stats['moving_time_hours']
                
                avg_distance = stats['distance_km'] / stats['rides'] if stats['rides'] > 0 else 0
                rides_per_week = stats['rides'] / 52  # Approximate
                
                athlete_sections += f"""
                <div style="background: #f8f9fa; padding: 20px; border-radius: 8px; margin: 15px 0;">
                    <h4 style="margin-top: 0; color: #FC4C02;">🚴‍♂️ {athlete.title()}</h4>
                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 10px;">
                        <div>
                            <p><strong>Total Rides:</strong> {self.format_number(stats['rides'])}</p>
                            <p><strong>Total Distance:</strong> {self.format_number(stats['distance_km'])} km</p>
                            <p><strong>Total Elevation:</strong> {self.format_number(stats['elevation_m'])} m</p>
                        </div>
                        <div>
                            <p><strong>Avg Distance:</strong> {self.format_number(avg_distance)} km</p>
                            <p><strong>Rides/Week:</strong> {self.format_number(rides_per_week)}</p>
                            <p><strong>Total Time:</strong> {self.format_number(stats['moving_time_hours'])} hrs</p>
                        </div>
                    </div>
                    <p><strong>Calories Burned:</strong> {self.format_number(stats['calories'])}</p>
                    
                    {self.get_calorie_equivalents(stats['calories'])}
                </div>
                """
            
            # Calculate some fun stats
            distance_around_earth = total_distance / 40075  # Earth's circumference
            everest_climbs = total_elevation / 8849  # Height of Everest
            
            body = f"""
            <html>
            <body style="font-family: Arial, sans-serif; color: #333;">
                <h2 style="color: #FC4C02;">🚴‍♂️ Annual Cycling Summary</h2>
                <p><strong>Year:</strong> {last_year}</p>
                
                <div style="background: #fff3e0; padding: 20px; border-radius: 8px; margin: 20px 0;">
                    <h3 style="margin-top: 0; color: #f57c00;">🏆 Epic Year in Numbers</h3>
                    <p><strong>Total Rides:</strong> {self.format_number(total_rides)}</p>
                    <p><strong>Total Distance:</strong> {self.format_number(total_distance)} km</p>
                    <p><strong>Total Elevation:</strong> {self.format_number(total_elevation)} m</p>
                    <p><strong>Total Time:</strong> {self.format_number(total_time)} hours</p>
                    <p><strong>Total Calories:</strong> {self.format_number(total_calories)}</p>
                </div>
                
                <div style="background: #e8f5e8; padding: 20px; border-radius: 8px; margin: 20px 0;">
                    <h3 style="margin-top: 0; color: #2e7d32;">🌍 Fun Comparisons</h3>
                    <p><strong>Around the Earth:</strong> {distance_around_earth:.1f} times</p>
                    <p><strong>Mount Everest:</strong> Climbed {everest_climbs:.1f} times</p>
                    <p><strong>Average per Week:</strong> {self.format_number(total_rides / 52)} rides, {self.format_number(total_distance / 52)} km</p>
                </div>
                
                <h3 style="color: #FC4C02;">👥 Individual Achievements</h3>
                {athlete_sections}
                
                <div style="background: #f3e5f5; padding: 20px; border-radius: 8px; margin: 20px 0;">
                    <h3 style="margin-top: 0; color: #7b1fa2;">🎉 What an Amazing Year!</h3>
                    <p>You've accomplished something incredible. Every ride, every kilometer, every climb - it all adds up to this amazing journey. Here's to an even better {today.year}!</p>
                </div>
                
                <p style="color: #666; font-size: 12px; margin-top: 30px;">
                    <em>🤖 Annual Summary - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</em>
                </p>
            </body>
            </html>
            """
            
            # Send to both admin emails
            self.send_email(subject, body)
            logging.info(f"Sent annual summary for {last_year}")
            
        except Exception as e:
            logging.error(f"Error sending annual summary: {e}")

# Initialize email notifier
email_notifier = EmailNotifier(config)

class ActivityRefreshManager:
    """Manages activity refresh scheduling and processing - MariaDB version"""
    
    def __init__(self):
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'database': os.getenv('DB_NAME'),
            'charset': 'utf8mb4',
            'autocommit': True
        }
        
    def get_connection(self):
        """Get database connection"""
        return mysql.connector.connect(**self.db_config)

    def get_athlete_id_from_name(self, athlete_name):
        """Get athlete_id from athlete_name - simple lookup"""
        athlete_mappings = {
            'Dominic': int(os.getenv('ATHLETE_ID_DOMINIC', 0)),
            'Clare': int(os.getenv('ATHLETE_ID_CLARE', 0)),
        }
        return athlete_mappings.get(athlete_name)

    async def refresh_access_token(self, athlete_name):
        """Refresh access token for an athlete"""
        try:
            # Get refresh token and client credentials
            refresh_token = None
            if athlete_name.lower() == 'dominic':
                refresh_token = os.getenv('DOMINIC_REFRESH_TOKEN')
            elif athlete_name.lower() == 'clare':
                refresh_token = os.getenv('CLARE_REFRESH_TOKEN')
            
            if not refresh_token:
                logging.error(f"No refresh token for {athlete_name}")
                return False
            
            client_id = os.getenv('STRAVA_CLIENT_ID')
            client_secret = os.getenv('STRAVA_CLIENT_SECRET')
            
            if not client_id or not client_secret:
                logging.error("Missing Strava client credentials")
                return False
            
            # Make token refresh request
            data = {
                'client_id': client_id,
                'client_secret': client_secret,
                'refresh_token': refresh_token,
                'grant_type': 'refresh_token'
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post('https://www.strava.com/oauth/token', data=data) as response:
                    if response.status == 200:
                        token_data = await response.json()
                        
                        # Update environment variables in memory
                        if athlete_name.lower() == 'dominic':
                            os.environ['DOMINIC_ACCESS_TOKEN'] = token_data['access_token']
                            os.environ['DOMINIC_REFRESH_TOKEN'] = token_data['refresh_token']
                            os.environ['DOMINIC_TOKEN_EXPIRES'] = str(token_data['expires_at'])
                        elif athlete_name.lower() == 'clare':
                            os.environ['CLARE_ACCESS_TOKEN'] = token_data['access_token']
                            os.environ['CLARE_REFRESH_TOKEN'] = token_data['refresh_token']
                            os.environ['CLARE_TOKEN_EXPIRES'] = str(token_data['expires_at'])
                        
                        logging.info(f"Successfully refreshed token for {athlete_name}")
                        return True
                    else:
                        logging.error(f"Failed to refresh token for {athlete_name}: {response.status}")
                        return False
                        
        except Exception as e:
            logging.error(f"Error refreshing token for {athlete_name}: {e}")
            return False

    def activity_exists(self, activity_id, athlete_id):
        """Check if activity exists in database"""
        try:
            connection = mysql.connector.connect(**self.db_config)
            cursor = connection.cursor()
            
            query = "SELECT COUNT(*) FROM strava_activities WHERE id = %s AND athlete_id = %s"
            cursor.execute(query, (activity_id, athlete_id))
            
            count = cursor.fetchone()[0]
            
            cursor.close()
            connection.close()
            
            return count > 0
            
        except Exception as e:
            logging.error(f"Error checking if activity exists: {e}")
            return False

    def fetch_and_save_activity(self, activity_id, athlete_id, athlete_name):
        """Fetch missing activity from Strava and save to database with automatic token refresh"""
        try:
            async def fetch_activity():
                # Get athlete's access token from environment
                access_token = None
                expires_at = 0
                
                if athlete_name.lower() == 'dominic':
                    access_token = os.getenv('DOMINIC_ACCESS_TOKEN')
                    expires_at = int(os.getenv('DOMINIC_TOKEN_EXPIRES', '0'))
                elif athlete_name.lower() == 'clare':
                    access_token = os.getenv('CLARE_ACCESS_TOKEN')
                    expires_at = int(os.getenv('CLARE_TOKEN_EXPIRES', '0'))
                
                if not access_token:
                    logging.error(f"No access token for {athlete_name}")
                    return None
                
                # Check if token is expired or will expire soon (refresh 1 hour early)
                current_time = int(time.time())
                if current_time >= (expires_at - 3600):
                    logging.info(f"Token for {athlete_name} is expired or expires soon, refreshing...")
                    success = await self.refresh_access_token(athlete_name)
                    if not success:
                        logging.error(f"Failed to refresh token for {athlete_name}")
                        return None
                    
                    # Get the new token
                    if athlete_name.lower() == 'dominic':
                        access_token = os.getenv('DOMINIC_ACCESS_TOKEN')
                    elif athlete_name.lower() == 'clare':
                        access_token = os.getenv('CLARE_ACCESS_TOKEN')
                
                # Fetch activity from Strava API
                headers = {'Authorization': f'Bearer {access_token}'}
                url = f"https://www.strava.com/api/v3/activities/{activity_id}"
                
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, headers=headers) as response:
                        if response.status == 200:
                            return await response.json()
                        elif response.status == 401:
                            # Token still invalid, try refreshing once more
                            logging.warning(f"Got 401 for {athlete_name}, attempting token refresh...")
                            success = await self.refresh_access_token(athlete_name)
                            if success:
                                # Retry with new token
                                if athlete_name.lower() == 'dominic':
                                    access_token = os.getenv('DOMINIC_ACCESS_TOKEN')
                                elif athlete_name.lower() == 'clare':
                                    access_token = os.getenv('CLARE_ACCESS_TOKEN')
                                
                                headers['Authorization'] = f'Bearer {access_token}'
                                async with session.get(url, headers=headers) as retry_response:
                                    if retry_response.status == 200:
                                        return await retry_response.json()
                                    else:
                                        logging.error(f"Still failed after token refresh: {retry_response.status}")
                                        return None
                            else:
                                logging.error(f"Token refresh failed for {athlete_name}")
                                return None
                        else:
                            logging.error(f"Failed to fetch activity {activity_id}: {response.status}")
                            return None
            
            # Run the async function
            activity_data = asyncio.run(fetch_activity())
            
            if activity_data:
                # Parse and save the activity
                activity = parse_strava_activity(activity_data, athlete_name, athlete_id)
                
                # Save to database
                connection = mysql.connector.connect(**self.db_config)
                cursor = connection.cursor()
                
                # Convert to dict for insertion
                activity_dict = asdict(activity)

                # Fix datetime formats - remove 'Z' and convert to MySQL format
                if activity_dict['start_date_local'] and activity_dict['start_date_local'].endswith('Z'):
                    activity_dict['start_date_local'] = activity_dict['start_date_local'][:-1].replace('T', ' ')

                if activity_dict['start_date'] and activity_dict['start_date'].endswith('Z'):
                    activity_dict['start_date'] = activity_dict['start_date'][:-1].replace('T', ' ')

                # Handle JSON fields
                start_latlng_json = json.dumps(activity_dict['start_latlng']) if activity_dict['start_latlng'] else None
                end_latlng_json = json.dumps(activity_dict['end_latlng']) if activity_dict['end_latlng'] else None
                
                # Updated SQL to match your actual database schema
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
                        updated_at = NOW()
                '''
                
                # Values tuple matching the SQL above
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
                
                cursor.close()
                connection.close()
                
                logging.info(f"Successfully fetched and saved missing activity {activity_id} for {athlete_name}")
                return True
            
        except Exception as e:
            logging.error(f"Error fetching and saving activity {activity_id}: {e}")
            return False

    def add_webhook_event(self, activity_id, athlete_id, event_type, aspect_type, raw_data):
        """Log webhook event for audit trail - now using athlete_id"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO webhook_events 
                (activity_id, athlete_id, event_type, aspect_type, raw_data)
                VALUES (%s, %s, %s, %s, %s)
            ''', (activity_id, athlete_id, event_type, aspect_type, json.dumps(raw_data)))
            
            cursor.close()
            conn.close()
            
        except Error as e:
            logging.error(f"Error adding webhook event: {e}")

    def get_activities_due_for_refresh(self, limit=10):
        """Get activities that are due for refresh"""
        try:
            connection = mysql.connector.connect(**self.db_config)
            cursor = connection.cursor(dictionary=True)
            
            # Query for activities that need refreshing - using athlete_id now
            query = """
            SELECT athlete_id, id, updated_at, athlete_name
            FROM strava_activities 
            WHERE (updated_at < DATE_SUB(NOW(), INTERVAL 24 HOUR) OR updated_at IS NULL)
            AND athlete_id IS NOT NULL
            ORDER BY updated_at ASC
            LIMIT %s
            """
            
            cursor.execute(query, (limit,))
            activities = cursor.fetchall()
            
            cursor.close()
            connection.close()
            
            return activities
            
        except Exception as e:
            print(f"Error getting activities due for refresh: {e}")
            return []

    def mark_activity_refreshed(self, activity_id):
        """Mark an activity as refreshed"""
        try:
            connection = mysql.connector.connect(**self.db_config)
            cursor = connection.cursor()
            
            query = "UPDATE strava_activities SET updated_at = NOW() WHERE id = %s"
            cursor.execute(query, (activity_id,))
            
            cursor.close()
            connection.close()
            
        except Exception as e:
            print(f"Error marking activity as refreshed: {e}")

    def schedule_activity_refresh(self, athlete_id, activity_id):
        """Schedule an activity for refresh"""
        try:
            connection = mysql.connector.connect(**self.db_config)
            cursor = connection.cursor()
            
            # Use athlete_id instead of athlete_name
            query = """
            UPDATE strava_activities 
            SET needs_refresh = TRUE, updated_at = NOW() 
            WHERE id = %s AND athlete_id = %s
            """
            cursor.execute(query, (activity_id, athlete_id))
            
            cursor.close()
            connection.close()
            
        except Exception as e:
            print(f"Error scheduling activity refresh: {e}")

    def schedule_delayed_refresh(self, activity_id, athlete_name, refresh_type='webhook_delayed', delay_hours=1.5):
        """Schedule an activity for delayed refresh"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            from datetime import datetime, timedelta
            now = datetime.now()
            scheduled_time = now + timedelta(hours=delay_hours)
            
            # Get athlete_id from athlete_name
            athlete_id = self.get_athlete_id_from_name(athlete_name)
            
            # Check if athlete_id column exists in activity_refresh_queue
            try:
                cursor.execute('''
                    INSERT INTO activity_refresh_queue
                    (activity_id, athlete_name, athlete_id, webhook_received_at, scheduled_refresh_at, refresh_type)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        webhook_received_at = VALUES(webhook_received_at),
                        scheduled_refresh_at = VALUES(scheduled_refresh_at)
                ''', (activity_id, athlete_name, athlete_id, now, scheduled_time, refresh_type))
            except Error as e:
                if "Unknown column 'athlete_id'" in str(e):
                    # Fallback to original schema without athlete_id
                    logging.warning("athlete_id column missing in activity_refresh_queue, using fallback")
                    cursor.execute('''
                        INSERT INTO activity_refresh_queue
                        (activity_id, athlete_name, webhook_received_at, scheduled_refresh_at, refresh_type)
                        VALUES (%s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            webhook_received_at = VALUES(webhook_received_at),
                            scheduled_refresh_at = VALUES(scheduled_refresh_at)
                    ''', (activity_id, athlete_name, now, scheduled_time, refresh_type))
                else:
                    raise e
            
            # Mark activity as needing refresh - using athlete_id
            cursor.execute('''
                UPDATE strava_activities 
                SET needs_refresh = TRUE, last_webhook_at = %s
                WHERE id = %s AND (athlete_name = %s OR athlete_id = %s)
            ''', (now, activity_id, athlete_name, athlete_id))
            
            cursor.close()
            conn.close()
            
            logging.info(f"Scheduled delayed refresh for activity {activity_id} at {scheduled_time}")
            
        except Error as e:
            logging.error(f"Error scheduling refresh: {e}")

# Initialize refresh manager
refresh_manager = ActivityRefreshManager()

# Initialize scheduler for periodic emails
scheduler = BackgroundScheduler(daemon=True)

# Schedule weekly summary (every Monday at 9 AM)
scheduler.add_job(
    func=email_notifier.send_weekly_summary,
    trigger=CronTrigger(day_of_week=0, hour=9, minute=0),  # Monday at 9 AM
    id='weekly_summary'
)

# Schedule monthly summary (1st of each month at 9 AM)
scheduler.add_job(
    func=email_notifier.send_monthly_summary,
    trigger=CronTrigger(day=1, hour=9, minute=0),  # 1st at 9 AM
    id='monthly_summary'
)

# Schedule annual summary (January 1st at 9 AM)
scheduler.add_job(
    func=email_notifier.send_annual_summary,
    trigger=CronTrigger(month=1, day=1, hour=9, minute=0),  # Jan 1st at 9 AM
    id='annual_summary'
)

@app.route('/webhook', methods=['GET'])
def webhook_challenge():
    """Handle Strava webhook subscription challenge"""
    challenge = request.args.get('hub.challenge')
    verify_token = request.args.get('hub.verify_token')
    
    # Add debug logging
    logging.info(f"DEBUG: Received verify_token: '{verify_token}'")
    logging.info(f"DEBUG: Expected WEBHOOK_VERIFY_TOKEN: '{WEBHOOK_VERIFY_TOKEN}'")
    logging.info(f"DEBUG: Tokens match: {verify_token == WEBHOOK_VERIFY_TOKEN}")

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
        owner_id = event_data.get('owner_id')  # This is the athlete_id!
        
        # Log the webhook event (already using owner_id as athlete_id)
        refresh_manager.add_webhook_event(
            activity_id, owner_id, object_type, aspect_type, event_data
        )
        
        # Check if this is an activity event we care about
        if object_type == 'activity' and aspect_type in ['create', 'update']:
            
            # Determine which athlete this belongs to
            athlete_name = determine_athlete_from_owner_id(owner_id)
            
            if athlete_name:
                # Check if activity exists in database
                activity_exists = refresh_manager.activity_exists(activity_id, owner_id)
                
                if not activity_exists:
                    logging.info(f"Activity {activity_id} not found in database, fetching from Strava...")
                    success = refresh_manager.fetch_and_save_activity(activity_id, owner_id, athlete_name)
                    
                    if success:
                        logging.info(f"Successfully fetched missing activity {activity_id} for {athlete_name}")
                        # Send immediate notification for new activity
                        email_notifier.send_immediate_notification(activity_id, 'create')
                    else:
                        logging.error(f"Failed to fetch missing activity {activity_id} for {athlete_name}")
                
                if aspect_type == 'create':
                    logging.info(f"New activity {activity_id} for {athlete_name} (ID: {owner_id})")
                    # Send immediate notification for new activity (if not already sent above)
                    if activity_exists:
                        email_notifier.send_immediate_notification(activity_id, 'create')
                    
                elif aspect_type == 'update':
                    logging.info(f"Activity {activity_id} updated for {athlete_name} (ID: {owner_id})")
                    
                    # Immediately refresh the activity data for updates
                    logging.info(f"Immediately refreshing updated activity {activity_id}")
                    success = refresh_manager.fetch_and_save_activity(activity_id, owner_id, athlete_name)
                    
                    if success:
                        logging.info(f"Successfully refreshed updated activity {activity_id}")
                    else:
                        logging.error(f"Failed to refresh updated activity {activity_id}")
                        # Still schedule delayed refresh as backup
                        refresh_manager.schedule_delayed_refresh(activity_id, athlete_name)
                    
                    # Send immediate notification for updated activity
                    email_notifier.send_immediate_notification(activity_id, 'update')
                    
            else:
                logging.warning(f"Unknown owner_id: {owner_id} - update ATHLETE_MAPPING in .env file")
                
        return "OK", 200
        
    except Exception as e:
        logging.error(f"Error processing webhook: {e}")
        return "Error", 500

def determine_athlete_from_owner_id(owner_id):
    """Determine athlete name from owner_id - updated to also validate athlete_id"""
    # Get athlete ID mappings from environment
    athlete_mappings = {
        int(os.getenv('ATHLETE_ID_DOMINIC', 0)): 'Dominic',
        int(os.getenv('ATHLETE_ID_CLARE', 0)): 'Clare',
        # Add more athletes as needed
    }
    
    # Return the athlete name if we recognize this owner_id
    return athlete_mappings.get(int(owner_id))

async def fetch_activity_immediate(activity_id, athlete_name):
    """Immediately fetch a single activity (for new activities)"""
    try:
        logging.info(f"Fetching new activity {activity_id} for {athlete_name}")
        
        # Get detailed activity data
        detailed_data = await fetcher.api.get_detailed_activity(activity_id, athlete_name)
        
        if detailed_data:
            activity = parse_strava_activity(detailed_data, athlete_name)
            
            # Save to database
            new_count = fetcher.db.save_activities([activity])
            
            logging.info(f"Successfully fetched and saved activity {activity_id} for {athlete_name}")
                
        else:
            logging.error(f"Could not fetch detailed data for activity {activity_id}")
            
    except Exception as e:
        logging.error(f"Error fetching activity {activity_id}: {e}")

@app.route('/refresh-activity/<int:activity_id>', methods=['POST'])
def manual_refresh_activity(activity_id):
    """Manual endpoint to refresh a specific activity"""
    try:
        # Try to determine athlete from activity
        conn = refresh_manager.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT athlete_id, athlete_name FROM strava_activities WHERE id = %s', (activity_id,))
        result = cursor.fetchone()
        
        if not result:
            return jsonify({"error": "Activity not found"}), 404
        
        athlete_id, athlete_name = result
        cursor.close()
        conn.close()
        
        logging.info(f"Manual refresh requested for activity {activity_id} ({athlete_name})")
        
        # Fetch and save the updated activity
        success = refresh_manager.fetch_and_save_activity(activity_id, athlete_id, athlete_name)
        
        if success:
            return jsonify({
                "status": "success", 
                "message": f"Activity {activity_id} refreshed successfully",
                "activity_id": activity_id,
                "athlete": athlete_name
            })
        else:
            return jsonify({
                "status": "error", 
                "message": f"Failed to refresh activity {activity_id}"
            }), 500
            
    except Exception as e:
        logging.error(f"Error in manual activity refresh: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/refresh-activities', methods=['POST'])
def manual_refresh():
    """Manual endpoint to trigger activity refresh"""
    try:
        process_scheduled_refreshes()
        return jsonify({"status": "success", "message": "Refresh processing initiated"})
    except Exception as e:
        logging.error(f"Error in manual refresh: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/debug-stats/<athlete_name>', methods=['GET'])
def debug_stats(athlete_name):
    """Debug endpoint to check 7-day stats calculations"""
    try:
        # Get 7-day stats with debug info (last 7 days including today)
        # Use date-only comparison to avoid time-of-day issues
        today = datetime.now().date()
        seven_days_ago = today - timedelta(days=6)  # 6 days ago + today = 7 days
        
        logging.info(f"DEBUG: Calculating stats for {athlete_name}")
        logging.info(f"DEBUG: Today: {today}")
        logging.info(f"DEBUG: Seven days ago: {seven_days_ago}")
        
        stats = email_notifier.get_activity_stats(
            athlete_name=athlete_name,
            start_date=seven_days_ago
        )
        
        return jsonify({
            "athlete": athlete_name,
            "date_range": {
                "from": seven_days_ago.isoformat(),
                "to": today.isoformat(),
                "days_difference": (today - seven_days_ago).days,
                "explanation": "6 days ago + today = 7 days total (date-only comparison)"
            },
            "stats": stats,
            "timestamp": datetime.now().isoformat()
        })
        
    except Exception as e:
        logging.error(f"Error in debug stats: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/test-email', methods=['POST'])
def test_email():
    """Test endpoint to send test emails"""
    try:
        data = request.json if request.is_json else {}
        email_type = data.get('type', 'immediate')
        test_athlete = data.get('athlete', 'Dominic')  # Allow testing specific athlete emails
        
        if email_type == 'weekly':
            email_notifier.send_weekly_summary()
        elif email_type == 'monthly':
            email_notifier.send_monthly_summary()
        elif email_type == 'annual':
            email_notifier.send_annual_summary()
        else:
            # Test immediate notification - athlete-specific
            athlete_email = email_notifier.athlete_emails.get(test_athlete)
            if not athlete_email:
                return jsonify({"error": f"No email configured for {test_athlete}"}), 400
            
            email_notifier.send_email(
                f"🧪 Test Email - {test_athlete}",
                f"""
                <html>
                <body style="font-family: Arial, sans-serif;">
                    <h2>🧪 Test Email for {test_athlete}</h2>
                    <p>This is a test email from the Strava Webhook Server.</p>
                    <p><strong>Recipient:</strong> {athlete_email}</p>
                    <p><strong>Time:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                    <p>Athlete-specific email notifications are working correctly!</p>
                </body>
                </html>
                """,
                athlete_email
            )
        
        return jsonify({
            "status": "success", 
            "message": f"Test {email_type} email sent",
            "recipient": athlete_email if email_type == 'immediate' else "both admins"
        })
    except Exception as e:
        logging.error(f"Error sending test email: {e}")
        return jsonify({"error": str(e)}), 500

def process_scheduled_refreshes():
    """Process activities that are due for refresh"""
    activities_to_refresh = refresh_manager.get_activities_due_for_refresh()
    
    if not activities_to_refresh:
        logging.info("No activities due for refresh")
        return
    
    logging.info(f"Processing {len(activities_to_refresh)} activities for refresh")
    
    for activity in activities_to_refresh:
        activity_id = activity['id']
        athlete_name = activity['athlete_name']
        try:
            # Run the refresh
            asyncio.run(refresh_single_activity(activity_id, athlete_name))
            
            # Mark as completed
            refresh_manager.mark_activity_refreshed(activity_id)
            
            logging.info(f"Completed refresh for activity {activity_id}")
            
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
        'athlete_mapping_configured': bool(ATHLETE_MAPPING),
        'email_configured': bool(email_notifier.api_key),
        'scheduler_running': scheduler.running
    })

@app.route('/stats', methods=['GET'])
def webhook_stats():
    """Show webhook statistics"""
    try:
        conn = refresh_manager.get_connection()
        cursor = conn.cursor()
        
        # Get recent webhook events
        cursor.execute('''
            SELECT COUNT(*) as total_events,
                   SUM(CASE WHEN event_type = 'create' THEN 1 ELSE 0 END) as creates,
                   SUM(CASE WHEN event_type = 'update' THEN 1 ELSE 0 END) as updates
            FROM webhook_events
            WHERE received_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
        ''')
        
        stats = cursor.fetchone()
        
        # Get pending refreshes
        cursor.execute('''
            SELECT COUNT(*) FROM activity_refresh_queue 
            WHERE completed_at IS NULL
        ''')
        
        pending_refreshes = cursor.fetchone()[0] if cursor.fetchone() else 0
        
        conn.close()
        
        return jsonify({
            'webhook_events_last_7_days': {
                'total': stats[0] if stats else 0,
                'creates': stats[1] if stats else 0, 
                'updates': stats[2] if stats else 0
            },
            'pending_refreshes': pending_refreshes,
            'email_notifications': {
                'configured': bool(email_notifier.api_key),
                'scheduler_jobs': len(scheduler.get_jobs())
            },
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        logging.error(f"Error getting stats: {e}")
        return jsonify({'error': 'Could not fetch stats'}), 500

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

# Start background processor and scheduler
refresh_thread = threading.Thread(target=refresh_processor, daemon=True)
refresh_thread.start()

# Start the email scheduler
scheduler.start()

if __name__ == '__main__':
    print("🚀 Starting Enhanced Strava Webhook Server with Email Notifications")
    print(f"Webhook endpoint: http://dashboard.allkins.com/webhook")
    print(f"Health check: http://dashboard.allkins.com/health")
    print(f"Stats: http://dashboard.allkins.com/stats")
    print(f"Test email: POST http://dashboard.allkins.com/test-email")
    print("📧 Email notifications configured:")
    print("  - Immediate: New/updated rides")
    print("  - Weekly: Every Monday at 9 AM")
    print("  - Monthly: 1st of month at 9 AM") 
    print("  - Annual: January 1st at 9 AM")
    print("Remember to update ATHLETE_MAPPING with your Strava athlete IDs!")
    
    # Run the Flask app
    app.run(host='0.0.0.0', port=int(os.getenv('WEBHOOK_PORT', 5000)), debug=False)