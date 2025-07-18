#hourly_forecast.py
#!/usr/bin/env python3
"""
Hourly weather forecast for rest of day
Fetches hourly forecast from current time to end of day
Called live when dashboard is accessed
"""

import os
import requests
import mysql.connector
from datetime import datetime, timedelta
import logging
from dotenv import load_dotenv
import json
import pytz
from timezonefinder import TimezoneFinder

# Load environment variables
load_dotenv()

# Configuration
WEATHER_API_KEY = os.getenv('WEATHER_API_KEY')
TOMORROW_IO_BASE_URL = "https://api.tomorrow.io/v4/timelines"

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME'),
    'charset': 'utf8mb4'
}

def get_db_connection():
    """Get database connection"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except mysql.connector.Error as e:
        logging.error(f"Database connection failed: {e}")
        return None

def get_default_location():
    """Get the default location from database"""
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT latitude, longitude, name, id FROM weather_locations WHERE is_default = TRUE LIMIT 1")
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if result:
            return {
                'lat': float(result[0]), 
                'lon': float(result[1]), 
                'name': result[2],
                'location_id': result[3]
            }
        else:
            return None
    except mysql.connector.Error as e:
        logging.error(f"Error fetching default location: {e}")
        return None

def get_location_by_id(location_id):
    """Get a specific location by ID"""
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT latitude, longitude, name, id FROM weather_locations WHERE id = %s", (location_id,))
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if result:
            return {
                'lat': float(result[0]), 
                'lon': float(result[1]), 
                'name': result[2],
                'location_id': result[3]
            }
        else:
            return None
    except mysql.connector.Error as e:
        logging.error(f"Error fetching location: {e}")
        return None

def fetch_hourly_forecast(lat, lon, hours_ahead=None):
    """
    Fetch hourly forecast from Tomorrow.io
    If hours_ahead is None, fetches from now until end of day
    If hours_ahead is specified, fetches that many hours from now
    """
    now = datetime.now()
    
    if hours_ahead is None:
        # Calculate end of today
        end_of_day = now.replace(hour=23, minute=59, second=59)
        end_time = end_of_day.strftime('%Y-%m-%dT%H:%M:%SZ')
    else:
        # Fetch specific number of hours ahead
        end_time = (now + timedelta(hours=hours_ahead)).strftime('%Y-%m-%dT%H:%M:%SZ')
    
    params = {
        'location': f"{lat},{lon}",
        'fields': [
            'temperature',
            'windSpeed', 
            'windDirection',
            'humidity',
            'precipitationIntensity',
            'precipitationProbability',
            'weatherCode'
        ],
        'timesteps': '1h',
        'startTime': 'now',
        'endTime': end_time,
        'apikey': WEATHER_API_KEY
    }
    
    try:
        response = requests.get(TOMORROW_IO_BASE_URL, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Hourly forecast API request failed: {e}")
        return None

def get_location_timezone(lat, lon):
    """
    Get timezone for a location using timezonefinder
    Much more accurate than manual mapping
    """
    try:
        tf = TimezoneFinder()
        timezone = tf.timezone_at(lat=lat, lng=lon)
        
        if timezone:
            return timezone
        else:
            # Fallback to UTC-based estimation if timezonefinder fails
            utc_offset_hours = round(lon / 15)  # 15 degrees per hour
            
            if utc_offset_hours == 0:
                return 'UTC'
            elif utc_offset_hours > 0:
                return f'Etc/GMT-{utc_offset_hours}'  # Note: sign is reversed in Etc/GMT
            else:
                return f'Etc/GMT+{abs(utc_offset_hours)}'
                
    except Exception as e:
        logging.warning(f"Could not determine timezone for {lat}, {lon}: {e}")
        # Simple fallback
        return 'UTC'

def format_hourly_data(weather_data, lat=None, lon=None):
    """Format the hourly weather data for easy consumption"""
    if not weather_data or 'data' not in weather_data:
        return []
    
    # Determine timezone based on location
    if lat is not None and lon is not None:
        timezone = get_location_timezone(lat, lon)
    else:
        timezone = 'UTC'  # Fallback
    
    try:
        local_tz = pytz.timezone(timezone)
    except:
        # If timezone is invalid, fall back to UTC
        local_tz = pytz.UTC
        timezone = 'UTC'
    
    hourly_forecast = []
    
    # Find hourly timeline
    for timeline in weather_data['data']['timelines']:
        if timeline['timestep'] == '1h':
            for interval in timeline['intervals']:
                # Convert UTC time to local time
                utc_time = datetime.fromisoformat(interval['startTime'].replace('Z', '+00:00'))
                local_time = utc_time.astimezone(local_tz)
                
                hour_data = {
                    'datetime': interval['startTime'],
                    'local_datetime': local_time.isoformat(),
                    'hour': local_time.strftime('%H:%M'),
                    'timezone': timezone,
                    'temperature': interval['values'].get('temperature', 0),
                    'wind_speed': interval['values'].get('windSpeed', 0),
                    'wind_direction': interval['values'].get('windDirection', 0),
                    'wind_direction_text': get_wind_direction_text(interval['values'].get('windDirection', 0)),
                    'humidity': interval['values'].get('humidity', 0),
                    'precipitation_intensity': interval['values'].get('precipitationIntensity', 0),
                    'precipitation_probability': interval['values'].get('precipitationProbability', 0) * 100,  # Convert to percentage
                    'weather_code': interval['values'].get('weatherCode', 0)
                }
                hourly_forecast.append(hour_data)
            break
    
    return hourly_forecast

def get_hourly_forecast_for_location(location_id=None, hours_ahead=None):
    """
    Get hourly forecast for a specific location or default location
    Returns formatted hourly data
    """
    # Get location details
    if location_id:
        location = get_location_by_id(location_id)
    else:
        location = get_default_location()
    
    if not location:
        logging.error("No location found for hourly forecast")
        return None
    
    # Fetch hourly data
    weather_data = fetch_hourly_forecast(location['lat'], location['lon'], hours_ahead)
    
    if not weather_data:
        logging.error("Failed to fetch hourly weather data")
        return None
    
    # Format the data
    hourly_data = format_hourly_data(weather_data, location['lat'], location['lon'])
    
    return {
        'location': location,
        'hourly_forecast': hourly_data,
        'generated_at': datetime.now().isoformat()
    }

def get_wind_direction_text(degrees):
    """Convert wind direction degrees to compass direction"""
    if degrees is None or degrees < 0:
        return "Unknown"
    
    # Normalize to 0-360
    degrees = degrees % 360
    
    directions = [
        "N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
        "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"
    ]
    
    # Each direction covers 22.5 degrees (360/16)
    index = round(degrees / 22.5) % 16
    return directions[index]

def get_weather_code_description(code):
    """Convert Tomorrow.io weather codes to readable descriptions"""
    weather_codes = {
        0: "Unknown",
        1000: "Clear",
        1100: "Mostly Clear",
        1101: "Partly Cloudy",
        1102: "Mostly Cloudy",
        1001: "Cloudy",
        2000: "Fog",
        2100: "Light Fog",
        4000: "Drizzle",
        4001: "Rain",
        4200: "Light Rain",
        4201: "Heavy Rain",
        5000: "Snow",
        5001: "Flurries",
        5100: "Light Snow",
        5101: "Heavy Snow",
        6000: "Freezing Drizzle",
        6001: "Freezing Rain",
        6200: "Light Freezing Rain",
        6201: "Heavy Freezing Rain",
        7000: "Ice Pellets",
        7101: "Heavy Ice Pellets",
        7102: "Light Ice Pellets",
        8000: "Thunderstorm"
    }
    return weather_codes.get(int(code), "Unknown")

# Web API endpoint functions (for integration with your web framework)
def hourly_forecast_api(location_id=None, hours=None):
    """
    API endpoint function that returns JSON-ready hourly forecast
    Can be called from Flask/FastAPI/etc
    """
    try:
        result = get_hourly_forecast_for_location(location_id, hours)
        
        if not result:
            return {
                'error': 'Failed to fetch hourly forecast',
                'data': None
            }
        
        # Add weather descriptions
        for hour in result['hourly_forecast']:
            hour['weather_description'] = get_weather_code_description(hour['weather_code'])
        
        return {
            'error': None,
            'data': result
        }
        
    except Exception as e:
        logging.error(f"Error in hourly forecast API: {e}")
        return {
            'error': str(e),
            'data': None
        }

# Command line interface for testing
if __name__ == "__main__":
    import sys
    
    # Set up logging for CLI
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    location_id = None
    hours_ahead = None
    
    # Parse command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1].isdigit():
            location_id = int(sys.argv[1])
    
    if len(sys.argv) > 2:
        if sys.argv[2].isdigit():
            hours_ahead = int(sys.argv[2])
    
    print("🌤️  Fetching hourly forecast...")
    
    if location_id:
        print(f"   Using location ID: {location_id}")
    else:
        print("   Using default location")
    
    if hours_ahead:
        print(f"   Forecast for next {hours_ahead} hours")
    else:
        print("   Forecast until end of day")
    
    result = hourly_forecast_api(location_id, hours_ahead)
    
    if result['error']:
        print(f"❌ Error: {result['error']}")
    else:
        data = result['data']
        print(f"\n📍 Location: {data['location']['name']}")
        print(f"🕒 Generated at: {data['generated_at']}")
        print(f"⏰ Timezone: {data['hourly_forecast'][0]['timezone'] if data['hourly_forecast'] else 'N/A'}")
        print(f"📊 Hours available: {len(data['hourly_forecast'])}")
        
        print("\n⏰ Hourly Forecast:")
        for hour in data['hourly_forecast']:
            print(f"  {hour['hour']}: {hour['temperature']:.1f}°C, {hour['weather_description']}")
            print(f"      💨 Wind: {hour['wind_speed']:.1f} km/h {hour['wind_direction_text']} ({hour['wind_direction']:.0f}°)")
            print(f"      💧 Rain chance: {hour['precipitation_probability']:.0f}%")
            print()