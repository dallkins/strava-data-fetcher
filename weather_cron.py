#weather_cron.py
#!/usr/bin/env python3
"""
Weather data fetcher for Tomorrow.io API
Fetches current conditions and 5-day forecast, stores in MariaDB
Run hourly via cron
"""

import os
import requests
import mysql.connector
from datetime import datetime, timedelta
import json
import logging
from dotenv import load_dotenv
import pytz
from timezonefinder import TimezoneFinder

# Load environment variables - explicitly specify .env file path if needed
load_dotenv()

# Set up logging
log_file = os.getenv('LOG_FILE', '/var/log/weather_cron.log')
log_level = getattr(logging, os.getenv('LOG_LEVEL', 'INFO'))

logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

# Configuration from environment
WEATHER_API_KEY = os.getenv('WEATHER_API_KEY')
TOMORROW_IO_BASE_URL = "https://api.tomorrow.io/v4/timelines"

# For air quality, we'll use OpenWeatherMap (free tier available)
# You'll need to get a free API key from: https://openweathermap.org/api
OPENWEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY')  # Add this to your .env file
OPENWEATHER_AIR_URL = "http://api.openweathermap.org/data/2.5/air_pollution"

# Database configuration from environment
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME'),
    'charset': 'utf8mb4'
}

# Validate required environment variables
if not WEATHER_API_KEY:
    logging.error("WEATHER_API_KEY not found in environment variables")
    exit(1)

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
        cursor.execute("SELECT latitude, longitude, id FROM weather_locations WHERE is_default = TRUE LIMIT 1")
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if result:
            return {'lat': float(result[0]), 'lon': float(result[1]), 'location_id': result[2]}
        else:
            logging.warning("No default location found in database")
            return None
    except mysql.connector.Error as e:
        logging.error(f"Error fetching default location: {e}")
        return None

def fetch_weather_data(lat, lon):
    """Fetch current weather and 5-day forecast from Tomorrow.io"""
    
    # Parameters for the API call - enhanced for forecast data
    params = {
        'location': f"{lat},{lon}",
        'fields': [
            'temperature',
            'temperatureMax',
            'temperatureMin', 
            'windSpeed', 
            'windDirection',
            'humidity',
            'precipitationIntensity',
            'precipitationProbability',
            'weatherCode',
            'sunriseTime',
            'sunsetTime'
        ],
        'timesteps': ['current', '1d'],
        'startTime': 'now',
        'endTime': (datetime.now() + timedelta(days=5)).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'apikey': WEATHER_API_KEY
    }
    
    try:
        response = requests.get(TOMORROW_IO_BASE_URL, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed: {e}")
        return None

def get_air_quality(lat, lon):
    """Fetch air quality data from OpenWeatherMap API"""
    if not OPENWEATHER_API_KEY:
        logging.warning("OPENWEATHER_API_KEY not configured, skipping air quality")
        return 0
        
    params = {
        'lat': lat,
        'lon': lon,
        'appid': OPENWEATHER_API_KEY
    }
    
    try:
        response = requests.get(OPENWEATHER_AIR_URL, params=params)
        response.raise_for_status()
        data = response.json()
        
        # OpenWeatherMap returns AQI directly (1-5 scale)
        # Convert to 0-500 EPA scale for consistency
        if 'list' in data and data['list']:
            aqi = data['list'][0]['main']['aqi']
            # Convert 1-5 scale to approximate EPA AQI
            aqi_conversion = {1: 50, 2: 100, 3: 150, 4: 200, 5: 300}
            return aqi_conversion.get(aqi, 0)
        return 0
    except requests.exceptions.RequestException as e:
        logging.warning(f"Could not fetch air quality data: {e}")
        return 0

def get_location_timezone(lat, lon):
    """Get timezone for a location using timezonefinder"""
    try:
        tf = TimezoneFinder()
        timezone = tf.timezone_at(lat=lat, lng=lon)
        return timezone if timezone else 'UTC'
    except Exception as e:
        logging.warning(f"Could not determine timezone for {lat}, {lon}: {e}")
        return 'UTC'

def convert_utc_time_to_local(utc_time_str, lat, lon):
    """Convert UTC time string to local time"""
    if not utc_time_str:
        return None
    
    try:
        # Parse UTC time
        utc_dt = datetime.fromisoformat(utc_time_str.replace('Z', '+00:00'))
        
        # Get local timezone
        local_tz_name = get_location_timezone(lat, lon)
        local_tz = pytz.timezone(local_tz_name)
        
        # Convert to local time
        local_dt = utc_dt.astimezone(local_tz)
        
        return local_dt.time()
        
    except Exception as e:
        logging.warning(f"Could not convert time {utc_time_str}: {e}")
        return None

def update_current_weather(location_id, weather_data, air_quality, lat, lon):
    """Update current weather conditions in database"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # Find current weather data in the response
        current_data = None
        if 'data' in weather_data and 'timelines' in weather_data['data']:
            for timeline in weather_data['data']['timelines']:
                if timeline['timestep'] == 'current':
                    current_data = timeline['intervals'][0]['values']
                    break
        
        if not current_data:
            logging.error("No current weather data found in response")
            return False
        
        # Delete existing current weather for this location
        cursor.execute("DELETE FROM weather_current WHERE location_id = %s", (location_id,))
        
        # Insert new current weather data
        insert_query = """
        INSERT INTO weather_current 
        (location_id, temperature, wind_speed, wind_direction, air_quality_index, humidity, precipitation, sunrise, sunset)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Extract sunrise/sunset times and convert to local time
        sunrise_time = convert_utc_time_to_local(current_data.get('sunriseTime'), lat, lon)
        sunset_time = convert_utc_time_to_local(current_data.get('sunsetTime'), lat, lon)
        
        if sunrise_time:
            logging.info(f"Parsed sunrise: {sunrise_time}")
        else:
            logging.warning("No valid sunrise time")
            
        if sunset_time:
            logging.info(f"Parsed sunset: {sunset_time}")
        else:
            logging.warning("No valid sunset time")
        
        values = (
            location_id,
            current_data.get('temperature', 0),
            current_data.get('windSpeed', 0),
            current_data.get('windDirection', 0),
            air_quality,
            current_data.get('humidity', 0),
            current_data.get('precipitationIntensity', 0),
            sunrise_time,
            sunset_time
        )
        
        cursor.execute(insert_query, values)
        conn.commit()
        cursor.close()
        conn.close()
        
        logging.info("Current weather data updated successfully")
        return True
        
    except mysql.connector.Error as e:
        logging.error(f"Error updating current weather: {e}")
        return False

def update_forecast(location_id, weather_data, lat, lon):
    """Update 5-day forecast in database with enhanced data"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # Find daily forecast data
        daily_data = None
        if 'data' in weather_data and 'timelines' in weather_data['data']:
            for timeline in weather_data['data']['timelines']:
                if timeline['timestep'] == '1d':
                    daily_data = timeline['intervals']
                    break
        
        if not daily_data:
            logging.error("No daily forecast data found in response")
            return False
        
        # Delete existing forecast for this location
        cursor.execute("DELETE FROM weather_forecast WHERE location_id = %s", (location_id,))
        
        # Insert new forecast data with enhanced fields
        for interval in daily_data[:5]:  # Only 5 days
            forecast_date = datetime.fromisoformat(interval['startTime'].replace('Z', '+00:00')).date()
            values_data = interval['values']
            
            insert_query = """
            INSERT INTO weather_forecast 
            (location_id, forecast_date, high_temp, low_temp, condition_code, precipitation_chance, wind_speed, wind_direction, sunrise, sunset)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            # Extract sunrise/sunset for this day and convert to local time
            sunrise_time = convert_utc_time_to_local(values_data.get('sunriseTime'), lat, lon)
            sunset_time = convert_utc_time_to_local(values_data.get('sunsetTime'), lat, lon)
            
            if sunrise_time:
                logging.info(f"Parsed forecast sunrise for {forecast_date}: {sunrise_time}")
            if sunset_time:
                logging.info(f"Parsed forecast sunset for {forecast_date}: {sunset_time}")
            
            values = (
                location_id,
                forecast_date,
                values_data.get('temperatureMax', values_data.get('temperature', 0)),
                values_data.get('temperatureMin', values_data.get('temperature', 0)),
                str(values_data.get('weatherCode', 0)),
                values_data.get('precipitationProbability', 0) * 100,  # Convert to percentage
                values_data.get('windSpeed', 0),
                values_data.get('windDirection', 0),
                sunrise_time,
                sunset_time
            )
            
            cursor.execute(insert_query, values)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logging.info("Forecast data updated successfully")
        return True
        
    except mysql.connector.Error as e:
        logging.error(f"Error updating forecast: {e}")
        return False

def main():
    """Main function to fetch and store weather data"""
    logging.info("Starting weather data fetch")
    
    # Get default location
    location = get_default_location()
    if not location:
        logging.error("No default location configured. Please add a location to the database.")
        return
    
    # Fetch weather data
    weather_data = fetch_weather_data(location['lat'], location['lon'])
    if not weather_data:
        logging.error("Failed to fetch weather data")
        return
    
    # Fetch air quality
    air_quality = get_air_quality(location['lat'], location['lon'])
    
    # Update database
    current_success = update_current_weather(location['location_id'], weather_data, air_quality, location['lat'], location['lon'])
    forecast_success = update_forecast(location['location_id'], weather_data, location['lat'], location['lon'])
    
    if current_success and forecast_success:
        logging.info("Weather data update completed successfully")
    else:
        logging.error("Weather data update had errors")

if __name__ == "__main__":
    main()