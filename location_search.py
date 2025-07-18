#location_search.py
#!/usr/bin/env python3
"""
Location search and geocoding for weather dashboard
Finds coordinates for location names and stores them in the database
"""

import os
import requests
import mysql.connector
import logging
from dotenv import load_dotenv
import json

# Load environment variables
load_dotenv()

# Set up logging
log_level = getattr(logging, os.getenv('LOG_LEVEL', 'INFO'))
logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Configuration
OPENWEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY')
GEOCODING_URL = "http://api.openweathermap.org/geo/1.0/direct"

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

def geocode_location(location_name, limit=5):
    """
    Geocode a location name to get coordinates
    Returns list of possible matches with details
    """
    if not OPENWEATHER_API_KEY:
        logging.error("OPENWEATHER_API_KEY not configured")
        return []
    
    params = {
        'q': location_name,
        'limit': limit,
        'appid': OPENWEATHER_API_KEY
    }
    
    try:
        response = requests.get(GEOCODING_URL, params=params)
        response.raise_for_status()
        locations = response.json()
        
        # Format the results for easier handling
        formatted_locations = []
        for loc in locations:
            formatted_loc = {
                'name': loc.get('name', ''),
                'country': loc.get('country', ''),
                'state': loc.get('state', ''),
                'latitude': loc.get('lat'),
                'longitude': loc.get('lon'),
                'display_name': format_display_name(loc)
            }
            formatted_locations.append(formatted_loc)
        
        return formatted_locations
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Geocoding request failed: {e}")
        return []

def format_display_name(location_data):
    """Create a readable display name from location data"""
    parts = []
    
    if location_data.get('name'):
        parts.append(location_data['name'])
    
    if location_data.get('state'):
        parts.append(location_data['state'])
    
    if location_data.get('country'):
        parts.append(location_data['country'])
    
    return ', '.join(parts)

def save_location(name, latitude, longitude, set_as_default=False):
    """
    Save a location to the database
    Returns the location ID if successful, None if failed
    """
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        
        # If setting as default, remove default flag from other locations
        if set_as_default:
            cursor.execute("UPDATE weather_locations SET is_default = FALSE")
        
        # Check if this exact location already exists
        check_query = """
        SELECT id FROM weather_locations 
        WHERE ABS(latitude - %s) < 0.001 AND ABS(longitude - %s) < 0.001
        """
        cursor.execute(check_query, (latitude, longitude))
        existing = cursor.fetchone()
        
        if existing:
            location_id = existing[0]
            # Update the name and default status if needed
            update_query = """
            UPDATE weather_locations 
            SET name = %s, is_default = %s 
            WHERE id = %s
            """
            cursor.execute(update_query, (name, set_as_default, location_id))
            logging.info(f"Updated existing location: {name}")
        else:
            # Insert new location
            insert_query = """
            INSERT INTO weather_locations (name, latitude, longitude, is_default)
            VALUES (%s, %s, %s, %s)
            """
            cursor.execute(insert_query, (name, latitude, longitude, set_as_default))
            location_id = cursor.lastrowid
            logging.info(f"Added new location: {name}")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return location_id
        
    except mysql.connector.Error as e:
        logging.error(f"Error saving location: {e}")
        return None

def search_and_save_location(location_name, set_as_default=False):
    """
    Search for a location, display options, and save the selected one
    Returns the saved location details
    """
    logging.info(f"Searching for location: {location_name}")
    
    # Geocode the location
    locations = geocode_location(location_name)
    
    if not locations:
        logging.error(f"No locations found for: {location_name}")
        return None
    
    # If only one result, use it automatically
    if len(locations) == 1:
        location = locations[0]
        logging.info(f"Found single match: {location['display_name']}")
    else:
        # Multiple results - in a web interface, you'd present these as options
        # For now, we'll use the first (most relevant) result
        location = locations[0]
        logging.info(f"Multiple matches found, using first: {location['display_name']}")
        
        # Log all options for reference
        logging.info("All matches found:")
        for i, loc in enumerate(locations):
            logging.info(f"  {i+1}. {loc['display_name']} ({loc['latitude']}, {loc['longitude']})")
    
    # Save the location
    location_id = save_location(
        location['display_name'],
        location['latitude'],
        location['longitude'],
        set_as_default
    )
    
    if location_id:
        location['id'] = location_id
        return location
    else:
        return None

def list_saved_locations():
    """List all saved locations in the database"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, name, latitude, longitude, is_default, created_at
            FROM weather_locations
            ORDER BY is_default DESC, name
        """)
        
        locations = []
        for row in cursor.fetchall():
            locations.append({
                'id': row[0],
                'name': row[1],
                'latitude': float(row[2]),
                'longitude': float(row[3]),
                'is_default': bool(row[4]),
                'created_at': row[5]
            })
        
        cursor.close()
        conn.close()
        return locations
        
    except mysql.connector.Error as e:
        logging.error(f"Error listing locations: {e}")
        return []

def set_default_location(location_id):
    """Set a location as the default"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # Remove default from all locations
        cursor.execute("UPDATE weather_locations SET is_default = FALSE")
        
        # Set the specified location as default
        cursor.execute(
            "UPDATE weather_locations SET is_default = TRUE WHERE id = %s",
            (location_id,)
        )
        
        if cursor.rowcount > 0:
            conn.commit()
            logging.info(f"Set location {location_id} as default")
            result = True
        else:
            logging.error(f"Location {location_id} not found")
            result = False
        
        cursor.close()
        conn.close()
        return result
        
    except mysql.connector.Error as e:
        logging.error(f"Error setting default location: {e}")
        return False

def delete_location(location_id):
    """Delete a location from the database"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # Check if this is the default location
        cursor.execute("SELECT is_default FROM weather_locations WHERE id = %s", (location_id,))
        result = cursor.fetchone()
        
        if not result:
            logging.error(f"Location {location_id} not found")
            return False
        
        was_default = result[0]
        
        # Delete the location (this will also delete related weather data due to foreign keys)
        cursor.execute("DELETE FROM weather_locations WHERE id = %s", (location_id,))
        
        if cursor.rowcount > 0:
            # If we deleted the default location, set another one as default
            if was_default:
                cursor.execute("""
                    UPDATE weather_locations 
                    SET is_default = TRUE 
                    WHERE id = (SELECT id FROM (
                        SELECT id FROM weather_locations ORDER BY created_at LIMIT 1
                    ) AS temp)
                """)
            
            conn.commit()
            logging.info(f"Deleted location {location_id}")
            result = True
        else:
            result = False
        
        cursor.close()
        conn.close()
        return result
        
    except mysql.connector.Error as e:
        logging.error(f"Error deleting location: {e}")
        return False

# Command line interface for testing
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python3 location_search.py search 'London, UK'")
        print("  python3 location_search.py search 'Tokyo' --default")
        print("  python3 location_search.py list")
        print("  python3 location_search.py set-default <location_id>")
        print("  python3 location_search.py delete <location_id>")
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == "search":
        if len(sys.argv) < 3:
            print("Please provide a location name")
            sys.exit(1)
        
        location_name = sys.argv[2]
        set_default = "--default" in sys.argv
        
        result = search_and_save_location(location_name, set_default)
        if result:
            print(f"✅ Saved location: {result['display_name']}")
            print(f"   Coordinates: {result['latitude']}, {result['longitude']}")
            print(f"   ID: {result['id']}")
            if set_default:
                print("   Set as default location")
        else:
            print("❌ Failed to save location")
    
    elif command == "list":
        locations = list_saved_locations()
        if locations:
            print("Saved locations:")
            for loc in locations:
                default_marker = " (DEFAULT)" if loc['is_default'] else ""
                print(f"  {loc['id']}: {loc['name']}{default_marker}")
                print(f"      Coordinates: {loc['latitude']}, {loc['longitude']}")
        else:
            print("No saved locations")
    
    elif command == "set-default":
        if len(sys.argv) < 3:
            print("Please provide a location ID")
            sys.exit(1)
        
        location_id = int(sys.argv[2])
        if set_default_location(location_id):
            print(f"✅ Set location {location_id} as default")
        else:
            print(f"❌ Failed to set location {location_id} as default")
    
    elif command == "delete":
        if len(sys.argv) < 3:
            print("Please provide a location ID")
            sys.exit(1)
        
        location_id = int(sys.argv[2])
        if delete_location(location_id):
            print(f"✅ Deleted location {location_id}")
        else:
            print(f"❌ Failed to delete location {location_id}")
    
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)