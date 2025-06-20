#!/usr/bin/env python3
"""
SQLite to MariaDB Migration Script
Migrates all Strava data from SQLite to MariaDB
"""

import sqlite3
import mysql.connector
import logging
import json
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def get_sqlite_connection():
    """Connect to SQLite database"""
    sqlite_path = os.getenv('STRAVA_DB_PATH', '/home/dom1n1c4/projects/strava_data/strava_data.db')
    if not os.path.exists(sqlite_path):
        raise FileNotFoundError(f"SQLite database not found at: {sqlite_path}")
    return sqlite3.connect(sqlite_path)

def get_mariadb_connection():
    """Connect to MariaDB database"""
    return mysql.connector.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        database=os.getenv('DB_NAME'),
        charset='utf8mb4',
        collation='utf8mb4_unicode_ci'
    )

def create_mariadb_tables(mysql_conn):
    """Create MariaDB tables with proper schema"""
    cursor = mysql_conn.cursor()
    
    # Drop existing tables if they exist
    tables_to_drop = ['strava_activities', 'activity_refresh_queue', 'webhook_events']
    for table in tables_to_drop:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
        logging.info(f"Dropped table {table} if it existed")
    
    # Create strava_activities table
    cursor.execute('''
        CREATE TABLE strava_activities (
            id BIGINT PRIMARY KEY,
            athlete_name VARCHAR(100) NOT NULL,
            name TEXT,
            start_date_local DATETIME,
            start_date DATETIME,
            utc_offset FLOAT,
            gear_id VARCHAR(50),
            gear_name VARCHAR(200),
            distance FLOAT,
            elapsed_time INT,
            moving_time INT,
            calories FLOAT,
            average_heartrate FLOAT,
            max_heartrate FLOAT,
            average_watts FLOAT,
            max_watts FLOAT,
            average_speed FLOAT,
            max_speed FLOAT,
            type VARCHAR(50),
            sport_type VARCHAR(50),
            total_elevation_gain FLOAT,
            kudos_count INT,
            weighted_average_watts FLOAT,
            average_cadence FLOAT,
            trainer BOOLEAN,
            map_polyline LONGTEXT,
            device_name VARCHAR(200),
            timezone VARCHAR(100),
            start_latlng JSON,
            end_latlng JSON,
            last_webhook_at DATETIME NULL,
            last_refresh_at DATETIME NULL,
            needs_refresh BOOLEAN DEFAULT FALSE,
            data_loaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY unique_activity (id, athlete_name),
            INDEX idx_athlete_date (athlete_name, start_date_local),
            INDEX idx_needs_refresh (needs_refresh, last_refresh_at),
            INDEX idx_start_date (start_date_local)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    ''')
    
    logging.info("Created strava_activities table")
    
    # Create activity_refresh_queue table
    cursor.execute('''
        CREATE TABLE activity_refresh_queue (
            id INT AUTO_INCREMENT PRIMARY KEY,
            activity_id BIGINT NOT NULL,
            athlete_name VARCHAR(100) NOT NULL,
            webhook_received_at DATETIME NOT NULL,
            scheduled_refresh_at DATETIME NOT NULL,
            completed_at DATETIME NULL,
            refresh_type VARCHAR(50) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY unique_refresh (activity_id, refresh_type),
            INDEX idx_scheduled (scheduled_refresh_at, completed_at),
            INDEX idx_athlete (athlete_name)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    ''')
    
    logging.info("Created activity_refresh_queue table")
    
    # Create webhook_events table
    cursor.execute('''
        CREATE TABLE webhook_events (
            id INT AUTO_INCREMENT PRIMARY KEY,
            activity_id BIGINT NOT NULL,
            athlete_id BIGINT NOT NULL,
            event_type VARCHAR(50) NOT NULL,
            aspect_type VARCHAR(50) NOT NULL,
            received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            processed_at DATETIME NULL,
            raw_data JSON,
            INDEX idx_activity (activity_id),
            INDEX idx_received (received_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    ''')
    
    logging.info("Created webhook_events table")
    
    mysql_conn.commit()

def convert_iso_datetime(iso_string):
    """Convert ISO 8601 datetime string to MySQL compatible format"""
    if not iso_string:
        return None
    
    try:
        # Handle Strava's ISO format: '2025-06-17T18:46:17Z'
        if iso_string.endswith('Z'):
            # Remove Z and parse
            dt_string = iso_string.replace('Z', '')
            dt = datetime.fromisoformat(dt_string)
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        elif 'T' in iso_string:
            # Handle other ISO variants
            dt = datetime.fromisoformat(iso_string.replace('Z', '+00:00'))
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        else:
            # Already in correct format
            return iso_string
    except Exception as e:
        logging.warning(f"Could not convert datetime {iso_string}: {e}")
        return None

def migrate_strava_activities(sqlite_conn, mysql_conn):
    """Migrate strava_activities data"""
    sqlite_cursor = sqlite_conn.cursor()
    mysql_cursor = mysql_conn.cursor()
    
    # Get all activities from SQLite
    sqlite_cursor.execute("SELECT * FROM strava_activities")
    activities = sqlite_cursor.fetchall()
    
    # Get column names
    columns = [description[0] for description in sqlite_cursor.description]
    logging.info(f"Found {len(activities)} activities to migrate")
    logging.info(f"Columns: {columns}")
    
    # Prepare insert statement for MariaDB
    # Handle the fact that MariaDB has additional columns
    mariadb_columns = [
        'id', 'athlete_name', 'name', 'start_date_local', 'start_date', 'utc_offset',
        'gear_id', 'gear_name', 'distance', 'elapsed_time', 'moving_time', 'calories',
        'average_heartrate', 'max_heartrate', 'average_watts', 'max_watts',
        'average_speed', 'max_speed', 'type', 'sport_type', 'total_elevation_gain',
        'kudos_count', 'weighted_average_watts', 'average_cadence', 'trainer',
        'map_polyline', 'device_name', 'timezone', 'start_latlng', 'end_latlng',
        'last_webhook_at', 'last_refresh_at', 'needs_refresh', 'data_loaded_at'
    ]
    
    placeholders = ', '.join(['%s'] * len(mariadb_columns))
    insert_sql = f"INSERT INTO strava_activities ({', '.join(mariadb_columns)}) VALUES ({placeholders})"
    
    migrated_count = 0
    for activity in activities:
        try:
            # Create a dict from the SQLite row
            activity_dict = dict(zip(columns, activity))
            
            # Handle JSON fields (start_latlng, end_latlng)
            start_latlng = activity_dict.get('start_latlng')
            end_latlng = activity_dict.get('end_latlng')
            
            # Convert string representations back to proper JSON
            if start_latlng and isinstance(start_latlng, str):
                try:
                    start_latlng = json.loads(start_latlng) if start_latlng != 'None' else None
                except:
                    start_latlng = None
            
            if end_latlng and isinstance(end_latlng, str):
                try:
                    end_latlng = json.loads(end_latlng) if end_latlng != 'None' else None
                except:
                    end_latlng = None
            
            # Prepare values for MariaDB insert
            values = []
            for col in mariadb_columns:
                if col in activity_dict:
                    value = activity_dict[col]
                    
                    # Handle specific field conversions
                    if col in ['start_date_local', 'start_date']:
                        # Convert datetime fields
                        value = convert_iso_datetime(value)
                    elif col == 'start_latlng':
                        value = json.dumps(start_latlng) if start_latlng else None
                    elif col == 'end_latlng':  
                        value = json.dumps(end_latlng) if end_latlng else None
                    elif col == 'trainer':
                        value = bool(value) if value is not None else False
                    
                    values.append(value)
                else:
                    # Handle new columns not in SQLite
                    if col == 'last_webhook_at':
                        values.append(None)
                    elif col == 'last_refresh_at':
                        values.append('2025-06-16 06:00:00')  # UAE 10am = UTC 6am
                    elif col == 'needs_refresh':
                        values.append(False)
                    elif col == 'data_loaded_at':
                        values.append('2025-06-16 06:00:00')  # UAE 10am = UTC 6am
                    else:
                        values.append(None)
            
            mysql_cursor.execute(insert_sql, values)
            migrated_count += 1
            
            if migrated_count % 100 == 0:
                logging.info(f"Migrated {migrated_count} activities...")
                mysql_conn.commit()
                
        except Exception as e:
            logging.error(f"Error migrating activity {activity_dict.get('id', 'unknown')}: {e}")
            continue
    
    mysql_conn.commit()
    logging.info(f"Successfully migrated {migrated_count} activities")

def verify_migration(mysql_conn):
    """Verify the migration was successful"""
    cursor = mysql_conn.cursor()
    
    # Count activities
    cursor.execute("SELECT COUNT(*) FROM strava_activities")
    activity_count = cursor.fetchone()[0]
    
    # Get sample activities
    cursor.execute("""
        SELECT athlete_name, COUNT(*) as count, 
               MIN(start_date_local) as earliest, 
               MAX(start_date_local) as latest
        FROM strava_activities 
        GROUP BY athlete_name
    """)
    
    athlete_stats = cursor.fetchall()
    
    logging.info(f"Migration verification:")
    logging.info(f"Total activities: {activity_count}")
    
    for athlete, count, earliest, latest in athlete_stats:
        logging.info(f"{athlete}: {count} activities from {earliest} to {latest}")
    
    # Test JSON fields
    cursor.execute("SELECT id, start_latlng, end_latlng FROM strava_activities WHERE start_latlng IS NOT NULL LIMIT 1")
    result = cursor.fetchone()
    if result:
        logging.info(f"JSON test - Activity {result[0]}: start_latlng={result[1]}, end_latlng={result[2]}")

def main():
    """Main migration function"""
    try:
        logging.info("Starting SQLite to MariaDB migration...")
        
        # Connect to databases
        logging.info("Connecting to SQLite database...")
        sqlite_conn = get_sqlite_connection()
        
        logging.info("Connecting to MariaDB database...")
        mysql_conn = get_mariadb_connection()
        
        # Create tables
        logging.info("Creating MariaDB tables...")
        create_mariadb_tables(mysql_conn)
        
        # Migrate data
        logging.info("Migrating strava_activities data...")
        migrate_strava_activities(sqlite_conn, mysql_conn)
        
        # Verify migration
        logging.info("Verifying migration...")
        verify_migration(mysql_conn)
        
        # Close connections
        sqlite_conn.close()
        mysql_conn.close()
        
        logging.info("✅ Migration completed successfully!")
        logging.info("You can now access your Strava data via phpMyAdmin!")
        
    except Exception as e:
        logging.error(f"❌ Migration failed: {e}")
        raise

if __name__ == "__main__":
    main()