"""
Database manager for Strava activity data persistence.

This module provides database operations for storing and retrieving
Strava activity data with proper connection management and error handling.
"""

import json
from typing import List, Dict, Any, Optional, Tuple
from contextlib import contextmanager

import mysql.connector
from mysql.connector import Error

from ..api.models import StravaActivity
from ..utils.config import DatabaseConfig
from ..utils.logging_config import get_logger, PerformanceTimer
from ..utils.error_handling import DatabaseError, handle_errors
from ..utils.cache import get_cache_manager, cache_database_result

logger = get_logger(__name__)


class DatabaseManager:
    """
    Manages database operations for Strava activity data.
    
    Provides methods for saving activities, retrieving summaries,
    and managing database connections with proper error handling.
    """
    
    def __init__(self, config: DatabaseConfig):
        """
        Initialize the database manager.
        
        Args:
            config: Database configuration
        """
        self.config = config
        self._connection_config = {
            'host': config.host,
            'port': config.port,
            'user': config.user,
            'password': config.password,
            'database': config.database,
            'charset': 'utf8mb4',
            'autocommit': True,
            'use_pure': True,
            'raise_on_warnings': False
        }
        
        # Caching
        self.cache_manager = get_cache_manager()
        self.activity_cache = self.cache_manager.get_activity_cache()
    
    def get_connection(self) -> mysql.connector.MySQLConnection:
        """
        Get a database connection.
        
        Returns:
            MySQL connection object
            
        Raises:
            Error: If connection fails
        """
        try:
            connection = mysql.connector.connect(**self._connection_config)
            return connection
        except Error as e:
            raise DatabaseError(f"Database connection failed: {e}", operation="connect", original_error=e)
    
    @contextmanager
    def get_cursor(self):
        """
        Context manager for database operations.
        
        Yields:
            Database cursor with automatic cleanup
        """
        connection = None
        cursor = None
        try:
            connection = self.get_connection()
            cursor = connection.cursor(buffered=True)
            yield cursor
        finally:
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
            if connection:
                try:
                    connection.close()
                except:
                    pass
    
    def execute_query(
        self, 
        query: str, 
        params: Optional[Tuple] = None,
        fetch_one: bool = False,
        fetch_all: bool = False
    ) -> Any:
        """
        Execute a database query with automatic connection management.
        
        Args:
            query: SQL query string
            params: Query parameters
            fetch_one: Whether to fetch one result
            fetch_all: Whether to fetch all results
            
        Returns:
            Query result or row count
        """
        with self.get_cursor() as cursor:
            cursor.execute(query, params or ())
            
            if fetch_one:
                return cursor.fetchone()
            elif fetch_all:
                return cursor.fetchall()
            else:
                return cursor.rowcount
    
    @handle_errors(default_return=False, log_errors=True)
    def test_connection(self) -> bool:
        """
        Test database connectivity.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            result = self.execute_query('SELECT 1', fetch_one=True)
            return result is not None
        except Exception as e:
            raise DatabaseError(f"Database connection test failed: {e}", operation="test_connection", original_error=e)
    
    def save_activities(self, activities: List[StravaActivity]) -> int:
        """
        Save multiple activities to the database.
        
        Args:
            activities: List of StravaActivity objects
            
        Returns:
            Number of rows affected
        """
        if not activities:
            return 0
        
        with PerformanceTimer(f"Save {len(activities)} activities to database"):
            try:
                with self.get_cursor() as cursor:
                    insert_sql = '''
                        INSERT INTO strava_activities
                        (id, athlete_id, athlete_name, name, start_date_local, start_date, utc_offset,
                         gear_id, gear_name, distance, elapsed_time, moving_time, calories,
                         average_heartrate, max_heartrate, average_watts, max_watts,
                         average_speed, max_speed, type, sport_type, total_elevation_gain,
                         kudos_count, weighted_average_watts, average_cadence, trainer,
                         map_polyline, device_name, timezone, start_latlng, end_latlng,
                         created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                NOW(), NOW())
                        ON DUPLICATE KEY UPDATE
                            name = VALUES(name),
                            gear_name = VALUES(gear_name),
                            calories = VALUES(calories),
                            kudos_count = VALUES(kudos_count),
                            distance = VALUES(distance),
                            moving_time = VALUES(moving_time),
                            total_elevation_gain = VALUES(total_elevation_gain),
                            updated_at = NOW()
                    '''
                    
                    total_affected = 0
                    for activity in activities:
                        try:
                            # Convert latlng to JSON strings
                            start_latlng_json = json.dumps(activity.start_latlng) if activity.start_latlng else None
                            end_latlng_json = json.dumps(activity.end_latlng) if activity.end_latlng else None
                            
                            values = (
                                activity.id, activity.athlete_id, activity.athlete_name,
                                activity.name, activity.start_date_local, activity.start_date,
                                activity.utc_offset, activity.gear_id, activity.gear_name,
                                activity.distance, activity.elapsed_time, activity.moving_time,
                                activity.calories, activity.average_heartrate, activity.max_heartrate,
                                activity.average_watts, activity.max_watts, activity.average_speed,
                                activity.max_speed, activity.type, activity.sport_type,
                                activity.total_elevation_gain, activity.kudos_count,
                                activity.weighted_average_watts, activity.average_cadence,
                                activity.trainer, activity.map_polyline, activity.device_name,
                                activity.timezone, start_latlng_json, end_latlng_json
                            )
                            
                            cursor.execute(insert_sql, values)
                            total_affected += cursor.rowcount
                            
                        except Exception as e:
                            logger.error(f"Error saving activity {activity.id}: {e}")
                            continue
                    
                    logger.info(f"Saved {len(activities)} activities, {total_affected} rows affected")
                    return total_affected
                    
            except Exception as e:
                raise DatabaseError(f"Error saving activities: {e}", operation="save_activities", original_error=e)
    
    def get_activity_summary(
        self,
        athlete_name: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Dict[str, Dict[str, Any]]:
        """
        Get activity summary statistics.
        
        Args:
            athlete_name: Filter by athlete name
            start_date: Start date filter (YYYY-MM-DD)
            end_date: End date filter (YYYY-MM-DD)
            
        Returns:
            Dictionary with athlete statistics
        """
        with PerformanceTimer(f"Get activity summary for {athlete_name or 'all athletes'}"):
            # Create cache key for this specific query
            date_range = f"{start_date or 'all'}_{end_date or 'all'}"
            athlete_key = athlete_name or "all"
            
            # Check cache first for expensive summary queries
            if athlete_name:
                # For specific athlete, use athlete cache
                cached_summary = self.activity_cache.get_summary(
                    hash(athlete_name), date_range
                )
                if cached_summary is not None:
                    logger.debug(f"Cache hit for activity summary: {athlete_name}")
                    return {athlete_name: cached_summary}
            
            try:
                # Build WHERE conditions
                where_conditions = []
                params = []
                
                if athlete_name:
                    where_conditions.append("LOWER(athlete_name) = LOWER(%s)")
                    params.append(athlete_name)
                
                if start_date:
                    where_conditions.append("DATE(start_date_local) >= %s")
                    params.append(start_date)
                
                if end_date:
                    where_conditions.append("DATE(start_date_local) <= %s")
                    params.append(end_date)
                
                where_clause = ""
                if where_conditions:
                    where_clause = "WHERE " + " AND ".join(where_conditions)
                
                query = f'''
                    SELECT
                        athlete_name,
                        COUNT(*) as total_activities,
                        COALESCE(SUM(distance), 0) / 1000 as total_distance_km,
                        COALESCE(SUM(total_elevation_gain), 0) as total_elevation_m,
                        COALESCE(SUM(calories), 0) as total_calories
                    FROM strava_activities
                    {where_clause}
                    GROUP BY athlete_name
                    ORDER BY athlete_name
                '''
                
                results = self.execute_query(query, params=tuple(params), fetch_all=True)
                
                summary = {}
                for row in results:
                    athlete, activities, distance_km, elevation_m, calories = row
                    athlete_summary = {
                        'total_activities': activities,
                        'total_distance_km': round(distance_km, 1),
                        'total_elevation_m': round(elevation_m),
                        'total_calories': int(calories)
                    }
                    summary[athlete] = athlete_summary
                    
                    # Cache individual athlete summaries
                    self.activity_cache.set_summary(
                        hash(athlete), date_range, athlete_summary, ttl=1800
                    )
                
                return summary
                
            except Exception as e:
                raise DatabaseError(f"Error getting activity summary: {e}", operation="get_activity_summary", original_error=e)
    
    @cache_database_result(ttl=300)  # Cache for 5 minutes
    def get_recent_activities(
        self,
        limit: int = 10,
        athlete_name: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get recent activities.
        
        Args:
            limit: Maximum number of activities to return
            athlete_name: Filter by athlete name
            
        Returns:
            List of recent activity data
        """
        try:
            where_clause = ""
            params = []
            
            if athlete_name:
                where_clause = "WHERE LOWER(athlete_name) = LOWER(%s)"
                params.append(athlete_name)
            
            query = f'''
                SELECT id, name, start_date_local, distance/1000 as distance_km, athlete_name
                FROM strava_activities
                {where_clause}
                ORDER BY start_date_local DESC
                LIMIT %s
            '''
            
            params.append(limit)
            results = self.execute_query(query, params=tuple(params), fetch_all=True)
            
            activities = []
            for row in results:
                activity_id, name, start_date, distance_km, athlete = row
                activities.append({
                    'id': activity_id,
                    'name': name,
                    'start_date_local': start_date,
                    'distance_km': round(distance_km, 1),
                    'athlete_name': athlete
                })
            
            return activities
            
        except Exception as e:
            raise DatabaseError(f"Error getting recent activities: {e}", operation="get_recent_activities", original_error=e)
    
    def activity_exists(self, activity_id: int) -> bool:
        """
        Check if an activity exists in the database.
        
        Args:
            activity_id: Strava activity ID
            
        Returns:
            True if activity exists, False otherwise
        """
        try:
            result = self.execute_query(
                "SELECT COUNT(*) FROM strava_activities WHERE id = %s",
                params=(activity_id,),
                fetch_one=True
            )
            return (result[0] if result else 0) > 0
        except Exception as e:
            raise DatabaseError(f"Error checking if activity exists: {e}", operation="activity_exists", original_error=e)
    
    def get_activity_by_id(self, activity_id: int) -> Optional[Dict[str, Any]]:
        """
        Get a specific activity by ID.
        
        Args:
            activity_id: Strava activity ID
            
        Returns:
            Activity data or None if not found
        """
        try:
            result = self.execute_query(
                '''
                SELECT id, athlete_id, athlete_name, name, start_date_local, 
                       distance/1000 as distance_km, total_elevation_gain, calories,
                       type, sport_type
                FROM strava_activities 
                WHERE id = %s
                ''',
                params=(activity_id,),
                fetch_one=True
            )
            
            if result:
                return {
                    'id': result[0],
                    'athlete_id': result[1],
                    'athlete_name': result[2],
                    'name': result[3],
                    'start_date_local': result[4],
                    'distance_km': round(result[5], 1),
                    'elevation_m': round(result[6]),
                    'calories': int(result[7]),
                    'type': result[8],
                    'sport_type': result[9]
                }
            return None
            
        except Exception as e:
            raise DatabaseError(f"Error getting activity by ID: {e}", operation="get_activity_by_id", original_error=e)
    
    def delete_activity(self, activity_id: int) -> bool:
        """
        Delete an activity from the database.
        
        Args:
            activity_id: Strava activity ID
            
        Returns:
            True if deleted, False otherwise
        """
        try:
            affected_rows = self.execute_query(
                "DELETE FROM strava_activities WHERE id = %s",
                params=(activity_id,)
            )
            return affected_rows > 0
        except Exception as e:
            raise DatabaseError(f"Error deleting activity: {e}", operation="delete_activity", original_error=e)