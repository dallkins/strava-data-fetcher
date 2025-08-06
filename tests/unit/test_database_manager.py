"""
Unit tests for database manager module.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock, call
import mysql.connector
from mysql.connector import Error

from src.database.manager import DatabaseManager
from src.utils.config import DatabaseConfig
from src.api.models import StravaActivity


class TestDatabaseManager:
    """Test DatabaseManager class"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.db_config = DatabaseConfig(
            host="localhost",
            port=3306,
            user="test_user",
            password="test_password",
            database="test_db"
        )
        self.db_manager = DatabaseManager(self.db_config)
    
    @patch('src.database.manager.mysql.connector.connect')
    def test_get_connection_success(self, mock_connect):
        """Test successful database connection"""
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        connection = self.db_manager.get_connection()
        
        assert connection == mock_connection
        mock_connect.assert_called_once_with(
            host="localhost",
            port=3306,
            user="test_user",
            password="test_password",
            database="test_db",
            charset='utf8mb4',
            autocommit=True,
            use_pure=True,
            raise_on_warnings=False
        )
    
    @patch('src.database.manager.mysql.connector.connect')
    def test_get_connection_failure(self, mock_connect):
        """Test database connection failure"""
        mock_connect.side_effect = Error("Connection failed")
        
        with pytest.raises(Error):
            self.db_manager.get_connection()
    
    @patch.object(DatabaseManager, 'get_connection')
    def test_execute_query_select(self, mock_get_connection):
        """Test executing a SELECT query"""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [('row1',), ('row2',)]
        mock_get_connection.return_value = mock_connection
        
        result = self.db_manager.execute_query(
            "SELECT * FROM test_table", 
            fetch_all=True
        )
        
        assert result == [('row1',), ('row2',)]
        mock_cursor.execute.assert_called_once_with("SELECT * FROM test_table", ())
        mock_cursor.fetchall.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()
    
    @patch.object(DatabaseManager, 'get_connection')
    def test_execute_query_select_one(self, mock_get_connection):
        """Test executing a SELECT query with fetch_one"""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = ('single_row',)
        mock_get_connection.return_value = mock_connection
        
        result = self.db_manager.execute_query(
            "SELECT * FROM test_table WHERE id = %s", 
            params=(1,),
            fetch_one=True
        )
        
        assert result == ('single_row',)
        mock_cursor.execute.assert_called_once_with("SELECT * FROM test_table WHERE id = %s", (1,))
        mock_cursor.fetchone.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()
    
    @patch.object(DatabaseManager, 'get_connection')
    def test_execute_query_insert(self, mock_get_connection):
        """Test executing an INSERT query"""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.rowcount = 1
        mock_get_connection.return_value = mock_connection
        
        result = self.db_manager.execute_query(
            "INSERT INTO test_table (name) VALUES (%s)", 
            params=("test_name",)
        )
        
        assert result == 1
        mock_cursor.execute.assert_called_once_with("INSERT INTO test_table (name) VALUES (%s)", ("test_name",))
        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()
    
    @patch.object(DatabaseManager, 'get_connection')
    def test_execute_query_error_handling(self, mock_get_connection):
        """Test query execution error handling"""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.execute.side_effect = Error("Query failed")
        mock_get_connection.return_value = mock_connection
        
        with pytest.raises(Error):
            self.db_manager.execute_query("SELECT * FROM test_table")
        
        # Ensure cleanup happens even on error
        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()
    
    @patch.object(DatabaseManager, 'execute_query')
    def test_test_connection_success(self, mock_execute_query):
        """Test successful connection test"""
        mock_execute_query.return_value = (1,)
        
        result = self.db_manager.test_connection()
        
        assert result is True
        mock_execute_query.assert_called_once_with('SELECT 1', fetch_one=True)
    
    @patch.object(DatabaseManager, 'execute_query')
    def test_test_connection_failure(self, mock_execute_query):
        """Test failed connection test"""
        mock_execute_query.side_effect = Error("Connection test failed")
        
        result = self.db_manager.test_connection()
        
        assert result is False
    
    @patch.object(DatabaseManager, 'get_cursor')
    def test_save_activities_single(self, mock_get_cursor):
        """Test saving a single activity"""
        # Mock the cursor context manager
        mock_cursor = Mock()
        mock_cursor.execute.return_value = None
        mock_cursor.rowcount = 1
        mock_get_cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_cursor.return_value.__exit__.return_value = None
        activity = StravaActivity(
            id=12345,
            athlete_id=67890,
            athlete_name="Test Athlete",
            name="Morning Ride",
            start_date_local="2024-01-01 08:00:00",
            start_date="2024-01-01 08:00:00",
            utc_offset=0,
            gear_id="b12345",
            gear_name="Test Bike",
            distance=25000.0,
            elapsed_time=3600,
            moving_time=3500,
            calories=500,
            average_heartrate=150.0,
            max_heartrate=180.0,
            average_watts=200.0,
            max_watts=400.0,
            average_speed=7.0,
            max_speed=15.0,
            type="Ride",
            sport_type="Ride",
            total_elevation_gain=300.0,
            kudos_count=5,
            weighted_average_watts=210.0,
            average_cadence=85.0,
            trainer=False,
            map_polyline="encoded_polyline",
            device_name="Garmin Edge",
            timezone="UTC",
            start_latlng=[51.5074, -0.1278],
            end_latlng=[51.5074, -0.1278]
        )
        
        result = self.db_manager.save_activities([activity])
        
        assert result == 1
        mock_get_cursor.assert_called_once()
        
        # Verify the SQL query structure
        call_args = mock_cursor.execute.call_args
        assert "INSERT INTO strava_activities" in call_args[0][0]
        assert "ON DUPLICATE KEY UPDATE" in call_args[0][0]
    
    @patch.object(DatabaseManager, 'get_cursor')
    def test_save_activities_multiple(self, mock_get_cursor):
        """Test saving multiple activities"""
        # Mock the cursor context manager
        mock_cursor = Mock()
        mock_cursor.execute.return_value = None
        mock_cursor.rowcount = 1  # Each execute returns 1 row affected
        mock_get_cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_cursor.return_value.__exit__.return_value = None
        activities = [
            StravaActivity(
                id=12345,
                athlete_id=67890,
                athlete_name="Test Athlete",
                name="Morning Ride",
                start_date_local="2024-01-01 08:00:00",
                start_date="2024-01-01 08:00:00",
                utc_offset=0,
                gear_id=None,
                gear_name=None,
                distance=25000.0,
                elapsed_time=3600,
                moving_time=3500,
                calories=500,
                average_heartrate=None,
                max_heartrate=None,
                average_watts=None,
                max_watts=None,
                average_speed=None,
                max_speed=None,
                type="Ride",
                sport_type="Ride",
                total_elevation_gain=300.0,
                kudos_count=5,
                weighted_average_watts=None,
                average_cadence=None,
                trainer=False,
                map_polyline=None,
                device_name=None,
                timezone=None,
                start_latlng=None,
                end_latlng=None
            ),
            StravaActivity(
                id=12346,
                athlete_id=67890,
                athlete_name="Test Athlete",
                name="Evening Ride",
                start_date_local="2024-01-01 18:00:00",
                start_date="2024-01-01 18:00:00",
                utc_offset=0,
                gear_id=None,
                gear_name=None,
                distance=15000.0,
                elapsed_time=2400,
                moving_time=2300,
                calories=300,
                average_heartrate=None,
                max_heartrate=None,
                average_watts=None,
                max_watts=None,
                average_speed=None,
                max_speed=None,
                type="Ride",
                sport_type="Ride",
                total_elevation_gain=200.0,
                kudos_count=3,
                weighted_average_watts=None,
                average_cadence=None,
                trainer=False,
                map_polyline=None,
                device_name=None,
                timezone=None,
                start_latlng=None,
                end_latlng=None
            )
        ]
        
        result = self.db_manager.save_activities(activities)
        
        assert result == 2
        mock_get_cursor.assert_called_once()
    
    @patch.object(DatabaseManager, 'get_cursor')
    def test_save_activities_empty_list(self, mock_get_cursor):
        """Test saving empty activities list"""
        result = self.db_manager.save_activities([])
        
        assert result == 0
        mock_get_cursor.assert_not_called()
    
    @patch.object(DatabaseManager, 'execute_query')
    def test_get_activity_summary_all_athletes(self, mock_execute_query):
        """Test getting activity summary for all athletes"""
        mock_execute_query.return_value = [
            ("Dominic", 50, 1250.5, 15000.0, 25000),
            ("Clare", 30, 750.2, 10000.0, 18000)
        ]
        
        result = self.db_manager.get_activity_summary()
        
        expected = {
            "Dominic": {
                "total_activities": 50,
                "total_distance_km": 1250.5,
                "total_elevation_m": 15000.0,
                "total_calories": 25000
            },
            "Clare": {
                "total_activities": 30,
                "total_distance_km": 750.2,
                "total_elevation_m": 10000.0,
                "total_calories": 18000
            }
        }
        
        assert result == expected
        mock_execute_query.assert_called_once()
    
    @patch.object(DatabaseManager, 'execute_query')
    def test_get_activity_summary_specific_athlete(self, mock_execute_query):
        """Test getting activity summary for specific athlete"""
        mock_execute_query.return_value = [
            ("Dominic", 50, 1250.5, 15000.0, 25000)
        ]
        
        result = self.db_manager.get_activity_summary(athlete_name="Dominic")
        
        expected = {
            "Dominic": {
                "total_activities": 50,
                "total_distance_km": 1250.5,
                "total_elevation_m": 15000.0,
                "total_calories": 25000
            }
        }
        
        assert result == expected
        
        # Verify the WHERE clause was added
        call_args = mock_execute_query.call_args
        assert "WHERE LOWER(athlete_name) = LOWER(%s)" in call_args[0][0]
        assert call_args[1]['params'] == ("Dominic",)
    
    @patch.object(DatabaseManager, 'execute_query')
    def test_get_activity_summary_date_range(self, mock_execute_query):
        """Test getting activity summary with date range"""
        mock_execute_query.return_value = [
            ("Dominic", 10, 250.5, 3000.0, 5000)
        ]
        
        result = self.db_manager.get_activity_summary(
            start_date="2024-01-01",
            end_date="2024-01-31"
        )
        
        expected = {
            "Dominic": {
                "total_activities": 10,
                "total_distance_km": 250.5,
                "total_elevation_m": 3000.0,
                "total_calories": 5000
            }
        }
        
        assert result == expected
        
        # Verify date filters were added
        call_args = mock_execute_query.call_args
        query = call_args[0][0]
        assert "DATE(start_date_local) >= %s" in query
        assert "DATE(start_date_local) <= %s" in query
        assert call_args[1]['params'] == ("2024-01-01", "2024-01-31")
    
    @patch.object(DatabaseManager, 'execute_query')
    def test_get_activity_summary_error_handling(self, mock_execute_query):
        """Test activity summary error handling"""
        mock_execute_query.side_effect = Error("Query failed")
        
        result = self.db_manager.get_activity_summary()
        
        assert result == {}
    
    @patch.object(DatabaseManager, 'execute_query')
    def test_get_recent_activities(self, mock_execute_query):
        """Test getting recent activities"""
        mock_execute_query.return_value = [
            (12345, "Morning Ride", "2024-01-01 08:00:00", 25.0, "Dominic"),
            (12346, "Evening Ride", "2024-01-01 18:00:00", 15.0, "Clare")
        ]
        
        result = self.db_manager.get_recent_activities(limit=10)
        
        expected = [
            {
                "id": 12345,
                "name": "Morning Ride",
                "start_date_local": "2024-01-01 08:00:00",
                "distance_km": 25.0,
                "athlete_name": "Dominic"
            },
            {
                "id": 12346,
                "name": "Evening Ride",
                "start_date_local": "2024-01-01 18:00:00",
                "distance_km": 15.0,
                "athlete_name": "Clare"
            }
        ]
        
        assert result == expected
        
        # Verify LIMIT was applied
        call_args = mock_execute_query.call_args
        assert "LIMIT %s" in call_args[0][0]
        assert call_args[1]['params'] == (10,)
    
    @patch.object(DatabaseManager, 'execute_query')
    def test_get_recent_activities_with_athlete_filter(self, mock_execute_query):
        """Test getting recent activities for specific athlete"""
        mock_execute_query.return_value = [
            (12345, "Morning Ride", "2024-01-01 08:00:00", 25.0, "Dominic")
        ]
        
        result = self.db_manager.get_recent_activities(athlete_name="Dominic", limit=5)
        
        # Verify athlete filter was applied
        call_args = mock_execute_query.call_args
        query = call_args[0][0]
        assert "WHERE LOWER(athlete_name) = LOWER(%s)" in query
        assert "LIMIT %s" in query
        assert call_args[1]['params'] == ("Dominic", 5)
    
    @patch.object(DatabaseManager, 'execute_query')
    def test_activity_exists_true(self, mock_execute_query):
        """Test checking if activity exists - returns True"""
        mock_execute_query.return_value = (1,)
        
        result = self.db_manager.activity_exists(12345)
        
        assert result is True
        mock_execute_query.assert_called_once_with(
            "SELECT COUNT(*) FROM strava_activities WHERE id = %s",
            params=(12345,),
            fetch_one=True
        )
    
    @patch.object(DatabaseManager, 'execute_query')
    def test_activity_exists_false(self, mock_execute_query):
        """Test checking if activity exists - returns False"""
        mock_execute_query.return_value = (0,)
        
        result = self.db_manager.activity_exists(12345)
        
        assert result is False
    
    @patch.object(DatabaseManager, 'execute_query')
    def test_activity_exists_error(self, mock_execute_query):
        """Test activity exists error handling"""
        mock_execute_query.side_effect = Error("Query failed")
        
        result = self.db_manager.activity_exists(12345)
        
        assert result is False