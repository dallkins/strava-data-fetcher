"""
Unit tests for Strava API data models.
"""

import pytest
from datetime import datetime
from unittest.mock import MagicMock

from src.api.models import StravaActivity


class TestStravaActivity:
    """Test StravaActivity dataclass"""
    
    def test_strava_activity_creation(self):
        """Test creating StravaActivity with valid data"""
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
        
        assert activity.id == 12345
        assert activity.athlete_id == 67890
        assert activity.athlete_name == "Test Athlete"
        assert activity.name == "Morning Ride"
        assert activity.distance == 25000.0
        assert activity.calories == 500
        assert activity.type == "Ride"
        assert activity.sport_type == "Ride"
    
    def test_strava_activity_from_strava_api_complete(self):
        """Test creating StravaActivity from complete Strava API response"""
        api_data = {
            "id": 12345,
            "name": "Morning Ride",
            "start_date_local": "2024-01-01T08:00:00Z",
            "start_date": "2024-01-01T08:00:00Z",
            "utc_offset": 0,
            "gear_id": "b12345",
            "distance": 25000.0,
            "elapsed_time": 3600,
            "moving_time": 3500,
            "calories": 500.0,
            "average_heartrate": 150.0,
            "max_heartrate": 180.0,
            "average_watts": 200.0,
            "max_watts": 400.0,
            "average_speed": 7.0,
            "max_speed": 15.0,
            "type": "Ride",
            "sport_type": "Ride",
            "total_elevation_gain": 300.0,
            "kudos_count": 5,
            "weighted_average_watts": 210.0,
            "average_cadence": 85.0,
            "trainer": False,
            "map": {
                "summary_polyline": "encoded_polyline"
            },
            "device_name": "Garmin Edge 530",
            "timezone": "(GMT+00:00) Europe/London",
            "start_latlng": [51.5074, -0.1278],
            "end_latlng": [51.5074, -0.1278],
            "gear": {
                "name": "Test Bike"
            }
        }
        
        activity = StravaActivity.from_strava_api(api_data, "Test Athlete", 67890)
        
        assert activity.id == 12345
        assert activity.athlete_id == 67890
        assert activity.athlete_name == "Test Athlete"
        assert activity.name == "Morning Ride"
        assert activity.start_date_local == "2024-01-01T08:00:00Z"
        assert activity.gear_id == "b12345"
        assert activity.gear_name == "Test Bike"
        assert activity.distance == 25000.0
        assert activity.calories == 500
        assert activity.map_polyline == "encoded_polyline"
        assert activity.device_name == "Garmin Edge 530"
        assert activity.timezone == "(GMT+00:00) Europe/London"
        assert activity.start_latlng == [51.5074, -0.1278]
        assert activity.end_latlng == [51.5074, -0.1278]
    
    def test_strava_activity_from_strava_api_minimal(self):
        """Test creating StravaActivity from minimal Strava API response"""
        api_data = {
            "id": 12345,
            "name": "Morning Ride",
            "start_date_local": "2024-01-01T08:00:00Z",
            "start_date": "2024-01-01T08:00:00Z",
            "type": "Ride",
            "sport_type": "Ride"
        }
        
        activity = StravaActivity.from_strava_api(api_data, "Test Athlete", 67890)
        
        assert activity.id == 12345
        assert activity.athlete_id == 67890
        assert activity.athlete_name == "Test Athlete"
        assert activity.name == "Morning Ride"
        assert activity.type == "Ride"
        assert activity.sport_type == "Ride"
        
        # Check default values
        assert activity.utc_offset == 0
        assert activity.gear_id is None
        assert activity.gear_name is None
        assert activity.distance == 0
        assert activity.elapsed_time == 0
        assert activity.moving_time == 0
        assert activity.calories == 0
        assert activity.average_heartrate is None
        assert activity.max_heartrate is None
        assert activity.average_watts is None
        assert activity.max_watts is None
        assert activity.average_speed is None
        assert activity.max_speed is None
        assert activity.total_elevation_gain == 0
        assert activity.kudos_count == 0
        assert activity.weighted_average_watts is None
        assert activity.average_cadence is None
        assert activity.trainer is False
        assert activity.map_polyline is None
        assert activity.device_name is None
        assert activity.timezone is None
        assert activity.start_latlng is None
        assert activity.end_latlng is None
    
    def test_strava_activity_from_strava_api_with_nulls(self):
        """Test creating StravaActivity from API response with null values"""
        api_data = {
            "id": 12345,
            "name": "Morning Ride",
            "start_date_local": "2024-01-01T08:00:00Z",
            "start_date": "2024-01-01T08:00:00Z",
            "type": "Ride",
            "sport_type": "Ride",
            "gear_id": None,
            "distance": None,
            "calories": None,
            "average_heartrate": None,
            "map": None,
            "gear": None,
            "start_latlng": None,
            "end_latlng": None
        }
        
        activity = StravaActivity.from_strava_api(api_data, "Test Athlete", 67890)
        
        assert activity.id == 12345
        assert activity.gear_id is None
        assert activity.gear_name is None
        assert activity.distance == 0  # Should default to 0 for None
        assert activity.calories == 0  # Should default to 0 for None
        assert activity.average_heartrate is None
        assert activity.map_polyline is None
        assert activity.start_latlng is None
        assert activity.end_latlng is None
    
    def test_strava_activity_from_strava_api_gear_handling(self):
        """Test gear name extraction from different gear formats"""
        # Test with gear object
        api_data_with_gear = {
            "id": 12345,
            "name": "Morning Ride",
            "start_date_local": "2024-01-01T08:00:00Z",
            "start_date": "2024-01-01T08:00:00Z",
            "type": "Ride",
            "sport_type": "Ride",
            "gear_id": "b12345",
            "gear": {
                "name": "My Awesome Bike"
            }
        }
        
        activity = StravaActivity.from_strava_api(api_data_with_gear, "Test Athlete", 67890)
        assert activity.gear_name == "My Awesome Bike"
        
        # Test with gear_id but no gear object
        api_data_no_gear = {
            "id": 12345,
            "name": "Morning Ride",
            "start_date_local": "2024-01-01T08:00:00Z",
            "start_date": "2024-01-01T08:00:00Z",
            "type": "Ride",
            "sport_type": "Ride",
            "gear_id": "b12345"
        }
        
        activity = StravaActivity.from_strava_api(api_data_no_gear, "Test Athlete", 67890)
        assert activity.gear_name is None
    
    def test_strava_activity_from_strava_api_map_handling(self):
        """Test map polyline extraction from different map formats"""
        # Test with map object containing summary_polyline
        api_data_with_map = {
            "id": 12345,
            "name": "Morning Ride",
            "start_date_local": "2024-01-01T08:00:00Z",
            "start_date": "2024-01-01T08:00:00Z",
            "type": "Ride",
            "sport_type": "Ride",
            "map": {
                "summary_polyline": "encoded_polyline_data"
            }
        }
        
        activity = StravaActivity.from_strava_api(api_data_with_map, "Test Athlete", 67890)
        assert activity.map_polyline == "encoded_polyline_data"
        
        # Test with empty map object
        api_data_empty_map = {
            "id": 12345,
            "name": "Morning Ride",
            "start_date_local": "2024-01-01T08:00:00Z",
            "start_date": "2024-01-01T08:00:00Z",
            "type": "Ride",
            "sport_type": "Ride",
            "map": {}
        }
        
        activity = StravaActivity.from_strava_api(api_data_empty_map, "Test Athlete", 67890)
        assert activity.map_polyline is None
        
        # Test with no map object
        api_data_no_map = {
            "id": 12345,
            "name": "Morning Ride",
            "start_date_local": "2024-01-01T08:00:00Z",
            "start_date": "2024-01-01T08:00:00Z",
            "type": "Ride",
            "sport_type": "Ride"
        }
        
        activity = StravaActivity.from_strava_api(api_data_no_map, "Test Athlete", 67890)
        assert activity.map_polyline is None
    
    def test_strava_activity_numeric_conversions(self):
        """Test proper handling of numeric conversions"""
        api_data = {
            "id": 12345,
            "name": "Morning Ride",
            "start_date_local": "2024-01-01T08:00:00Z",
            "start_date": "2024-01-01T08:00:00Z",
            "type": "Ride",
            "sport_type": "Ride",
            "distance": "25000.5",  # String that should be converted to float
            "elapsed_time": "3600",  # String that should be converted to int
            "calories": 500.7,  # Float that should be converted to int
            "average_heartrate": "150.5",  # String that should be converted to float
            "trainer": "true"  # String that should be converted to bool
        }
        
        activity = StravaActivity.from_strava_api(api_data, "Test Athlete", 67890)
        
        assert activity.distance == 25000.5
        assert activity.elapsed_time == 3600
        assert activity.calories == 500  # Should be converted to int
        assert activity.average_heartrate == 150.5
        assert activity.trainer is True
    
    def test_strava_activity_boolean_conversions(self):
        """Test proper handling of boolean conversions"""
        test_cases = [
            (True, True),
            (False, False),
            ("true", True),
            ("false", False),
            ("True", True),
            ("False", False),
            (1, True),
            (0, False),
            ("1", True),
            ("0", False),
            ("yes", False),  # Should default to False for unknown strings
            (None, False)
        ]
        
        for input_value, expected in test_cases:
            api_data = {
                "id": 12345,
                "name": "Morning Ride",
                "start_date_local": "2024-01-01T08:00:00Z",
                "start_date": "2024-01-01T08:00:00Z",
                "type": "Ride",
                "sport_type": "Ride",
                "trainer": input_value
            }
            
            activity = StravaActivity.from_strava_api(api_data, "Test Athlete", 67890)
            assert activity.trainer is expected, f"Failed for input: {input_value}"