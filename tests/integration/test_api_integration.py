"""
Integration tests for API interactions.

These tests verify that the Strava API client works correctly with real
or mocked API responses, testing the complete flow from API calls to
data parsing and storage.
"""

import pytest
import asyncio
import json
from unittest.mock import Mock, patch, AsyncMock
from datetime import datetime, timedelta

from src.utils.config import AthleteConfig, DatabaseConfig, EmailConfig, Config
from src.api.strava_client import StravaAPIClient
from src.api.models import StravaActivity
from src.database.manager import DatabaseManager
from src.notifications.email_service import EmailService
from src.main import StravaDataFetcher
from src.utils.error_handling import APIError, DatabaseError, EmailError


@pytest.fixture
def sample_athlete_config():
    """Create a sample athlete configuration for testing."""
    return AthleteConfig(
        name="Test Athlete",
        athlete_id=12345,
        access_token="test_access_token",
        refresh_token="test_refresh_token",
        expires_at=int((datetime.now() + timedelta(hours=1)).timestamp()),
        email="test@example.com"
    )


@pytest.fixture
def sample_database_config():
    """Create a sample database configuration for testing."""
    return DatabaseConfig(
        host="localhost",
        port=3306,
        user="test_user",
        password="test_password",
        database="test_db"
    )


@pytest.fixture
def sample_email_config():
    """Create a sample email configuration for testing."""
    return EmailConfig(
        api_key="test_api_key",
        from_email="test@example.com",
        from_name="Test Sender"
    )


@pytest.fixture
def sample_config(sample_athlete_config, sample_database_config, sample_email_config):
    """Create a complete sample configuration for testing."""
    return Config(
        athletes=[sample_athlete_config],
        database=sample_database_config,
        email=sample_email_config
    )


@pytest.fixture
def sample_strava_activity_data():
    """Create sample Strava activity data for testing."""
    return {
        "id": 123456789,
        "name": "Morning Run",
        "start_date": "2024-01-15T08:00:00Z",
        "start_date_local": "2024-01-15T09:00:00",
        "utc_offset": 3600.0,
        "distance": 5000.0,
        "moving_time": 1800,
        "elapsed_time": 1900,
        "total_elevation_gain": 100.0,
        "type": "Run",
        "sport_type": "Run",
        "gear_id": "g123",
        "average_speed": 2.78,
        "max_speed": 4.5,
        "average_heartrate": 150.0,
        "max_heartrate": 180.0,
        "calories": 300.0,
        "kudos_count": 5,
        "trainer": False,
        "map": {
            "summary_polyline": "test_polyline"
        },
        "start_latlng": [40.7128, -74.0060],
        "end_latlng": [40.7130, -74.0058]
    }


class TestStravaAPIClientIntegration:
    """Integration tests for Strava API client."""
    
    @pytest.mark.asyncio
    async def test_fetch_and_parse_activities_success(self, sample_athlete_config, sample_strava_activity_data):
        """Test successful activity fetching and parsing."""
        with patch('aiohttp.ClientSession') as mock_session_class:
            # Mock the session and response
            mock_session = AsyncMock()
            mock_session_class.return_value = mock_session
            
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json.return_value = [sample_strava_activity_data]
            
            mock_session.get.return_value.__aenter__.return_value = mock_response
            
            # Test the API client
            async with StravaAPIClient(sample_athlete_config) as client:
                activities = await client.fetch_and_parse_activities(limit=1)
                
                assert len(activities) == 1
                assert isinstance(activities[0], StravaActivity)
                assert activities[0].id == 123456789
                assert activities[0].name == "Morning Run"
                assert activities[0].athlete_name == "Test Athlete"
    
    @pytest.mark.asyncio
    async def test_fetch_activity_by_id_success(self, sample_athlete_config, sample_strava_activity_data):
        """Test successful single activity fetching."""
        with patch('aiohttp.ClientSession') as mock_session_class:
            mock_session = AsyncMock()
            mock_session_class.return_value = mock_session
            
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json.return_value = sample_strava_activity_data
            
            mock_session.get.return_value.__aenter__.return_value = mock_response
            
            async with StravaAPIClient(sample_athlete_config) as client:
                activity = await client.fetch_activity_by_id(123456789)
                
                assert activity is not None
                assert isinstance(activity, StravaActivity)
                assert activity.id == 123456789
                assert activity.name == "Morning Run"
    
    @pytest.mark.asyncio
    async def test_api_rate_limiting(self, sample_athlete_config):
        """Test API rate limiting behavior."""
        with patch('aiohttp.ClientSession') as mock_session_class:
            mock_session = AsyncMock()
            mock_session_class.return_value = mock_session
            
            # Mock rate limit response
            mock_response = AsyncMock()
            mock_response.status = 429
            
            mock_session.get.return_value.__aenter__.return_value = mock_response
            
            async with StravaAPIClient(sample_athlete_config) as client:
                with pytest.raises(APIError) as exc_info:
                    await client.get_activity(123456789)
                
                assert "rate limit" in str(exc_info.value).lower()
    
    @pytest.mark.asyncio
    async def test_authentication_error(self, sample_athlete_config):
        """Test authentication error handling."""
        with patch('aiohttp.ClientSession') as mock_session_class:
            mock_session = AsyncMock()
            mock_session_class.return_value = mock_session
            
            # Mock authentication error response
            mock_response = AsyncMock()
            mock_response.status = 401
            
            mock_session.get.return_value.__aenter__.return_value = mock_response
            
            async with StravaAPIClient(sample_athlete_config) as client:
                with pytest.raises(APIError) as exc_info:
                    await client.get_activity(123456789)
                
                assert "authentication" in str(exc_info.value).lower()


class TestDatabaseIntegration:
    """Integration tests for database operations."""
    
    def test_save_and_retrieve_activities(self, sample_database_config, sample_strava_activity_data):
        """Test saving and retrieving activities from database."""
        # Create a StravaActivity object
        activity = StravaActivity.from_strava_api(
            sample_strava_activity_data,
            "Test Athlete",
            12345
        )
        
        with patch('mysql.connector.connect') as mock_connect:
            # Mock database connection and cursor
            mock_connection = Mock()
            mock_cursor = Mock()
            mock_cursor.rowcount = 1
            mock_cursor.fetchall.return_value = [
                (
                    123456789, 12345, "Test Athlete", "Morning Run",
                    "2024-01-15 09:00:00", 5.0, 100.0, 300,
                    "Run", "Run"
                )
            ]
            
            mock_connection.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_connection
            
            # Test database operations
            db_manager = DatabaseManager(sample_database_config)
            
            # Test saving activities
            affected_rows = db_manager.save_activities([activity])
            assert affected_rows == 1
            
            # Test retrieving recent activities
            recent_activities = db_manager.get_recent_activities(limit=1)
            assert len(recent_activities) == 1
            assert recent_activities[0]['id'] == 123456789
    
    def test_database_connection_error(self, sample_database_config):
        """Test database connection error handling."""
        with patch('mysql.connector.connect') as mock_connect:
            # Mock connection failure
            mock_connect.side_effect = Exception("Connection failed")
            
            db_manager = DatabaseManager(sample_database_config)
            
            with pytest.raises(DatabaseError):
                db_manager.get_connection()
    
    def test_activity_summary_generation(self, sample_database_config):
        """Test activity summary generation."""
        with patch('mysql.connector.connect') as mock_connect:
            mock_connection = Mock()
            mock_cursor = Mock()
            mock_cursor.fetchall.return_value = [
                ("Test Athlete", 5, 25.5, 500.0, 1500)
            ]
            
            mock_connection.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_connection
            
            db_manager = DatabaseManager(sample_database_config)
            summary = db_manager.get_activity_summary()
            
            assert "Test Athlete" in summary
            assert summary["Test Athlete"]["total_activities"] == 5
            assert summary["Test Athlete"]["total_distance_km"] == 25.5


class TestEmailIntegration:
    """Integration tests for email service."""
    
    def test_send_activity_notification_success(self, sample_email_config, sample_strava_activity_data):
        """Test successful activity notification email."""
        with patch('requests.post') as mock_post:
            # Mock successful email response
            mock_response = Mock()
            mock_response.status_code = 201
            mock_post.return_value = mock_response
            
            email_service = EmailService(sample_email_config)
            
            # Convert to activity format expected by email service
            activity_data = {
                'id': sample_strava_activity_data['id'],
                'name': sample_strava_activity_data['name'],
                'athlete_name': 'Test Athlete',
                'start_date_local': sample_strava_activity_data['start_date_local'],
                'distance_km': sample_strava_activity_data['distance'] / 1000,
                'elevation_m': sample_strava_activity_data['total_elevation_gain'],
                'calories': sample_strava_activity_data['calories'],
                'sport_type': sample_strava_activity_data['sport_type']
            }
            
            success = email_service.send_activity_notification(
                activity_data, 
                'create', 
                'test@example.com'
            )
            
            assert success is True
            mock_post.assert_called_once()
    
    def test_send_summary_email_success(self, sample_email_config):
        """Test successful summary email."""
        with patch('requests.post') as mock_post:
            mock_response = Mock()
            mock_response.status_code = 201
            mock_post.return_value = mock_response
            
            email_service = EmailService(sample_email_config)
            
            summary_data = {
                'period': 'weekly',
                'start_date': '2024-01-01',
                'end_date': '2024-01-07',
                'total_activities': 5,
                'total_distance_km': 25.5,
                'total_elevation_m': 500,
                'total_calories': 1500
            }
            
            success = email_service.send_summary_email(
                summary_data, 
                'test@example.com'
            )
            
            assert success is True
            mock_post.assert_called_once()
    
    def test_email_api_error(self, sample_email_config):
        """Test email API error handling."""
        with patch('requests.post') as mock_post:
            # Mock API error response
            mock_response = Mock()
            mock_response.status_code = 400
            mock_response.text = "Bad request"
            mock_post.return_value = mock_response
            
            email_service = EmailService(sample_email_config)
            
            activity_data = {
                'id': 123,
                'name': 'Test Activity',
                'athlete_name': 'Test Athlete'
            }
            
            success = email_service.send_activity_notification(
                activity_data, 
                'create', 
                'test@example.com'
            )
            
            assert success is False


class TestFullApplicationIntegration:
    """Integration tests for the complete application flow."""
    
    @pytest.mark.asyncio
    async def test_complete_activity_fetch_flow(self, sample_config, sample_strava_activity_data):
        """Test complete flow from API fetch to database storage."""
        with patch('aiohttp.ClientSession') as mock_session_class, \
             patch('mysql.connector.connect') as mock_db_connect:
            
            # Mock API response
            mock_session = AsyncMock()
            mock_session_class.return_value = mock_session
            
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json.return_value = [sample_strava_activity_data]
            
            mock_session.get.return_value.__aenter__.return_value = mock_response
            
            # Mock database
            mock_connection = Mock()
            mock_cursor = Mock()
            mock_cursor.rowcount = 1
            mock_connection.cursor.return_value = mock_cursor
            mock_db_connect.return_value = mock_connection
            
            # Test complete flow
            fetcher = StravaDataFetcher(sample_config)
            athlete_config = sample_config.athletes[0]
            
            count = await fetcher.fetch_activities_for_athlete(athlete_config, limit=1)
            
            assert count == 1
            mock_cursor.execute.assert_called()  # Verify database insert was called
    
    @pytest.mark.asyncio
    async def test_fetch_all_athletes_flow(self, sample_config, sample_strava_activity_data):
        """Test fetching activities for all configured athletes."""
        with patch('aiohttp.ClientSession') as mock_session_class, \
             patch('mysql.connector.connect') as mock_db_connect:
            
            # Mock API response
            mock_session = AsyncMock()
            mock_session_class.return_value = mock_session
            
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json.return_value = [sample_strava_activity_data]
            
            mock_session.get.return_value.__aenter__.return_value = mock_response
            
            # Mock database
            mock_connection = Mock()
            mock_cursor = Mock()
            mock_cursor.rowcount = 1
            mock_connection.cursor.return_value = mock_cursor
            mock_db_connect.return_value = mock_connection
            
            # Test fetching for all athletes
            fetcher = StravaDataFetcher(sample_config)
            results = await fetcher.fetch_all_activities(limit=1)
            
            assert len(results) == 1  # One athlete configured
            assert "Test Athlete" in results
            assert results["Test Athlete"] == 1
    
    def test_summary_email_integration(self, sample_config):
        """Test complete summary email flow."""
        with patch('mysql.connector.connect') as mock_db_connect, \
             patch('requests.post') as mock_email_post:
            
            # Mock database summary data
            mock_connection = Mock()
            mock_cursor = Mock()
            mock_cursor.fetchall.return_value = [
                ("Test Athlete", 5, 25.5, 500.0, 1500)
            ]
            mock_connection.cursor.return_value = mock_cursor
            mock_db_connect.return_value = mock_connection
            
            # Mock email success
            mock_response = Mock()
            mock_response.status_code = 201
            mock_email_post.return_value = mock_response
            
            # Test summary email flow
            fetcher = StravaDataFetcher(sample_config)
            success = fetcher.send_summary_email("weekly")
            
            assert success is True
            mock_email_post.assert_called()
    
    @pytest.mark.asyncio
    async def test_error_propagation_in_integration(self, sample_config):
        """Test that errors are properly propagated through the integration."""
        with patch('aiohttp.ClientSession') as mock_session_class:
            # Mock API error
            mock_session = AsyncMock()
            mock_session_class.return_value = mock_session
            
            mock_response = AsyncMock()
            mock_response.status = 500
            
            mock_session.get.return_value.__aenter__.return_value = mock_response
            
            # Test error propagation
            fetcher = StravaDataFetcher(sample_config)
            athlete_config = sample_config.athletes[0]
            
            with pytest.raises(APIError):
                await fetcher.fetch_activities_for_athlete(athlete_config, limit=1)


class TestWebhookIntegration:
    """Integration tests for webhook processing."""
    
    @pytest.mark.asyncio
    async def test_webhook_activity_create_flow(self, sample_config, sample_strava_activity_data):
        """Test complete webhook activity creation flow."""
        from src.webhook_server import StravaWebhookServer
        
        with patch('aiohttp.ClientSession') as mock_session_class, \
             patch('mysql.connector.connect') as mock_db_connect, \
             patch('requests.post') as mock_email_post:
            
            # Mock API response for activity fetch
            mock_session = AsyncMock()
            mock_session_class.return_value = mock_session
            
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json.return_value = sample_strava_activity_data
            
            mock_session.get.return_value.__aenter__.return_value = mock_response
            
            # Mock database
            mock_connection = Mock()
            mock_cursor = Mock()
            mock_cursor.rowcount = 1
            mock_connection.cursor.return_value = mock_cursor
            mock_db_connect.return_value = mock_connection
            
            # Mock email success
            mock_email_response = Mock()
            mock_email_response.status_code = 201
            mock_email_post.return_value = mock_email_response
            
            # Test webhook processing
            webhook_server = StravaWebhookServer(sample_config)
            
            event_data = {
                'object_type': 'activity',
                'object_id': 123456789,
                'aspect_type': 'create',
                'owner_id': 12345,
                'event_time': int(datetime.now().timestamp())
            }
            
            # Process the webhook event
            await webhook_server._process_webhook_event(event_data)
            
            # Verify database save was called
            mock_cursor.execute.assert_called()
            
            # Verify email was sent
            mock_email_post.assert_called()


if __name__ == '__main__':
    pytest.main([__file__])