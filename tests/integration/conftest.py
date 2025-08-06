"""
Configuration and fixtures for integration tests.

This module provides shared fixtures and configuration for integration tests
that verify component interactions and end-to-end functionality.
"""

import pytest
import os
import tempfile
from unittest.mock import Mock, patch
from datetime import datetime, timedelta

from src.utils.config import AthleteConfig, DatabaseConfig, EmailConfig, Config


@pytest.fixture(scope="session")
def test_athlete_config():
    """Create a test athlete configuration."""
    return AthleteConfig(
        name="Integration Test Athlete",
        athlete_id=99999,
        access_token="integration_test_token",
        refresh_token="integration_test_refresh",
        expires_at=int((datetime.now() + timedelta(hours=6)).timestamp()),
        email="integration.test@example.com"
    )


@pytest.fixture(scope="session")
def test_database_config():
    """Create a test database configuration."""
    return DatabaseConfig(
        host="localhost",
        port=3306,
        user="test_user",
        password="test_password",
        database="test_strava_db"
    )


@pytest.fixture(scope="session")
def test_email_config():
    """Create a test email configuration."""
    return EmailConfig(
        api_key="test_brevo_api_key",
        from_email="noreply@example.com",
        from_name="Strava Data Fetcher Test"
    )


@pytest.fixture(scope="session")
def test_config(test_athlete_config, test_database_config, test_email_config):
    """Create a complete test configuration."""
    return Config(
        athletes=[test_athlete_config],
        database=test_database_config,
        email=test_email_config
    )


@pytest.fixture
def sample_activity_response():
    """Sample Strava API activity response."""
    return {
        "id": 987654321,
        "name": "Integration Test Run",
        "start_date": "2024-01-20T10:00:00Z",
        "start_date_local": "2024-01-20T11:00:00",
        "utc_offset": 3600.0,
        "distance": 8000.0,
        "moving_time": 2400,
        "elapsed_time": 2500,
        "total_elevation_gain": 150.0,
        "type": "Run",
        "sport_type": "Run",
        "gear_id": "g456",
        "average_speed": 3.33,
        "max_speed": 5.0,
        "average_heartrate": 155.0,
        "max_heartrate": 185.0,
        "calories": 450.0,
        "kudos_count": 8,
        "trainer": False,
        "device_name": "Garmin Forerunner",
        "map": {
            "summary_polyline": "integration_test_polyline"
        },
        "start_latlng": [51.5074, -0.1278],
        "end_latlng": [51.5076, -0.1275],
        "timezone": "(GMT+01:00) Europe/London"
    }


@pytest.fixture
def sample_activities_list_response(sample_activity_response):
    """Sample Strava API activities list response."""
    return [sample_activity_response]


@pytest.fixture
def mock_database_connection():
    """Mock database connection for integration tests."""
    with patch('mysql.connector.connect') as mock_connect:
        mock_connection = Mock()
        mock_cursor = Mock()
        
        # Configure cursor behavior
        mock_cursor.rowcount = 1
        mock_cursor.fetchone.return_value = (1,)
        mock_cursor.fetchall.return_value = []
        
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        yield {
            'connection': mock_connection,
            'cursor': mock_cursor,
            'connect': mock_connect
        }


@pytest.fixture
def mock_email_api():
    """Mock email API for integration tests."""
    with patch('requests.post') as mock_post:
        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.json.return_value = {"message_id": "test_message_id"}
        mock_post.return_value = mock_response
        
        yield {
            'post': mock_post,
            'response': mock_response
        }


@pytest.fixture
def mock_strava_api():
    """Mock Strava API for integration tests."""
    with patch('aiohttp.ClientSession') as mock_session_class:
        mock_session = Mock()
        mock_response = Mock()
        
        # Configure async context managers
        mock_session.__aenter__ = Mock(return_value=mock_session)
        mock_session.__aexit__ = Mock(return_value=None)
        
        mock_response.__aenter__ = Mock(return_value=mock_response)
        mock_response.__aexit__ = Mock(return_value=None)
        
        # Configure response behavior
        mock_response.status = 200
        mock_response.json = Mock()
        
        mock_session.get.return_value = mock_response
        mock_session.post.return_value = mock_response
        
        mock_session_class.return_value = mock_session
        
        yield {
            'session_class': mock_session_class,
            'session': mock_session,
            'response': mock_response
        }


@pytest.fixture
def temp_config_file(test_config):
    """Create a temporary configuration file for testing."""
    config_data = {
        "athletes": [
            {
                "name": athlete.name,
                "athlete_id": athlete.athlete_id,
                "access_token": athlete.access_token,
                "refresh_token": athlete.refresh_token,
                "expires_at": athlete.expires_at,
                "email": athlete.email
            }
            for athlete in test_config.athletes
        ],
        "database": {
            "host": test_config.database.host,
            "port": test_config.database.port,
            "user": test_config.database.user,
            "password": test_config.database.password,
            "database": test_config.database.database
        },
        "email": {
            "api_key": test_config.email.api_key,
            "from_email": test_config.email.from_email,
            "from_name": test_config.email.from_name
        }
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        import json
        json.dump(config_data, f, indent=2)
        temp_file_path = f.name
    
    yield temp_file_path
    
    # Cleanup
    try:
        os.unlink(temp_file_path)
    except OSError:
        pass


@pytest.fixture
def integration_test_env_vars(test_config):
    """Set up environment variables for integration testing."""
    athlete = test_config.athletes[0]
    
    env_vars = {
        'DOMINIC_NAME': athlete.name,
        'DOMINIC_ATHLETE_ID': str(athlete.athlete_id),
        'DOMINIC_ACCESS_TOKEN': athlete.access_token,
        'DOMINIC_REFRESH_TOKEN': athlete.refresh_token,
        'DOMINIC_TOKEN_EXPIRES': str(athlete.expires_at),
        'DOMINIC_EMAIL': athlete.email,
        'DB_HOST': test_config.database.host,
        'DB_PORT': str(test_config.database.port),
        'DB_USER': test_config.database.user,
        'DB_PASSWORD': test_config.database.password,
        'DB_NAME': test_config.database.database,
        'BREVO_API_KEY': test_config.email.api_key,
        'BREVO_FROM_EMAIL': test_config.email.from_email,
        'BREVO_FROM_NAME': test_config.email.from_name
    }
    
    # Store original values
    original_values = {}
    for key, value in env_vars.items():
        original_values[key] = os.environ.get(key)
        os.environ[key] = value
    
    yield env_vars
    
    # Restore original values
    for key, original_value in original_values.items():
        if original_value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = original_value


@pytest.fixture
def webhook_event_data():
    """Sample webhook event data for testing."""
    return {
        "object_type": "activity",
        "object_id": 987654321,
        "aspect_type": "create",
        "owner_id": 99999,
        "event_time": int(datetime.now().timestamp()),
        "subscription_id": 12345
    }


# Pytest configuration for integration tests
def pytest_configure(config):
    """Configure pytest for integration tests."""
    config.addinivalue_line(
        "markers", "integration: mark test as an integration test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )


def pytest_collection_modifyitems(config, items):
    """Modify test collection to add markers."""
    for item in items:
        # Add integration marker to all tests in integration directory
        if "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)
        
        # Add slow marker to tests that might be slow
        if any(keyword in item.name.lower() for keyword in ["complete", "full", "end_to_end"]):
            item.add_marker(pytest.mark.slow)