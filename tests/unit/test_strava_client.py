"""
Unit tests for Strava API client module.
"""

import pytest
import asyncio
from unittest.mock import Mock, patch, AsyncMock, MagicMock
import aiohttp
from aiohttp import ClientResponseError

from src.api.strava_client import StravaAPIClient
from src.utils.config import AthleteConfig
from src.api.models import StravaActivity


class TestStravaAPIClient:
    """Test StravaAPIClient class"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.athlete_config = AthleteConfig(
            name="Test Athlete",
            athlete_id=12345,
            access_token="test_access_token",
            refresh_token="test_refresh_token",
            expires_at=1234567890,
            email="test@example.com"
        )
    
    def test_init(self):
        """Test StravaAPIClient initialization"""
        client = StravaAPIClient(self.athlete_config)
        
        assert client.athlete_config == self.athlete_config
        assert client.base_url == "https://www.strava.com/api/v3"
        assert client.rate_limit_requests == 100
        assert client.rate_limit_window == 900  # 15 minutes
        assert client.session is None
    
    @pytest.mark.asyncio
    async def test_context_manager(self):
        """Test StravaAPIClient as async context manager"""
        with patch('aiohttp.ClientSession') as mock_session_class:
            mock_session = AsyncMock()
            mock_session_class.return_value = mock_session
            
            async with StravaAPIClient(self.athlete_config) as client:
                assert client.session == mock_session
                mock_session_class.assert_called_once()
            
            mock_session.close.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_headers(self):
        """Test getting request headers"""
        client = StravaAPIClient(self.athlete_config)
        
        headers = client._get_headers()
        
        expected_headers = {
            'Authorization': 'Bearer test_access_token',
            'Content-Type': 'application/json'
        }
        
        assert headers == expected_headers
    
    @pytest.mark.asyncio
    @patch('src.api.strava_client.asyncio.sleep', new_callable=AsyncMock)
    async def test_check_rate_limit_no_limit(self, mock_sleep):
        """Test rate limit check when under limit"""
        client = StravaAPIClient(self.athlete_config)
        
        # Should not sleep when under rate limit
        await client._check_rate_limit()
        
        mock_sleep.assert_not_called()
    
    @pytest.mark.asyncio
    @patch('src.api.strava_client.asyncio.sleep', new_callable=AsyncMock)
    @patch('src.api.strava_client.time.time')
    async def test_check_rate_limit_with_limit(self, mock_time, mock_sleep):
        """Test rate limit check when at limit"""
        client = StravaAPIClient(self.athlete_config)
        
        # Simulate being at rate limit
        current_time = 1000
        mock_time.return_value = current_time
        
        # Fill up the rate limit
        for i in range(100):
            client.request_times.append(current_time - i)
        
        await client._check_rate_limit()
        
        # Should sleep for the remaining time in the window
        mock_sleep.assert_called_once()
        sleep_time = mock_sleep.call_args[0][0]
        assert sleep_time > 0
    
    @pytest.mark.asyncio
    async def test_make_request_success(self):
        """Test successful API request"""
        client = StravaAPIClient(self.athlete_config)
        
        # Mock response
        mock_response = Mock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={"id": 12345, "name": "Test Activity"})
        mock_response.raise_for_status = Mock()
        
        # Mock session with proper get method
        mock_session = Mock()
        
        # Create async context manager for session.get
        async def mock_get_context_manager(*args, **kwargs):
            class MockContext:
                async def __aenter__(self):
                    return mock_response
                async def __aexit__(self, exc_type, exc_val, exc_tb):
                    return None
            return MockContext()
        
        mock_session.get = mock_get_context_manager
        client.session = mock_session
        
        with patch.object(client, '_check_rate_limit', new_callable=AsyncMock):
            result = await client._make_request("GET", "/activities/12345")
        
        assert result == {"id": 12345, "name": "Test Activity"}
        mock_session.get.assert_called_once_with(
            "https://www.strava.com/api/v3/activities/12345",
            headers=client._get_headers(),
            params=None
        )
    
    @pytest.mark.asyncio
    async def test_make_request_with_params(self):
        """Test API request with parameters"""
        client = StravaAPIClient(self.athlete_config)
        
        # Mock response
        mock_response = Mock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=[])
        mock_response.raise_for_status = Mock()
        
        # Mock session with proper get method
        mock_session = Mock()
        
        # Create async context manager for session.get
        async def mock_get_context_manager(*args, **kwargs):
            class MockContext:
                async def __aenter__(self):
                    return mock_response
                async def __aexit__(self, exc_type, exc_val, exc_tb):
                    return None
            return MockContext()
        
        mock_session.get = mock_get_context_manager
        client.session = mock_session
        
        params = {"per_page": 50, "page": 1}
        
        with patch.object(client, '_check_rate_limit', new_callable=AsyncMock):
            result = await client._make_request("GET", "/activities", params=params)
        
        assert result == []
        mock_session.get.assert_called_once_with(
            "https://www.strava.com/api/v3/activities",
            headers=client._get_headers(),
            params=params
        )
    
    @pytest.mark.asyncio
    async def test_make_request_post(self):
        """Test POST API request"""
        client = StravaAPIClient(self.athlete_config)
        
        # Mock response
        mock_response = Mock()
        mock_response.status = 201
        mock_response.json = AsyncMock(return_value={"success": True})
        mock_response.raise_for_status = Mock()
        
        # Mock session with proper post method
        mock_session = Mock()
        
        # Create async context manager for session.post
        async def mock_post_context_manager(*args, **kwargs):
            class MockContext:
                async def __aenter__(self):
                    return mock_response
                async def __aexit__(self, exc_type, exc_val, exc_tb):
                    return None
            return MockContext()
        
        mock_session.post = mock_post_context_manager
        client.session = mock_session
        
        data = {"name": "Updated Activity"}
        
        with patch.object(client, '_check_rate_limit', new_callable=AsyncMock):
            result = await client._make_request("POST", "/activities/12345", data=data)
        
        assert result == {"success": True}
        mock_session.post.assert_called_once_with(
            "https://www.strava.com/api/v3/activities/12345",
            headers=client._get_headers(),
            json=data
        )
    
    @pytest.mark.asyncio
    async def test_make_request_404_error(self):
        """Test API request with 404 error"""
        client = StravaAPIClient(self.athlete_config)
        
        mock_response = AsyncMock()
        mock_response.status = 404
        mock_response.raise_for_status.side_effect = ClientResponseError(
            request_info=Mock(), history=(), status=404
        )
        
        mock_session = AsyncMock()
        mock_session.get.return_value.__aenter__.return_value = mock_response
        client.session = mock_session
        
        with patch.object(client, '_check_rate_limit', new_callable=AsyncMock):
            result = await client._make_request("GET", "/activities/99999")
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_make_request_429_rate_limit(self):
        """Test API request with rate limit error"""
        client = StravaAPIClient(self.athlete_config)
        
        # Mock response that will trigger rate limit handling
        mock_response = Mock()
        mock_response.status = 429
        mock_response.raise_for_status = Mock(side_effect=ClientResponseError(
            request_info=Mock(), history=(), status=429
        ))
        
        # Mock session with proper get method
        mock_session = Mock()
        
        # Create async context manager for session.get
        async def mock_get_context_manager(*args, **kwargs):
            class MockContext:
                async def __aenter__(self):
                    return mock_response
                async def __aexit__(self, exc_type, exc_val, exc_tb):
                    return None
            return MockContext()
        
        mock_session.get = mock_get_context_manager
        client.session = mock_session
        
        with patch.object(client, '_check_rate_limit', new_callable=AsyncMock):
            with patch('src.api.strava_client.asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
                result = await client._make_request("GET", "/activities/12345")
        
        assert result is None
        mock_sleep.assert_called_once_with(900)  # Should sleep for 15 minutes
    
    @pytest.mark.asyncio
    async def test_make_request_server_error(self):
        """Test API request with server error"""
        client = StravaAPIClient(self.athlete_config)
        
        mock_response = AsyncMock()
        mock_response.status = 500
        mock_response.raise_for_status.side_effect = ClientResponseError(
            request_info=Mock(), history=(), status=500
        )
        
        mock_session = AsyncMock()
        mock_session.get.return_value.__aenter__.return_value = mock_response
        client.session = mock_session
        
        with patch.object(client, '_check_rate_limit', new_callable=AsyncMock):
            result = await client._make_request("GET", "/activities/12345")
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_get_activity_success(self):
        """Test getting a single activity"""
        client = StravaAPIClient(self.athlete_config)
        
        activity_data = {
            "id": 12345,
            "name": "Morning Ride",
            "type": "Ride",
            "sport_type": "Ride",
            "start_date_local": "2024-01-01T08:00:00Z",
            "start_date": "2024-01-01T08:00:00Z",
            "distance": 25000.0
        }
        
        with patch.object(client, '_make_request', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = activity_data
            
            result = await client.get_activity(12345)
        
        assert result == activity_data
        mock_request.assert_called_once_with("GET", "/activities/12345")
    
    @pytest.mark.asyncio
    async def test_get_activity_not_found(self):
        """Test getting a non-existent activity"""
        client = StravaAPIClient(self.athlete_config)
        
        with patch.object(client, '_make_request', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = None
            
            result = await client.get_activity(99999)
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_get_activities_success(self):
        """Test getting multiple activities"""
        client = StravaAPIClient(self.athlete_config)
        
        activities_data = [
            {
                "id": 12345,
                "name": "Morning Ride",
                "type": "Ride",
                "sport_type": "Ride",
                "start_date_local": "2024-01-01T08:00:00Z",
                "distance": 25000.0
            },
            {
                "id": 12346,
                "name": "Evening Ride",
                "type": "Ride",
                "sport_type": "Ride",
                "start_date_local": "2024-01-01T18:00:00Z",
                "distance": 15000.0
            }
        ]
        
        with patch.object(client, '_make_request', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = activities_data
            
            result = await client.get_activities(per_page=50, page=1)
        
        assert result == activities_data
        mock_request.assert_called_once_with(
            "GET", 
            "/athlete/activities", 
            params={"per_page": 50, "page": 1}
        )
    
    @pytest.mark.asyncio
    async def test_get_activities_with_date_filters(self):
        """Test getting activities with date filters"""
        client = StravaAPIClient(self.athlete_config)
        
        with patch.object(client, '_make_request', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = []
            
            await client.get_activities(
                per_page=30,
                page=1,
                before=1640995200,  # 2022-01-01
                after=1609459200    # 2021-01-01
            )
        
        mock_request.assert_called_once_with(
            "GET", 
            "/athlete/activities", 
            params={
                "per_page": 30, 
                "page": 1, 
                "before": 1640995200, 
                "after": 1609459200
            }
        )
    
    @pytest.mark.asyncio
    async def test_fetch_and_parse_activities_success(self):
        """Test fetching and parsing activities"""
        client = StravaAPIClient(self.athlete_config)
        
        api_activities = [
            {
                "id": 12345,
                "name": "Morning Ride",
                "type": "Ride",
                "sport_type": "Ride",
                "start_date_local": "2024-01-01T08:00:00Z",
                "start_date": "2024-01-01T08:00:00Z",
                "distance": 25000.0
            }
        ]
        
        with patch.object(client, 'get_activities', new_callable=AsyncMock) as mock_get:
            mock_get.return_value = api_activities
            
            activities = await client.fetch_and_parse_activities(limit=50)
        
        assert len(activities) == 1
        assert isinstance(activities[0], StravaActivity)
        assert activities[0].id == 12345
        assert activities[0].athlete_name == "Test Athlete"
        assert activities[0].athlete_id == 12345
    
    @pytest.mark.asyncio
    async def test_fetch_and_parse_activities_pagination(self):
        """Test fetching activities with pagination"""
        client = StravaAPIClient(self.athlete_config)
        
        # Mock multiple pages of results
        page1_data = [{"id": i, "name": f"Activity {i}", "type": "Ride", "sport_type": "Ride",
                      "start_date_local": "2024-01-01T08:00:00Z", "start_date": "2024-01-01T08:00:00Z"}
                     for i in range(1, 51)]  # 50 activities
        page2_data = [{"id": i, "name": f"Activity {i}", "type": "Ride", "sport_type": "Ride",
                      "start_date_local": "2024-01-01T08:00:00Z", "start_date": "2024-01-01T08:00:00Z"}
                     for i in range(51, 76)]  # 25 activities
        
        with patch.object(client, 'get_activities', new_callable=AsyncMock) as mock_get:
            mock_get.side_effect = [page1_data, page2_data]  # Only two calls needed for 75 activities
            
            activities = await client.fetch_and_parse_activities(limit=100)
        
        assert len(activities) == 75  # Total activities fetched
        assert mock_get.call_count == 2  # Should make 2 API calls
        
        # Verify pagination parameters
        calls = mock_get.call_args_list
        # The actual implementation includes after/before parameters
        assert calls[0][1]["per_page"] == 50
        assert calls[0][1]["page"] == 1
        assert calls[1][1]["per_page"] == 50
        assert calls[1][1]["page"] == 2
    
    @pytest.mark.asyncio
    async def test_fetch_and_parse_activities_limit_reached(self):
        """Test fetching activities when limit is reached"""
        client = StravaAPIClient(self.athlete_config)
        
        # Mock more activities than the limit
        page1_data = [{"id": i, "name": f"Activity {i}", "type": "Ride", "sport_type": "Ride",
                      "start_date_local": "2024-01-01T08:00:00Z", "start_date": "2024-01-01T08:00:00Z"} 
                     for i in range(1, 51)]  # 50 activities
        
        with patch.object(client, 'get_activities', new_callable=AsyncMock) as mock_get:
            mock_get.return_value = page1_data
            
            activities = await client.fetch_and_parse_activities(limit=30)
        
        assert len(activities) == 30  # Should be limited to 30
        assert mock_get.call_count == 1  # Should only make 1 API call
    
    @pytest.mark.asyncio
    async def test_fetch_and_parse_activities_api_error(self):
        """Test fetching activities when API returns error"""
        client = StravaAPIClient(self.athlete_config)
        
        with patch.object(client, 'get_activities', new_callable=AsyncMock) as mock_get:
            mock_get.return_value = None
            
            activities = await client.fetch_and_parse_activities(limit=50)
        
        assert activities == []
    
    @pytest.mark.asyncio
    async def test_fetch_and_parse_activities_empty_response(self):
        """Test fetching activities when API returns empty list"""
        client = StravaAPIClient(self.athlete_config)
        
        with patch.object(client, 'get_activities', new_callable=AsyncMock) as mock_get:
            mock_get.return_value = []
            
            activities = await client.fetch_and_parse_activities(limit=50)
        
        assert activities == []
    
    @pytest.mark.asyncio
    async def test_session_cleanup_on_exception(self):
        """Test that session is properly cleaned up on exception"""
        with patch('aiohttp.ClientSession') as mock_session_class:
            mock_session = AsyncMock()
            mock_session_class.return_value = mock_session
            
            try:
                async with StravaAPIClient(self.athlete_config) as client:
                    raise ValueError("Test exception")
            except ValueError:
                pass
            
            mock_session.close.assert_called_once()