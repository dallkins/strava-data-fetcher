"""
Strava API client with rate limiting and async support.

This module provides an async HTTP client for interacting with the Strava API
with proper rate limiting, token management, and error handling.
"""

import asyncio
import time
from typing import List, Optional, Dict, Any
from collections import deque

import aiohttp
from aiohttp import ClientResponseError

from .models import StravaActivity
from ..utils.config import AthleteConfig
from ..utils.logging_config import get_logger, PerformanceTimer
from ..utils.error_handling import APIError, RateLimitError, AuthenticationError, handle_async_errors
from ..utils.cache import get_cache_manager, cache_api_result

logger = get_logger(__name__)


class StravaAPIClient:
    """
    Async Strava API client with rate limiting and token management.
    
    Handles Strava's rate limits (100 requests per 15 minutes, 1000 per day)
    and provides methods for fetching activity data.
    """
    
    def __init__(self, athlete_config: AthleteConfig):
        """
        Initialize the Strava API client.
        
        Args:
            athlete_config: Configuration for the athlete
        """
        self.athlete_config = athlete_config
        self.base_url = "https://www.strava.com/api/v3"
        self.session: Optional[aiohttp.ClientSession] = None
        
        # Rate limiting (100 requests per 15 minutes)
        self.rate_limit_requests = 100
        self.rate_limit_window = 900  # 15 minutes in seconds
        self.request_times: deque = deque()
        
        # Caching
        self.cache_manager = get_cache_manager()
        self.activity_cache = self.cache_manager.get_activity_cache()
    
    async def __aenter__(self) -> 'StravaAPIClient':
        """Async context manager entry."""
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        if self.session:
            await self.session.close()
    
    def _get_headers(self) -> Dict[str, str]:
        """Get headers for API requests."""
        return {
            'Authorization': f'Bearer {self.athlete_config.access_token}',
            'Content-Type': 'application/json'
        }
    
    async def _check_rate_limit(self) -> None:
        """
        Check and enforce rate limiting.
        
        Strava allows 100 requests per 15 minutes. This method ensures
        we don't exceed that limit by tracking request times and sleeping
        if necessary.
        """
        current_time = time.time()
        
        # Remove old requests outside the window
        while self.request_times and current_time - self.request_times[0] > self.rate_limit_window:
            self.request_times.popleft()
        
        # If we're at the limit, wait until we can make another request
        if len(self.request_times) >= self.rate_limit_requests:
            sleep_time = self.rate_limit_window - (current_time - self.request_times[0])
            if sleep_time > 0:
                logger.warning(f"Rate limit reached, sleeping for {sleep_time:.1f} seconds")
                await asyncio.sleep(sleep_time)
        
        # Record this request
        self.request_times.append(current_time)
    
    async def _make_request(
        self, 
        method: str, 
        endpoint: str, 
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Make an HTTP request to the Strava API.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint (e.g., '/activities')
            params: Query parameters
            data: Request body data
            
        Returns:
            JSON response data or None if request failed
        """
        if not self.session:
            raise APIError("Client session not initialized. Use async context manager.")
        
        await self._check_rate_limit()
        
        url = f"{self.base_url}{endpoint}"
        headers = self._get_headers()
        
        try:
            if method.upper() == 'GET':
                async with self.session.get(url, headers=headers, params=params) as response:
                    return await self._handle_response(response)
            elif method.upper() == 'POST':
                async with self.session.post(url, headers=headers, json=data) as response:
                    return await self._handle_response(response)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
                
        except Exception as e:
            logger.error(f"Request failed for {method} {endpoint}: {e}")
            return None
    
    async def _handle_response(self, response: aiohttp.ClientResponse) -> Optional[Dict[str, Any]]:
        """
        Handle API response with proper error handling.
        
        Args:
            response: aiohttp response object
            
        Returns:
            JSON response data or None if error
        """
        try:
            if response.status == 200 or response.status == 201:
                return await response.json()
            elif response.status == 401:
                raise AuthenticationError("Authentication failed - invalid or expired token")
            elif response.status == 403:
                raise APIError("Access forbidden - insufficient permissions", status_code=403)
            elif response.status == 404:
                logger.warning(f"Resource not found: {response.url}")
                return None
            elif response.status == 429:
                # Rate limited - sleep for 15 minutes
                logger.warning("Rate limited by Strava API, sleeping for 15 minutes")
                raise RateLimitError("API rate limit exceeded", retry_after=900)
            else:
                response.raise_for_status()
                
        except ClientResponseError as e:
            if e.status == 401:
                raise AuthenticationError("Authentication failed - invalid or expired token")
            elif e.status == 403:
                raise APIError("Access forbidden - insufficient permissions", status_code=403)
            elif e.status == 404:
                logger.warning(f"Resource not found: {e.request_info.url}")
                return None
            elif e.status == 429:
                logger.warning("Rate limited by Strava API")
                raise RateLimitError("API rate limit exceeded", retry_after=900)
            else:
                raise APIError(f"HTTP error {e.status}: {e.message}", status_code=e.status)
        except Exception as e:
            if isinstance(e, (APIError, RateLimitError, AuthenticationError)):
                raise
            raise APIError(f"Error handling response: {e}")
    
    async def get_activity(self, activity_id: int) -> Optional[Dict[str, Any]]:
        """
        Get a single activity by ID.
        
        Args:
            activity_id: Strava activity ID
            
        Returns:
            Activity data or None if not found
        """
        with PerformanceTimer(f"Get activity {activity_id}"):
            # Check cache first
            cached_activity = self.activity_cache.get_activity(activity_id)
            if cached_activity is not None:
                logger.debug(f"Cache hit for activity {activity_id}")
                return cached_activity
            
            # Fetch from API
            activity_data = await self._make_request('GET', f'/activities/{activity_id}')
            
            # Cache the result if successful
            if activity_data:
                self.activity_cache.set_activity(activity_id, activity_data, ttl=3600)
                logger.debug(f"Cached activity {activity_id}")
            
            return activity_data
    
    async def fetch_activity_by_id(self, activity_id: int) -> Optional[StravaActivity]:
        """
        Fetch and parse a single activity by ID.
        
        Args:
            activity_id: Strava activity ID
            
        Returns:
            Parsed StravaActivity object or None if not found
        """
        with PerformanceTimer(f"Fetch and parse activity {activity_id}"):
            activity_data = await self.get_activity(activity_id)
            
            if not activity_data:
                return None
            
            try:
                return StravaActivity.from_strava_api(
                    activity_data,
                    self.athlete_config.name,
                    self.athlete_config.athlete_id
                )
            except Exception as e:
                logger.error(f"Error parsing activity {activity_id}: {e}")
                return None
    
    async def get_activities(
        self, 
        per_page: int = 50, 
        page: int = 1,
        before: Optional[int] = None,
        after: Optional[int] = None
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Get a list of activities for the athlete.
        
        Args:
            per_page: Number of activities per page (max 200)
            page: Page number
            before: Unix timestamp to get activities before
            after: Unix timestamp to get activities after
            
        Returns:
            List of activity data or None if error
        """
        params = {
            'per_page': min(per_page, 200),  # Strava max is 200
            'page': page
        }
        
        if before:
            params['before'] = before
        if after:
            params['after'] = after
        
        return await self._make_request('GET', '/athlete/activities', params=params)
    
    async def fetch_and_parse_activities(
        self,
        limit: Optional[int] = None,
        before: Optional[int] = None,
        after: Optional[int] = None
    ) -> List[StravaActivity]:
        """
        Fetch and parse activities into StravaActivity objects.
        
        Args:
            limit: Maximum number of activities to fetch
            before: Unix timestamp to get activities before
            after: Unix timestamp to get activities after
            
        Returns:
            List of parsed StravaActivity objects
        """
        with PerformanceTimer(f"Fetch activities for {self.athlete_config.name} (limit: {limit})"):
            activities = []
            page = 1
            per_page = 50
            
            while True:
                # Adjust per_page if we're close to the limit
                if limit and len(activities) + per_page > limit:
                    per_page = limit - len(activities)
                
                # Fetch activities for this page
                page_activities = await self.get_activities(
                    per_page=per_page,
                    page=page,
                    before=before,
                    after=after
                )
                
                if not page_activities:
                    # No more activities or error occurred
                    break
                
                # Parse activities
                for activity_data in page_activities:
                    try:
                        activity = StravaActivity.from_strava_api(
                            activity_data,
                            self.athlete_config.name,
                            self.athlete_config.athlete_id
                        )
                        activities.append(activity)
                    except Exception as e:
                        logger.error(f"Error parsing activity {activity_data.get('id', 'unknown')}: {e}")
                        continue
                
                # Check if we've reached our limit
                if limit and len(activities) >= limit:
                    activities = activities[:limit]
                    break
                
                # Check if we got fewer activities than requested (last page)
                if len(page_activities) < per_page:
                    break
                
                page += 1
            
            logger.info(f"Fetched and parsed {len(activities)} activities for {self.athlete_config.name}")
            return activities
    
    async def refresh_token(self, client_id: str, client_secret: str) -> Optional[Dict[str, Any]]:
        """
        Refresh the access token using the refresh token.
        
        Args:
            client_id: Strava application client ID
            client_secret: Strava application client secret
            
        Returns:
            New token data or None if refresh failed
        """
        data = {
            'client_id': client_id,
            'client_secret': client_secret,
            'refresh_token': self.athlete_config.refresh_token,
            'grant_type': 'refresh_token'
        }
        
        try:
            if not self.session:
                raise APIError("Client session not initialized")
                
            async with self.session.post('https://www.strava.com/oauth/token', data=data) as response:
                if response.status == 200:
                    token_data = await response.json()
                    logger.info(f"Successfully refreshed token for {self.athlete_config.name}")
                    return token_data
                elif response.status == 400:
                    raise AuthenticationError("Invalid refresh token or client credentials")
                elif response.status == 401:
                    raise AuthenticationError("Unauthorized - invalid client credentials")
                else:
                    raise APIError(f"Token refresh failed with status {response.status}", status_code=response.status)
                    
        except Exception as e:
            if isinstance(e, (APIError, AuthenticationError)):
                raise
            raise APIError(f"Error refreshing token: {e}")