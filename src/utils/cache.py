"""
Caching utilities for the Strava Data Fetcher application.

This module provides caching mechanisms to improve performance by reducing
redundant API calls, database queries, and expensive computations.
"""

import asyncio
import json
import time
from typing import Any, Dict, Optional, Callable, Union
from functools import wraps
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta

from .logging_config import get_logger, PerformanceTimer
from .error_handling import StravaDataFetcherError

logger = get_logger(__name__)


@dataclass
class CacheEntry:
    """Represents a cache entry with value and metadata."""
    value: Any
    timestamp: float
    ttl: float
    access_count: int = 0
    last_access: float = 0.0
    
    def is_expired(self) -> bool:
        """Check if the cache entry has expired."""
        return time.time() - self.timestamp > self.ttl
    
    def is_valid(self) -> bool:
        """Check if the cache entry is valid (not expired)."""
        return not self.is_expired()
    
    def touch(self) -> None:
        """Update access statistics."""
        self.access_count += 1
        self.last_access = time.time()


class InMemoryCache:
    """
    In-memory cache with TTL support and LRU eviction.
    
    This cache is suitable for storing frequently accessed data
    that doesn't need to persist across application restarts.
    """
    
    def __init__(self, max_size: int = 1000, default_ttl: float = 3600):
        """
        Initialize the in-memory cache.
        
        Args:
            max_size: Maximum number of entries to store
            default_ttl: Default time-to-live in seconds
        """
        self.max_size = max_size
        self.default_ttl = default_ttl
        self._cache: Dict[str, CacheEntry] = {}
        self._access_order: list = []
        self.stats = {
            'hits': 0,
            'misses': 0,
            'evictions': 0,
            'expired': 0
        }
    
    def get(self, key: str) -> Optional[Any]:
        """
        Get a value from the cache.
        
        Args:
            key: Cache key
            
        Returns:
            Cached value or None if not found/expired
        """
        if key not in self._cache:
            self.stats['misses'] += 1
            return None
        
        entry = self._cache[key]
        
        if entry.is_expired():
            self.stats['expired'] += 1
            self.delete(key)
            return None
        
        # Update access statistics and LRU order
        entry.touch()
        self._update_access_order(key)
        self.stats['hits'] += 1
        
        return entry.value
    
    def set(self, key: str, value: Any, ttl: Optional[float] = None) -> None:
        """
        Set a value in the cache.
        
        Args:
            key: Cache key
            value: Value to cache
            ttl: Time-to-live in seconds (uses default if None)
        """
        if ttl is None:
            ttl = self.default_ttl
        
        # Check if we need to evict entries
        if len(self._cache) >= self.max_size and key not in self._cache:
            self._evict_lru()
        
        entry = CacheEntry(
            value=value,
            timestamp=time.time(),
            ttl=ttl
        )
        
        self._cache[key] = entry
        self._update_access_order(key)
        
        logger.debug(f"Cached value for key: {key} (TTL: {ttl}s)")
    
    def delete(self, key: str) -> bool:
        """
        Delete a value from the cache.
        
        Args:
            key: Cache key
            
        Returns:
            True if key was deleted, False if not found
        """
        if key in self._cache:
            del self._cache[key]
            if key in self._access_order:
                self._access_order.remove(key)
            return True
        return False
    
    def clear(self) -> None:
        """Clear all entries from the cache."""
        self._cache.clear()
        self._access_order.clear()
        logger.info("Cache cleared")
    
    def cleanup_expired(self) -> int:
        """
        Remove expired entries from the cache.
        
        Returns:
            Number of entries removed
        """
        expired_keys = [
            key for key, entry in self._cache.items()
            if entry.is_expired()
        ]
        
        for key in expired_keys:
            self.delete(key)
        
        if expired_keys:
            logger.debug(f"Cleaned up {len(expired_keys)} expired cache entries")
        
        return len(expired_keys)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        total_requests = self.stats['hits'] + self.stats['misses']
        hit_rate = (self.stats['hits'] / total_requests * 100) if total_requests > 0 else 0
        
        return {
            **self.stats,
            'total_requests': total_requests,
            'hit_rate_percent': round(hit_rate, 2),
            'current_size': len(self._cache),
            'max_size': self.max_size
        }
    
    def _update_access_order(self, key: str) -> None:
        """Update the LRU access order."""
        if key in self._access_order:
            self._access_order.remove(key)
        self._access_order.append(key)
    
    def _evict_lru(self) -> None:
        """Evict the least recently used entry."""
        if self._access_order:
            lru_key = self._access_order[0]
            self.delete(lru_key)
            self.stats['evictions'] += 1
            logger.debug(f"Evicted LRU cache entry: {lru_key}")


class ActivityCache:
    """
    Specialized cache for Strava activities with intelligent caching strategies.
    """
    
    def __init__(self, cache: InMemoryCache):
        """
        Initialize the activity cache.
        
        Args:
            cache: Underlying cache implementation
        """
        self.cache = cache
        self.logger = get_logger(__name__)
    
    def get_activity(self, activity_id: int) -> Optional[Dict[str, Any]]:
        """Get a cached activity by ID."""
        key = f"activity:{activity_id}"
        return self.cache.get(key)
    
    def set_activity(self, activity_id: int, activity_data: Dict[str, Any], ttl: float = 3600) -> None:
        """Cache an activity with appropriate TTL."""
        key = f"activity:{activity_id}"
        self.cache.set(key, activity_data, ttl)
    
    def get_activities_list(self, athlete_id: int, page: int, per_page: int) -> Optional[list]:
        """Get cached activities list for pagination."""
        key = f"activities_list:{athlete_id}:{page}:{per_page}"
        return self.cache.get(key)
    
    def set_activities_list(
        self, 
        athlete_id: int, 
        page: int, 
        per_page: int, 
        activities: list, 
        ttl: float = 300
    ) -> None:
        """Cache activities list with shorter TTL (more dynamic data)."""
        key = f"activities_list:{athlete_id}:{page}:{per_page}"
        self.cache.set(key, activities, ttl)
    
    def get_summary(self, athlete_id: int, date_range: str) -> Optional[Dict[str, Any]]:
        """Get cached activity summary."""
        key = f"summary:{athlete_id}:{date_range}"
        return self.cache.get(key)
    
    def set_summary(
        self, 
        athlete_id: int, 
        date_range: str, 
        summary_data: Dict[str, Any], 
        ttl: float = 1800
    ) -> None:
        """Cache activity summary with medium TTL."""
        key = f"summary:{athlete_id}:{date_range}"
        self.cache.set(key, summary_data, ttl)


def cache_result(
    cache: InMemoryCache,
    key_func: Optional[Callable] = None,
    ttl: float = 3600,
    skip_cache_on_error: bool = True
):
    """
    Decorator to cache function results.
    
    Args:
        cache: Cache instance to use
        key_func: Function to generate cache key from arguments
        ttl: Time-to-live for cached results
        skip_cache_on_error: Whether to skip caching on exceptions
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Generate cache key
            if key_func:
                cache_key = key_func(*args, **kwargs)
            else:
                # Default key generation
                cache_key = f"{func.__name__}:{hash(str(args) + str(sorted(kwargs.items())))}"
            
            # Try to get from cache
            cached_result = cache.get(cache_key)
            if cached_result is not None:
                logger.debug(f"Cache hit for {func.__name__}: {cache_key}")
                return cached_result
            
            # Execute function
            try:
                with PerformanceTimer(f"Execute and cache {func.__name__}"):
                    result = func(*args, **kwargs)
                
                # Cache the result
                cache.set(cache_key, result, ttl)
                logger.debug(f"Cached result for {func.__name__}: {cache_key}")
                
                return result
                
            except Exception as e:
                if not skip_cache_on_error:
                    # Cache the exception for a short time to avoid repeated failures
                    cache.set(cache_key, e, ttl=60)
                raise
        
        return wrapper
    return decorator


def async_cache_result(
    cache: InMemoryCache,
    key_func: Optional[Callable] = None,
    ttl: float = 3600,
    skip_cache_on_error: bool = True
):
    """
    Decorator to cache async function results.
    
    Args:
        cache: Cache instance to use
        key_func: Function to generate cache key from arguments
        ttl: Time-to-live for cached results
        skip_cache_on_error: Whether to skip caching on exceptions
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Generate cache key
            if key_func:
                cache_key = key_func(*args, **kwargs)
            else:
                # Default key generation
                cache_key = f"{func.__name__}:{hash(str(args) + str(sorted(kwargs.items())))}"
            
            # Try to get from cache
            cached_result = cache.get(cache_key)
            if cached_result is not None:
                # Check if cached result is an exception
                if isinstance(cached_result, Exception):
                    raise cached_result
                logger.debug(f"Cache hit for {func.__name__}: {cache_key}")
                return cached_result
            
            # Execute function
            try:
                with PerformanceTimer(f"Execute and cache {func.__name__}"):
                    result = await func(*args, **kwargs)
                
                # Cache the result
                cache.set(cache_key, result, ttl)
                logger.debug(f"Cached result for {func.__name__}: {cache_key}")
                
                return result
                
            except Exception as e:
                if not skip_cache_on_error:
                    # Cache the exception for a short time to avoid repeated failures
                    cache.set(cache_key, e, ttl=60)
                raise
        
        return wrapper
    return decorator


class CacheManager:
    """
    Central cache manager for the application.
    
    Manages multiple cache instances and provides cleanup and monitoring.
    """
    
    def __init__(self):
        """Initialize the cache manager."""
        self.caches: Dict[str, InMemoryCache] = {}
        self.activity_cache: Optional[ActivityCache] = None
        self.cleanup_interval = 300  # 5 minutes
        self.last_cleanup = time.time()
    
    def get_cache(self, name: str, max_size: int = 1000, default_ttl: float = 3600) -> InMemoryCache:
        """
        Get or create a named cache.
        
        Args:
            name: Cache name
            max_size: Maximum cache size
            default_ttl: Default TTL for entries
            
        Returns:
            Cache instance
        """
        if name not in self.caches:
            self.caches[name] = InMemoryCache(max_size=max_size, default_ttl=default_ttl)
            logger.info(f"Created cache: {name} (max_size: {max_size}, ttl: {default_ttl}s)")
        
        return self.caches[name]
    
    def get_activity_cache(self) -> ActivityCache:
        """Get the specialized activity cache."""
        if self.activity_cache is None:
            cache = self.get_cache('activities', max_size=2000, default_ttl=3600)
            self.activity_cache = ActivityCache(cache)
        
        return self.activity_cache
    
    def cleanup_all(self) -> Dict[str, int]:
        """
        Clean up expired entries from all caches.
        
        Returns:
            Dictionary with cleanup statistics per cache
        """
        cleanup_stats = {}
        
        for name, cache in self.caches.items():
            expired_count = cache.cleanup_expired()
            cleanup_stats[name] = expired_count
        
        self.last_cleanup = time.time()
        
        total_expired = sum(cleanup_stats.values())
        if total_expired > 0:
            logger.info(f"Cache cleanup completed: {total_expired} expired entries removed")
        
        return cleanup_stats
    
    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics for all caches."""
        return {
            name: cache.get_stats()
            for name, cache in self.caches.items()
        }
    
    def clear_all(self) -> None:
        """Clear all caches."""
        for cache in self.caches.values():
            cache.clear()
        logger.info("All caches cleared")
    
    def should_cleanup(self) -> bool:
        """Check if it's time for automatic cleanup."""
        return time.time() - self.last_cleanup > self.cleanup_interval
    
    def auto_cleanup(self) -> None:
        """Perform automatic cleanup if needed."""
        if self.should_cleanup():
            self.cleanup_all()


# Global cache manager instance
cache_manager = CacheManager()


def get_cache_manager() -> CacheManager:
    """Get the global cache manager instance."""
    return cache_manager


# Convenience functions for common caching patterns
def cache_api_result(ttl: float = 3600):
    """Decorator for caching API results."""
    api_cache = cache_manager.get_cache('api_results', max_size=500, default_ttl=ttl)
    return async_cache_result(api_cache, ttl=ttl)


def cache_database_result(ttl: float = 1800):
    """Decorator for caching database query results."""
    db_cache = cache_manager.get_cache('database_results', max_size=1000, default_ttl=ttl)
    return cache_result(db_cache, ttl=ttl)


def cache_computation_result(ttl: float = 3600):
    """Decorator for caching expensive computation results."""
    compute_cache = cache_manager.get_cache('computations', max_size=200, default_ttl=ttl)
    return cache_result(compute_cache, ttl=ttl)