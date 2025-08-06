"""
Data models for Strava API responses.

This module provides dataclass-based models for parsing and handling
Strava activity data with proper type conversion and validation.
"""

from dataclasses import dataclass
from typing import Optional, List, Any, Dict


@dataclass
class StravaActivity:
    """Represents a Strava activity with all relevant data."""
    
    # Core identifiers
    id: int
    athlete_id: int
    athlete_name: str
    
    # Basic activity info
    name: str
    start_date_local: str
    start_date: str
    utc_offset: int
    
    # Gear information
    gear_id: Optional[str]
    gear_name: Optional[str]
    
    # Distance and time metrics
    distance: float  # meters
    elapsed_time: int  # seconds
    moving_time: int  # seconds
    
    # Performance metrics
    calories: int
    average_heartrate: Optional[float]
    max_heartrate: Optional[float]
    average_watts: Optional[float]
    max_watts: Optional[float]
    average_speed: Optional[float]  # m/s
    max_speed: Optional[float]  # m/s
    
    # Activity classification
    type: str
    sport_type: str
    
    # Elevation and terrain
    total_elevation_gain: float  # meters
    
    # Social metrics
    kudos_count: int
    
    # Advanced metrics
    weighted_average_watts: Optional[float]
    average_cadence: Optional[float]
    trainer: bool
    
    # Location and mapping
    map_polyline: Optional[str]
    device_name: Optional[str]
    timezone: Optional[str]
    start_latlng: Optional[List[float]]
    end_latlng: Optional[List[float]]
    
    @classmethod
    def from_strava_api(cls, data: Dict[str, Any], athlete_name: str, athlete_id: int) -> 'StravaActivity':
        """
        Create a StravaActivity from Strava API response data.
        
        Args:
            data: Raw API response data
            athlete_name: Name of the athlete
            athlete_id: ID of the athlete
            
        Returns:
            StravaActivity instance with parsed data
        """
        # Helper function to safely convert values
        def safe_float(value: Any) -> Optional[float]:
            if value is None:
                return None
            try:
                return float(value)
            except (ValueError, TypeError):
                return None
        
        def safe_int(value: Any, default: int = 0) -> int:
            if value is None:
                return default
            try:
                return int(float(value))  # Handle string numbers
            except (ValueError, TypeError):
                return default
        
        def safe_bool(value: Any) -> bool:
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.lower() in ('true', '1')
            if isinstance(value, (int, float)):
                return bool(value)
            return False
        
        # Extract gear information
        gear_id = data.get('gear_id')
        gear_name = None
        if data.get('gear') and isinstance(data['gear'], dict):
            gear_name = data['gear'].get('name')
        
        # Extract map polyline
        map_polyline = None
        if data.get('map') and isinstance(data['map'], dict):
            map_polyline = data['map'].get('summary_polyline')
        
        # Extract location data
        start_latlng = data.get('start_latlng')
        end_latlng = data.get('end_latlng')
        
        # Ensure latlng are lists or None
        if start_latlng and not isinstance(start_latlng, list):
            start_latlng = None
        if end_latlng and not isinstance(end_latlng, list):
            end_latlng = None
        
        return cls(
            # Core identifiers
            id=int(data['id']),
            athlete_id=athlete_id,
            athlete_name=athlete_name,
            
            # Basic activity info
            name=data.get('name', ''),
            start_date_local=data.get('start_date_local', ''),
            start_date=data.get('start_date', ''),
            utc_offset=safe_int(data.get('utc_offset'), 0),
            
            # Gear information
            gear_id=gear_id,
            gear_name=gear_name,
            
            # Distance and time metrics
            distance=safe_float(data.get('distance')) or 0.0,
            elapsed_time=safe_int(data.get('elapsed_time'), 0),
            moving_time=safe_int(data.get('moving_time'), 0),
            
            # Performance metrics
            calories=safe_int(data.get('calories'), 0),
            average_heartrate=safe_float(data.get('average_heartrate')),
            max_heartrate=safe_float(data.get('max_heartrate')),
            average_watts=safe_float(data.get('average_watts')),
            max_watts=safe_float(data.get('max_watts')),
            average_speed=safe_float(data.get('average_speed')),
            max_speed=safe_float(data.get('max_speed')),
            
            # Activity classification
            type=data.get('type', ''),
            sport_type=data.get('sport_type', ''),
            
            # Elevation and terrain
            total_elevation_gain=safe_float(data.get('total_elevation_gain')) or 0.0,
            
            # Social metrics
            kudos_count=safe_int(data.get('kudos_count'), 0),
            
            # Advanced metrics
            weighted_average_watts=safe_float(data.get('weighted_average_watts')),
            average_cadence=safe_float(data.get('average_cadence')),
            trainer=safe_bool(data.get('trainer', False)),
            
            # Location and mapping
            map_polyline=map_polyline,
            device_name=data.get('device_name'),
            timezone=data.get('timezone'),
            start_latlng=start_latlng,
            end_latlng=end_latlng
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the activity to a dictionary for database storage."""
        return {
            'id': self.id,
            'athlete_id': self.athlete_id,
            'athlete_name': self.athlete_name,
            'name': self.name,
            'start_date_local': self.start_date_local,
            'start_date': self.start_date,
            'utc_offset': self.utc_offset,
            'gear_id': self.gear_id,
            'gear_name': self.gear_name,
            'distance': self.distance,
            'elapsed_time': self.elapsed_time,
            'moving_time': self.moving_time,
            'calories': self.calories,
            'average_heartrate': self.average_heartrate,
            'max_heartrate': self.max_heartrate,
            'average_watts': self.average_watts,
            'max_watts': self.max_watts,
            'average_speed': self.average_speed,
            'max_speed': self.max_speed,
            'type': self.type,
            'sport_type': self.sport_type,
            'total_elevation_gain': self.total_elevation_gain,
            'kudos_count': self.kudos_count,
            'weighted_average_watts': self.weighted_average_watts,
            'average_cadence': self.average_cadence,
            'trainer': self.trainer,
            'map_polyline': self.map_polyline,
            'device_name': self.device_name,
            'timezone': self.timezone,
            'start_latlng': self.start_latlng,
            'end_latlng': self.end_latlng
        }
    
    @property
    def distance_km(self) -> float:
        """Get distance in kilometers."""
        return self.distance / 1000.0
    
    @property
    def pace_per_km(self) -> Optional[float]:
        """Get pace in minutes per kilometer."""
        if self.moving_time > 0 and self.distance > 0:
            return (self.moving_time / 60.0) / self.distance_km
        return None
    
    @property
    def average_speed_kmh(self) -> Optional[float]:
        """Get average speed in km/h."""
        if self.average_speed:
            return self.average_speed * 3.6
        return None
    
    def is_cycling_activity(self) -> bool:
        """Check if this is a cycling activity."""
        cycling_types = {'Ride', 'VirtualRide', 'EBikeRide'}
        return self.sport_type in cycling_types or self.type in cycling_types
    
    def is_running_activity(self) -> bool:
        """Check if this is a running activity."""
        running_types = {'Run', 'VirtualRun', 'TrailRun'}
        return self.sport_type in running_types or self.type in running_types