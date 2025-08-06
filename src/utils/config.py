"""
Configuration management for the Strava Data Fetcher application.

This module provides dataclass-based configuration management with validation
and environment variable loading.
"""

import os
import re
from dataclasses import dataclass
from typing import List, Optional

from .error_handling import ConfigurationError, ValidationError, validate_email_format


@dataclass(frozen=True)
class AthleteConfig:
    """Configuration for a single athlete."""
    name: str
    athlete_id: int
    access_token: str
    refresh_token: str
    expires_at: int
    email: str
    
    def validate(self) -> None:
        """Validate athlete configuration."""
        if not self.name or not self.name.strip():
            raise ConfigurationError("Athlete name is required", config_field="name")
        
        if self.athlete_id <= 0:
            raise ConfigurationError("Athlete ID must be positive", config_field="athlete_id")
        
        if not self.access_token or not self.access_token.strip():
            raise ConfigurationError("Access token is required", config_field="access_token")
        
        if not self.refresh_token or not self.refresh_token.strip():
            raise ConfigurationError("Refresh token is required", config_field="refresh_token")
        
        if self.expires_at <= 0:
            raise ConfigurationError("Token expiration time must be positive", config_field="expires_at")
        
        if self.email:
            try:
                validate_email_format(self.email)
            except ValidationError:
                raise ConfigurationError("Invalid email format", config_field="email")
    
    def _is_valid_email(self, email: str) -> bool:
        """Validate email format."""
        if not email:
            return False
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email))


@dataclass(frozen=True)
class DatabaseConfig:
    """Database connection configuration."""
    host: str
    port: int
    user: str
    password: str
    database: str
    
    def validate(self) -> None:
        """Validate database configuration."""
        if not self.host or not self.host.strip():
            raise ConfigurationError("Database host is required", config_field="host")
        
        if not (1 <= self.port <= 65535):
            raise ConfigurationError("Database port must be between 1 and 65535", config_field="port")
        
        if not self.user or not self.user.strip():
            raise ConfigurationError("Database user is required", config_field="user")
        
        if not self.password:
            raise ConfigurationError("Database password is required", config_field="password")
        
        if not self.database or not self.database.strip():
            raise ConfigurationError("Database name is required", config_field="database")


@dataclass(frozen=True)
class EmailConfig:
    """Email service configuration."""
    api_key: str
    from_email: str
    from_name: str
    
    def validate(self) -> None:
        """Validate email configuration."""
        if not self.api_key or not self.api_key.strip():
            raise ConfigurationError("Email API key is required", config_field="api_key")
        
        if not self.from_email:
            raise ConfigurationError("From email is required", config_field="from_email")
        
        try:
            validate_email_format(self.from_email)
        except ValidationError:
            raise ConfigurationError("Invalid from email format", config_field="from_email")
        
        if not self.from_name or not self.from_name.strip():
            raise ConfigurationError("From name is required", config_field="from_name")
    
    def _is_valid_email(self, email: str) -> bool:
        """Validate email format."""
        if not email:
            return False
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email))


@dataclass(frozen=True)
class Config:
    """Main application configuration."""
    athletes: List[AthleteConfig]
    database: DatabaseConfig
    email: EmailConfig
    
    def validate(self) -> None:
        """Validate entire configuration."""
        if not self.athletes:
            raise ConfigurationError("At least one athlete configuration is required", config_field="athletes")
        
        # Validate each component
        for i, athlete in enumerate(self.athletes):
            try:
                athlete.validate()
            except ConfigurationError as e:
                raise ConfigurationError(f"Athlete {i+1} validation failed: {e.message}", config_field=f"athletes[{i}].{e.config_field}")
        
        try:
            self.database.validate()
        except ConfigurationError as e:
            raise ConfigurationError(f"Database validation failed: {e.message}", config_field=f"database.{e.config_field}")
        
        try:
            self.email.validate()
        except ConfigurationError as e:
            raise ConfigurationError(f"Email validation failed: {e.message}", config_field=f"email.{e.config_field}")
        
        # Check for duplicate athlete IDs
        athlete_ids = [athlete.athlete_id for athlete in self.athletes]
        if len(athlete_ids) != len(set(athlete_ids)):
            raise ConfigurationError("Duplicate athlete IDs found", config_field="athletes")
        
        # Check for duplicate athlete names
        athlete_names = [athlete.name.lower() for athlete in self.athletes]
        if len(athlete_names) != len(set(athlete_names)):
            raise ConfigurationError("Duplicate athlete names found", config_field="athletes")
    
    def get_athlete_by_id(self, athlete_id: int) -> Optional[AthleteConfig]:
        """Get athlete configuration by ID."""
        if not isinstance(athlete_id, int) or athlete_id <= 0:
            raise ValidationError("Athlete ID must be a positive integer", field="athlete_id", value=athlete_id)
        
        for athlete in self.athletes:
            if athlete.athlete_id == athlete_id:
                return athlete
        return None
    
    def get_athlete_by_name(self, name: str) -> Optional[AthleteConfig]:
        """Get athlete configuration by name (case-insensitive)."""
        if not name or not name.strip():
            raise ValidationError("Athlete name cannot be empty", field="name")
        
        name_lower = name.lower()
        for athlete in self.athletes:
            if athlete.name.lower() == name_lower:
                return athlete
        return None
    
    @classmethod
    def from_env(cls) -> 'Config':
        """Create configuration from environment variables."""
        try:
            # Load athletes
            athletes = []
            
            # Check for Dominic's configuration
            if all(os.getenv(f'DOMINIC_{key}') for key in ['NAME', 'ATHLETE_ID', 'ACCESS_TOKEN', 'REFRESH_TOKEN', 'TOKEN_EXPIRES', 'EMAIL']):
                try:
                    dominic = AthleteConfig(
                        name=os.getenv('DOMINIC_NAME'),
                        athlete_id=int(os.getenv('DOMINIC_ATHLETE_ID')),
                        access_token=os.getenv('DOMINIC_ACCESS_TOKEN'),
                        refresh_token=os.getenv('DOMINIC_REFRESH_TOKEN'),
                        expires_at=int(os.getenv('DOMINIC_TOKEN_EXPIRES')),
                        email=os.getenv('DOMINIC_EMAIL')
                    )
                    athletes.append(dominic)
                except (ValueError, TypeError) as e:
                    raise ConfigurationError(f"Invalid Dominic configuration: {e}")
            
            # Check for Clare's configuration
            if all(os.getenv(f'CLARE_{key}') for key in ['NAME', 'ATHLETE_ID', 'ACCESS_TOKEN', 'REFRESH_TOKEN', 'TOKEN_EXPIRES', 'EMAIL']):
                try:
                    clare = AthleteConfig(
                        name=os.getenv('CLARE_NAME'),
                        athlete_id=int(os.getenv('CLARE_ATHLETE_ID')),
                        access_token=os.getenv('CLARE_ACCESS_TOKEN'),
                        refresh_token=os.getenv('CLARE_REFRESH_TOKEN'),
                        expires_at=int(os.getenv('CLARE_TOKEN_EXPIRES')),
                        email=os.getenv('CLARE_EMAIL')
                    )
                    athletes.append(clare)
                except (ValueError, TypeError) as e:
                    raise ConfigurationError(f"Invalid Clare configuration: {e}")
            
            # Database configuration
            try:
                database = DatabaseConfig(
                    host=os.getenv('DB_HOST', 'localhost'),
                    port=int(os.getenv('DB_PORT', '3306')),
                    user=os.getenv('DB_USER'),
                    password=os.getenv('DB_PASSWORD'),
                    database=os.getenv('DB_NAME')
                )
            except (ValueError, TypeError) as e:
                raise ConfigurationError(f"Invalid database configuration: {e}")
            
            # Email configuration
            email = EmailConfig(
                api_key=os.getenv('BREVO_API_KEY'),
                from_email=os.getenv('BREVO_FROM_EMAIL'),
                from_name=os.getenv('BREVO_FROM_NAME', 'Strava Data Fetcher')
            )
            
            config = cls(athletes=athletes, database=database, email=email)
            config.validate()
            return config
            
        except Exception as e:
            if isinstance(e, ConfigurationError):
                raise
            raise ConfigurationError(f"Error loading configuration from environment: {e}")