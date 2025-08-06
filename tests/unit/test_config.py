"""
Unit tests for configuration management module.
"""

import pytest
import os
from unittest.mock import patch, MagicMock
from dataclasses import FrozenInstanceError

from src.utils.config import AthleteConfig, DatabaseConfig, EmailConfig, Config


class TestAthleteConfig:
    """Test AthleteConfig dataclass"""
    
    def test_athlete_config_creation(self):
        """Test creating AthleteConfig with valid data"""
        config = AthleteConfig(
            name="Test Athlete",
            athlete_id=12345,
            access_token="test_access_token",
            refresh_token="test_refresh_token",
            expires_at=1234567890,
            email="test@example.com"
        )
        
        assert config.name == "Test Athlete"
        assert config.athlete_id == 12345
        assert config.access_token == "test_access_token"
        assert config.refresh_token == "test_refresh_token"
        assert config.expires_at == 1234567890
        assert config.email == "test@example.com"
    
    def test_athlete_config_immutable(self):
        """Test that AthleteConfig is immutable (frozen)"""
        config = AthleteConfig(
            name="Test Athlete",
            athlete_id=12345,
            access_token="test_access_token",
            refresh_token="test_refresh_token",
            expires_at=1234567890,
            email="test@example.com"
        )
        
        with pytest.raises(FrozenInstanceError):
            config.name = "Modified Name"
    
    def test_athlete_config_validation(self):
        """Test AthleteConfig validation"""
        config = AthleteConfig(
            name="Test Athlete",
            athlete_id=12345,
            access_token="test_access_token",
            refresh_token="test_refresh_token",
            expires_at=1234567890,
            email="test@example.com"
        )
        
        # Should not raise any exceptions
        config.validate()
    
    def test_athlete_config_validation_missing_name(self):
        """Test validation fails with missing name"""
        config = AthleteConfig(
            name="",
            athlete_id=12345,
            access_token="test_access_token",
            refresh_token="test_refresh_token",
            expires_at=1234567890,
            email="test@example.com"
        )
        
        with pytest.raises(ValueError, match="Athlete name is required"):
            config.validate()
    
    def test_athlete_config_validation_invalid_email(self):
        """Test validation fails with invalid email"""
        config = AthleteConfig(
            name="Test Athlete",
            athlete_id=12345,
            access_token="test_access_token",
            refresh_token="test_refresh_token",
            expires_at=1234567890,
            email="invalid-email"
        )
        
        with pytest.raises(ValueError, match="Invalid email format"):
            config.validate()


class TestDatabaseConfig:
    """Test DatabaseConfig dataclass"""
    
    def test_database_config_creation(self):
        """Test creating DatabaseConfig with valid data"""
        config = DatabaseConfig(
            host="localhost",
            port=3306,
            user="test_user",
            password="test_password",
            database="test_db"
        )
        
        assert config.host == "localhost"
        assert config.port == 3306
        assert config.user == "test_user"
        assert config.password == "test_password"
        assert config.database == "test_db"
    
    def test_database_config_validation(self):
        """Test DatabaseConfig validation"""
        config = DatabaseConfig(
            host="localhost",
            port=3306,
            user="test_user",
            password="test_password",
            database="test_db"
        )
        
        # Should not raise any exceptions
        config.validate()
    
    def test_database_config_validation_missing_host(self):
        """Test validation fails with missing host"""
        config = DatabaseConfig(
            host="",
            port=3306,
            user="test_user",
            password="test_password",
            database="test_db"
        )
        
        with pytest.raises(ValueError, match="Database host is required"):
            config.validate()
    
    def test_database_config_validation_invalid_port(self):
        """Test validation fails with invalid port"""
        config = DatabaseConfig(
            host="localhost",
            port=0,
            user="test_user",
            password="test_password",
            database="test_db"
        )
        
        with pytest.raises(ValueError, match="Database port must be between 1 and 65535"):
            config.validate()


class TestEmailConfig:
    """Test EmailConfig dataclass"""
    
    def test_email_config_creation(self):
        """Test creating EmailConfig with valid data"""
        config = EmailConfig(
            api_key="test_api_key",
            from_email="test@example.com",
            from_name="Test Sender"
        )
        
        assert config.api_key == "test_api_key"
        assert config.from_email == "test@example.com"
        assert config.from_name == "Test Sender"
    
    def test_email_config_validation(self):
        """Test EmailConfig validation"""
        config = EmailConfig(
            api_key="test_api_key",
            from_email="test@example.com",
            from_name="Test Sender"
        )
        
        # Should not raise any exceptions
        config.validate()
    
    def test_email_config_validation_missing_api_key(self):
        """Test validation fails with missing API key"""
        config = EmailConfig(
            api_key="",
            from_email="test@example.com",
            from_name="Test Sender"
        )
        
        with pytest.raises(ValueError, match="Email API key is required"):
            config.validate()
    
    def test_email_config_validation_invalid_from_email(self):
        """Test validation fails with invalid from email"""
        config = EmailConfig(
            api_key="test_api_key",
            from_email="invalid-email",
            from_name="Test Sender"
        )
        
        with pytest.raises(ValueError, match="Invalid from email format"):
            config.validate()


class TestConfig:
    """Test main Config class"""
    
    @patch.dict(os.environ, {
        'DOMINIC_NAME': 'Dominic',
        'DOMINIC_ATHLETE_ID': '12345',
        'DOMINIC_ACCESS_TOKEN': 'dominic_access_token',
        'DOMINIC_REFRESH_TOKEN': 'dominic_refresh_token',
        'DOMINIC_TOKEN_EXPIRES': '1234567890',
        'DOMINIC_EMAIL': 'dominic@example.com',
        'CLARE_NAME': 'Clare',
        'CLARE_ATHLETE_ID': '67890',
        'CLARE_ACCESS_TOKEN': 'clare_access_token',
        'CLARE_REFRESH_TOKEN': 'clare_refresh_token',
        'CLARE_TOKEN_EXPIRES': '1234567890',
        'CLARE_EMAIL': 'clare@example.com',
        'DB_HOST': 'localhost',
        'DB_PORT': '3306',
        'DB_USER': 'test_user',
        'DB_PASSWORD': 'test_password',
        'DB_NAME': 'test_db',
        'BREVO_API_KEY': 'test_brevo_key',
        'BREVO_FROM_EMAIL': 'sender@example.com',
        'BREVO_FROM_NAME': 'Test Sender'
    })
    def test_config_from_env(self):
        """Test creating Config from environment variables"""
        config = Config.from_env()
        
        assert len(config.athletes) == 2
        assert config.athletes[0].name == "Dominic"
        assert config.athletes[0].athlete_id == 12345
        assert config.athletes[1].name == "Clare"
        assert config.athletes[1].athlete_id == 67890
        
        assert config.database.host == "localhost"
        assert config.database.port == 3306
        assert config.database.user == "test_user"
        
        assert config.email.api_key == "test_brevo_key"
        assert config.email.from_email == "sender@example.com"
    
    @patch.dict(os.environ, {
        'DOMINIC_NAME': 'Dominic',
        'DOMINIC_ATHLETE_ID': '12345',
        'DOMINIC_ACCESS_TOKEN': 'dominic_access_token',
        'DOMINIC_REFRESH_TOKEN': 'dominic_refresh_token',
        'DOMINIC_TOKEN_EXPIRES': '1234567890',
        'DOMINIC_EMAIL': 'dominic@example.com',
        'DB_HOST': 'localhost',
        'DB_PORT': '3306',
        'DB_USER': 'test_user',
        'DB_PASSWORD': 'test_password',
        'DB_NAME': 'test_db',
        'BREVO_API_KEY': 'test_brevo_key',
        'BREVO_FROM_EMAIL': 'sender@example.com',
        'BREVO_FROM_NAME': 'Test Sender'
    })
    def test_config_from_env_single_athlete(self):
        """Test creating Config with only one athlete"""
        config = Config.from_env()
        
        assert len(config.athletes) == 1
        assert config.athletes[0].name == "Dominic"
        assert config.athletes[0].athlete_id == 12345
    
    def test_config_validation(self):
        """Test Config validation"""
        athletes = [
            AthleteConfig(
                name="Test Athlete",
                athlete_id=12345,
                access_token="test_access_token",
                refresh_token="test_refresh_token",
                expires_at=1234567890,
                email="test@example.com"
            )
        ]
        
        database = DatabaseConfig(
            host="localhost",
            port=3306,
            user="test_user",
            password="test_password",
            database="test_db"
        )
        
        email = EmailConfig(
            api_key="test_api_key",
            from_email="test@example.com",
            from_name="Test Sender"
        )
        
        config = Config(athletes=athletes, database=database, email=email)
        
        # Should not raise any exceptions
        config.validate()
    
    def test_config_validation_no_athletes(self):
        """Test validation fails with no athletes"""
        database = DatabaseConfig(
            host="localhost",
            port=3306,
            user="test_user",
            password="test_password",
            database="test_db"
        )
        
        email = EmailConfig(
            api_key="test_api_key",
            from_email="test@example.com",
            from_name="Test Sender"
        )
        
        config = Config(athletes=[], database=database, email=email)
        
        with pytest.raises(ValueError, match="At least one athlete configuration is required"):
            config.validate()
    
    def test_get_athlete_by_id(self):
        """Test getting athlete by ID"""
        athletes = [
            AthleteConfig(
                name="Dominic",
                athlete_id=12345,
                access_token="dominic_access_token",
                refresh_token="dominic_refresh_token",
                expires_at=1234567890,
                email="dominic@example.com"
            ),
            AthleteConfig(
                name="Clare",
                athlete_id=67890,
                access_token="clare_access_token",
                refresh_token="clare_refresh_token",
                expires_at=1234567890,
                email="clare@example.com"
            )
        ]
        
        database = DatabaseConfig(
            host="localhost",
            port=3306,
            user="test_user",
            password="test_password",
            database="test_db"
        )
        
        email = EmailConfig(
            api_key="test_api_key",
            from_email="test@example.com",
            from_name="Test Sender"
        )
        
        config = Config(athletes=athletes, database=database, email=email)
        
        athlete = config.get_athlete_by_id(12345)
        assert athlete is not None
        assert athlete.name == "Dominic"
        
        athlete = config.get_athlete_by_id(67890)
        assert athlete is not None
        assert athlete.name == "Clare"
        
        athlete = config.get_athlete_by_id(99999)
        assert athlete is None
    
    def test_get_athlete_by_name(self):
        """Test getting athlete by name"""
        athletes = [
            AthleteConfig(
                name="Dominic",
                athlete_id=12345,
                access_token="dominic_access_token",
                refresh_token="dominic_refresh_token",
                expires_at=1234567890,
                email="dominic@example.com"
            ),
            AthleteConfig(
                name="Clare",
                athlete_id=67890,
                access_token="clare_access_token",
                refresh_token="clare_refresh_token",
                expires_at=1234567890,
                email="clare@example.com"
            )
        ]
        
        database = DatabaseConfig(
            host="localhost",
            port=3306,
            user="test_user",
            password="test_password",
            database="test_db"
        )
        
        email = EmailConfig(
            api_key="test_api_key",
            from_email="test@example.com",
            from_name="Test Sender"
        )
        
        config = Config(athletes=athletes, database=database, email=email)
        
        athlete = config.get_athlete_by_name("Dominic")
        assert athlete is not None
        assert athlete.athlete_id == 12345
        
        athlete = config.get_athlete_by_name("dominic")  # Case insensitive
        assert athlete is not None
        assert athlete.athlete_id == 12345
        
        athlete = config.get_athlete_by_name("Clare")
        assert athlete is not None
        assert athlete.athlete_id == 67890
        
        athlete = config.get_athlete_by_name("Unknown")
        assert athlete is None