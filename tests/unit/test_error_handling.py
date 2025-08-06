"""
Unit tests for the error handling module.

Tests custom exceptions, decorators, and error handling utilities.
"""

import pytest
import asyncio
from unittest.mock import Mock, patch

from src.utils.error_handling import (
    StravaDataFetcherError, ConfigurationError, DatabaseError, APIError,
    RateLimitError, AuthenticationError, EmailError, ValidationError,
    ErrorSeverity, handle_errors, handle_async_errors, safe_execute,
    safe_execute_async, validate_required_fields, validate_email_format,
    create_error_context, ErrorCollector
)


class TestCustomExceptions:
    """Test custom exception classes."""
    
    def test_strava_data_fetcher_error_basic(self):
        """Test basic StravaDataFetcherError functionality."""
        error = StravaDataFetcherError("Test error")
        assert str(error) == "[MEDIUM] Test error"
        assert error.severity == ErrorSeverity.MEDIUM
        assert error.error_code is None
        assert error.original_error is None
    
    def test_strava_data_fetcher_error_with_details(self):
        """Test StravaDataFetcherError with all details."""
        original = ValueError("Original error")
        error = StravaDataFetcherError(
            "Test error",
            severity=ErrorSeverity.HIGH,
            error_code="TEST_001",
            original_error=original
        )
        
        expected = "[HIGH] (TEST_001) Test error Caused by: Original error"
        assert str(error) == expected
        assert error.severity == ErrorSeverity.HIGH
        assert error.error_code == "TEST_001"
        assert error.original_error == original
    
    def test_configuration_error(self):
        """Test ConfigurationError."""
        error = ConfigurationError("Config error", config_field="test_field")
        assert error.severity == ErrorSeverity.HIGH
        assert error.error_code == "CONFIG_ERROR"
        assert error.config_field == "test_field"
    
    def test_database_error(self):
        """Test DatabaseError."""
        original = Exception("DB connection failed")
        error = DatabaseError("Database error", operation="connect", original_error=original)
        assert error.severity == ErrorSeverity.HIGH
        assert error.error_code == "DB_ERROR"
        assert error.operation == "connect"
        assert error.original_error == original
    
    def test_api_error(self):
        """Test APIError."""
        error = APIError("API error", status_code=500, endpoint="/test")
        assert error.severity == ErrorSeverity.MEDIUM
        assert error.error_code == "API_ERROR"
        assert error.status_code == 500
        assert error.endpoint == "/test"
    
    def test_rate_limit_error(self):
        """Test RateLimitError."""
        error = RateLimitError("Rate limited", retry_after=900)
        assert error.status_code == 429
        assert error.error_code == "RATE_LIMIT"
        assert error.retry_after == 900
    
    def test_authentication_error(self):
        """Test AuthenticationError."""
        error = AuthenticationError("Auth failed")
        assert error.status_code == 401
        assert error.error_code == "AUTH_ERROR"
    
    def test_email_error(self):
        """Test EmailError."""
        error = EmailError("Email error", email_type="notification")
        assert error.severity == ErrorSeverity.MEDIUM
        assert error.error_code == "EMAIL_ERROR"
        assert error.email_type == "notification"
    
    def test_validation_error(self):
        """Test ValidationError."""
        error = ValidationError("Validation error", field="email", value="invalid")
        assert error.severity == ErrorSeverity.MEDIUM
        assert error.error_code == "VALIDATION_ERROR"
        assert error.field == "email"
        assert error.value == "invalid"


class TestErrorDecorators:
    """Test error handling decorators."""
    
    def test_handle_errors_success(self):
        """Test handle_errors decorator with successful function."""
        @handle_errors(default_return="default")
        def test_func():
            return "success"
        
        result = test_func()
        assert result == "success"
    
    def test_handle_errors_with_exception(self):
        """Test handle_errors decorator with exception."""
        @handle_errors(default_return="default", reraise=False)
        def test_func():
            raise ValueError("Test error")
        
        result = test_func()
        assert result == "default"
    
    def test_handle_errors_with_reraise(self):
        """Test handle_errors decorator with reraise=True."""
        @handle_errors(default_return="default", reraise=True)
        def test_func():
            raise ValueError("Test error")
        
        with pytest.raises(ValueError):
            test_func()
    
    def test_handle_errors_specific_types(self):
        """Test handle_errors decorator with specific error types."""
        @handle_errors(default_return="default", error_types=(ValueError,))
        def test_func(error_type):
            if error_type == "value":
                raise ValueError("Value error")
            else:
                raise TypeError("Type error")
        
        # Should catch ValueError
        result = test_func("value")
        assert result == "default"
        
        # Should not catch TypeError
        with pytest.raises(TypeError):
            test_func("type")
    
    @pytest.mark.asyncio
    async def test_handle_async_errors_success(self):
        """Test handle_async_errors decorator with successful function."""
        @handle_async_errors(default_return="default")
        async def test_func():
            return "success"
        
        result = await test_func()
        assert result == "success"
    
    @pytest.mark.asyncio
    async def test_handle_async_errors_with_exception(self):
        """Test handle_async_errors decorator with exception."""
        @handle_async_errors(default_return="default", reraise=False)
        async def test_func():
            raise ValueError("Test error")
        
        result = await test_func()
        assert result == "default"


class TestSafeExecution:
    """Test safe execution utilities."""
    
    def test_safe_execute_success(self):
        """Test safe_execute with successful function."""
        def test_func(x, y):
            return x + y
        
        result = safe_execute(test_func, 2, 3)
        assert result == 5
    
    def test_safe_execute_with_error(self):
        """Test safe_execute with error."""
        def test_func():
            raise ValueError("Test error")
        
        result = safe_execute(test_func, default_return="default")
        assert result == "default"
    
    @pytest.mark.asyncio
    async def test_safe_execute_async_success(self):
        """Test safe_execute_async with successful function."""
        async def test_func(x, y):
            return x + y
        
        result = await safe_execute_async(test_func, 2, 3)
        assert result == 5
    
    @pytest.mark.asyncio
    async def test_safe_execute_async_with_error(self):
        """Test safe_execute_async with error."""
        async def test_func():
            raise ValueError("Test error")
        
        result = await safe_execute_async(test_func, default_return="default")
        assert result == "default"


class TestValidationUtilities:
    """Test validation utility functions."""
    
    def test_validate_required_fields_success(self):
        """Test validate_required_fields with valid data."""
        data = {"name": "test", "email": "test@example.com"}
        required_fields = ["name", "email"]
        
        # Should not raise an exception
        validate_required_fields(data, required_fields)
    
    def test_validate_required_fields_missing(self):
        """Test validate_required_fields with missing fields."""
        data = {"name": "test"}
        required_fields = ["name", "email"]
        
        with pytest.raises(ValidationError) as exc_info:
            validate_required_fields(data, required_fields)
        
        assert "Missing required fields" in str(exc_info.value)
        assert "email" in str(exc_info.value)
    
    def test_validate_required_fields_none_values(self):
        """Test validate_required_fields with None values."""
        data = {"name": "test", "email": None}
        required_fields = ["name", "email"]
        
        with pytest.raises(ValidationError):
            validate_required_fields(data, required_fields)
    
    def test_validate_email_format_valid(self):
        """Test validate_email_format with valid emails."""
        valid_emails = [
            "test@example.com",
            "user.name@domain.co.uk",
            "test+tag@example.org"
        ]
        
        for email in valid_emails:
            # Should not raise an exception
            validate_email_format(email)
    
    def test_validate_email_format_invalid(self):
        """Test validate_email_format with invalid emails."""
        invalid_emails = [
            "",
            "invalid",
            "@example.com",
            "test@",
            "test.example.com"
        ]
        
        for email in invalid_emails:
            with pytest.raises(ValidationError):
                validate_email_format(email)
    
    def test_validate_email_format_empty(self):
        """Test validate_email_format with empty email."""
        with pytest.raises(ValidationError) as exc_info:
            validate_email_format("")
        
        assert "Email address is required" in str(exc_info.value)


class TestErrorContext:
    """Test error context utilities."""
    
    def test_create_error_context_basic(self):
        """Test create_error_context with basic parameters."""
        context = create_error_context("test_operation", "test_component")
        
        assert context["operation"] == "test_operation"
        assert context["component"] == "test_component"
        assert "timestamp" in context
    
    def test_create_error_context_with_additional_info(self):
        """Test create_error_context with additional information."""
        additional_info = {"user_id": 123, "request_id": "abc-123"}
        context = create_error_context(
            "test_operation", 
            "test_component", 
            additional_info=additional_info
        )
        
        assert context["operation"] == "test_operation"
        assert context["component"] == "test_component"
        assert context["user_id"] == 123
        assert context["request_id"] == "abc-123"


class TestErrorCollector:
    """Test ErrorCollector utility class."""
    
    def test_error_collector_basic(self):
        """Test basic ErrorCollector functionality."""
        collector = ErrorCollector()
        
        assert not collector.has_errors()
        assert collector.get_error_count() == 0
        
        collector.add_error("Test error")
        
        assert collector.has_errors()
        assert collector.get_error_count() == 1
    
    def test_error_collector_with_exceptions(self):
        """Test ErrorCollector with exception objects."""
        collector = ErrorCollector()
        
        error1 = ValueError("Value error")
        error2 = StravaDataFetcherError("Custom error", severity=ErrorSeverity.HIGH)
        
        collector.add_error(error1, context="test_context_1")
        collector.add_error(error2, context="test_context_2")
        
        assert collector.get_error_count() == 2
    
    def test_error_collector_by_severity(self):
        """Test ErrorCollector filtering by severity."""
        collector = ErrorCollector()
        
        high_error = StravaDataFetcherError("High error", severity=ErrorSeverity.HIGH)
        low_error = StravaDataFetcherError("Low error", severity=ErrorSeverity.LOW)
        
        collector.add_error(high_error)
        collector.add_error(low_error)
        collector.add_error("String error")  # Will be converted to StravaDataFetcherError
        
        high_errors = collector.get_errors_by_severity(ErrorSeverity.HIGH)
        low_errors = collector.get_errors_by_severity(ErrorSeverity.LOW)
        
        assert len(high_errors) == 1
        assert len(low_errors) == 1
    
    def test_error_collector_clear(self):
        """Test ErrorCollector clear functionality."""
        collector = ErrorCollector()
        
        collector.add_error("Error 1")
        collector.add_error("Error 2")
        
        assert collector.get_error_count() == 2
        
        collector.clear()
        
        assert collector.get_error_count() == 0
        assert not collector.has_errors()
    
    def test_error_collector_raise_if_errors(self):
        """Test ErrorCollector raise_if_errors functionality."""
        collector = ErrorCollector()
        
        # Should not raise when no errors
        collector.raise_if_errors()
        
        # Should raise when errors exist
        collector.add_error("Test error")
        
        with pytest.raises(StravaDataFetcherError) as exc_info:
            collector.raise_if_errors("Custom message")
        
        assert "Custom message" in str(exc_info.value)
        assert "Test error" in str(exc_info.value)
    
    def test_error_collector_severity_priority(self):
        """Test ErrorCollector severity prioritization."""
        collector = ErrorCollector()
        
        # Add errors of different severities
        collector.add_error(StravaDataFetcherError("Low", severity=ErrorSeverity.LOW))
        collector.add_error(StravaDataFetcherError("Critical", severity=ErrorSeverity.CRITICAL))
        collector.add_error(StravaDataFetcherError("Medium", severity=ErrorSeverity.MEDIUM))
        
        # Should raise with CRITICAL severity (highest)
        with pytest.raises(StravaDataFetcherError) as exc_info:
            collector.raise_if_errors()
        
        assert exc_info.value.severity == ErrorSeverity.CRITICAL
    
    @patch('src.utils.error_handling.logger')
    def test_error_collector_log_all_errors(self, mock_logger):
        """Test ErrorCollector log_all_errors functionality."""
        collector = ErrorCollector()
        
        # Add errors of different types
        collector.add_error(StravaDataFetcherError("Critical error", severity=ErrorSeverity.CRITICAL))
        collector.add_error(StravaDataFetcherError("High error", severity=ErrorSeverity.HIGH))
        collector.add_error(StravaDataFetcherError("Medium error", severity=ErrorSeverity.MEDIUM))
        collector.add_error(StravaDataFetcherError("Low error", severity=ErrorSeverity.LOW))
        collector.add_error(ValueError("Regular error"))
        
        collector.log_all_errors()
        
        # Verify appropriate log methods were called
        assert mock_logger.critical.called
        assert mock_logger.error.called
        assert mock_logger.warning.called
        assert mock_logger.info.called


if __name__ == '__main__':
    pytest.main([__file__])