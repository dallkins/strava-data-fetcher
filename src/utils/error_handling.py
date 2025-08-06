"""
Comprehensive error handling utilities for the Strava Data Fetcher application.

This module provides custom exceptions, error handling decorators, and utilities
for consistent error management across all components.
"""

import functools
import traceback
from typing import Any, Callable, Optional, Type, Union
from enum import Enum

from .logging_config import get_logger

logger = get_logger(__name__)


class ErrorSeverity(Enum):
    """Error severity levels for categorizing exceptions."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class StravaDataFetcherError(Exception):
    """Base exception class for all application-specific errors."""
    
    def __init__(
        self, 
        message: str, 
        severity: ErrorSeverity = ErrorSeverity.MEDIUM,
        error_code: Optional[str] = None,
        original_error: Optional[Exception] = None
    ):
        """
        Initialize the error.
        
        Args:
            message: Human-readable error message
            severity: Error severity level
            error_code: Optional error code for categorization
            original_error: Original exception that caused this error
        """
        super().__init__(message)
        self.message = message
        self.severity = severity
        self.error_code = error_code
        self.original_error = original_error
    
    def __str__(self) -> str:
        """Return string representation of the error."""
        parts = [f"[{self.severity.value.upper()}]"]
        
        if self.error_code:
            parts.append(f"({self.error_code})")
        
        parts.append(self.message)
        
        if self.original_error:
            parts.append(f"Caused by: {str(self.original_error)}")
        
        return " ".join(parts)


class ConfigurationError(StravaDataFetcherError):
    """Raised when there are configuration-related issues."""
    
    def __init__(self, message: str, config_field: Optional[str] = None):
        super().__init__(
            message, 
            severity=ErrorSeverity.HIGH,
            error_code="CONFIG_ERROR"
        )
        self.config_field = config_field


class DatabaseError(StravaDataFetcherError):
    """Raised when database operations fail."""
    
    def __init__(self, message: str, operation: Optional[str] = None, original_error: Optional[Exception] = None):
        super().__init__(
            message,
            severity=ErrorSeverity.HIGH,
            error_code="DB_ERROR",
            original_error=original_error
        )
        self.operation = operation


class APIError(StravaDataFetcherError):
    """Raised when API operations fail."""
    
    def __init__(
        self, 
        message: str, 
        status_code: Optional[int] = None,
        endpoint: Optional[str] = None,
        original_error: Optional[Exception] = None
    ):
        super().__init__(
            message,
            severity=ErrorSeverity.MEDIUM,
            error_code="API_ERROR",
            original_error=original_error
        )
        self.status_code = status_code
        self.endpoint = endpoint


class RateLimitError(APIError):
    """Raised when API rate limits are exceeded."""
    
    def __init__(self, message: str = "API rate limit exceeded", retry_after: Optional[int] = None):
        super().__init__(
            message,
            status_code=429,
            error_code="RATE_LIMIT"
        )
        self.retry_after = retry_after


class AuthenticationError(APIError):
    """Raised when API authentication fails."""
    
    def __init__(self, message: str = "Authentication failed"):
        super().__init__(
            message,
            status_code=401,
            error_code="AUTH_ERROR"
        )


class EmailError(StravaDataFetcherError):
    """Raised when email operations fail."""
    
    def __init__(self, message: str, email_type: Optional[str] = None, original_error: Optional[Exception] = None):
        super().__init__(
            message,
            severity=ErrorSeverity.MEDIUM,
            error_code="EMAIL_ERROR",
            original_error=original_error
        )
        self.email_type = email_type


class ValidationError(StravaDataFetcherError):
    """Raised when data validation fails."""
    
    def __init__(self, message: str, field: Optional[str] = None, value: Optional[Any] = None):
        super().__init__(
            message,
            severity=ErrorSeverity.MEDIUM,
            error_code="VALIDATION_ERROR"
        )
        self.field = field
        self.value = value


def handle_errors(
    default_return: Any = None,
    reraise: bool = False,
    log_errors: bool = True,
    error_types: Optional[tuple] = None
):
    """
    Decorator for comprehensive error handling.
    
    Args:
        default_return: Value to return if an error occurs
        reraise: Whether to reraise the exception after handling
        log_errors: Whether to log errors
        error_types: Tuple of exception types to catch (catches all if None)
    
    Returns:
        Decorated function with error handling
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                # Check if we should catch this error type
                if error_types and not isinstance(e, error_types):
                    raise
                
                # Log the error if requested
                if log_errors:
                    if isinstance(e, StravaDataFetcherError):
                        if e.severity == ErrorSeverity.CRITICAL:
                            logger.critical(f"Critical error in {func.__name__}: {e}")
                        elif e.severity == ErrorSeverity.HIGH:
                            logger.error(f"High severity error in {func.__name__}: {e}")
                        elif e.severity == ErrorSeverity.MEDIUM:
                            logger.warning(f"Medium severity error in {func.__name__}: {e}")
                        else:
                            logger.info(f"Low severity error in {func.__name__}: {e}")
                    else:
                        logger.error(f"Unexpected error in {func.__name__}: {e}")
                        logger.debug(f"Traceback: {traceback.format_exc()}")
                
                # Reraise if requested
                if reraise:
                    raise
                
                # Return default value
                return default_return
        
        return wrapper
    return decorator


def handle_async_errors(
    default_return: Any = None,
    reraise: bool = False,
    log_errors: bool = True,
    error_types: Optional[tuple] = None
):
    """
    Decorator for comprehensive async error handling.
    
    Args:
        default_return: Value to return if an error occurs
        reraise: Whether to reraise the exception after handling
        log_errors: Whether to log errors
        error_types: Tuple of exception types to catch (catches all if None)
    
    Returns:
        Decorated async function with error handling
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                # Check if we should catch this error type
                if error_types and not isinstance(e, error_types):
                    raise
                
                # Log the error if requested
                if log_errors:
                    if isinstance(e, StravaDataFetcherError):
                        if e.severity == ErrorSeverity.CRITICAL:
                            logger.critical(f"Critical error in {func.__name__}: {e}")
                        elif e.severity == ErrorSeverity.HIGH:
                            logger.error(f"High severity error in {func.__name__}: {e}")
                        elif e.severity == ErrorSeverity.MEDIUM:
                            logger.warning(f"Medium severity error in {func.__name__}: {e}")
                        else:
                            logger.info(f"Low severity error in {func.__name__}: {e}")
                    else:
                        logger.error(f"Unexpected error in {func.__name__}: {e}")
                        logger.debug(f"Traceback: {traceback.format_exc()}")
                
                # Reraise if requested
                if reraise:
                    raise
                
                # Return default value
                return default_return
        
        return wrapper
    return decorator


def safe_execute(
    func: Callable,
    *args,
    default_return: Any = None,
    log_errors: bool = True,
    **kwargs
) -> Any:
    """
    Safely execute a function with error handling.
    
    Args:
        func: Function to execute
        *args: Positional arguments for the function
        default_return: Value to return if an error occurs
        log_errors: Whether to log errors
        **kwargs: Keyword arguments for the function
    
    Returns:
        Function result or default_return if an error occurs
    """
    try:
        return func(*args, **kwargs)
    except Exception as e:
        if log_errors:
            logger.error(f"Error executing {func.__name__}: {e}")
            logger.debug(f"Traceback: {traceback.format_exc()}")
        return default_return


async def safe_execute_async(
    func: Callable,
    *args,
    default_return: Any = None,
    log_errors: bool = True,
    **kwargs
) -> Any:
    """
    Safely execute an async function with error handling.
    
    Args:
        func: Async function to execute
        *args: Positional arguments for the function
        default_return: Value to return if an error occurs
        log_errors: Whether to log errors
        **kwargs: Keyword arguments for the function
    
    Returns:
        Function result or default_return if an error occurs
    """
    try:
        return await func(*args, **kwargs)
    except Exception as e:
        if log_errors:
            logger.error(f"Error executing {func.__name__}: {e}")
            logger.debug(f"Traceback: {traceback.format_exc()}")
        return default_return


def validate_required_fields(data: dict, required_fields: list, context: str = "data") -> None:
    """
    Validate that required fields are present in data.
    
    Args:
        data: Dictionary to validate
        required_fields: List of required field names
        context: Context description for error messages
    
    Raises:
        ValidationError: If any required fields are missing
    """
    missing_fields = [field for field in required_fields if field not in data or data[field] is None]
    
    if missing_fields:
        raise ValidationError(
            f"Missing required fields in {context}: {', '.join(missing_fields)}",
            field=missing_fields[0] if len(missing_fields) == 1 else None
        )


def validate_email_format(email: str) -> None:
    """
    Validate email address format.
    
    Args:
        email: Email address to validate
    
    Raises:
        ValidationError: If email format is invalid
    """
    import re
    
    if not email:
        raise ValidationError("Email address is required", field="email")
    
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if not re.match(pattern, email):
        raise ValidationError(f"Invalid email format: {email}", field="email", value=email)


def create_error_context(
    operation: str,
    component: str,
    additional_info: Optional[dict] = None
) -> dict:
    """
    Create standardized error context for logging and debugging.
    
    Args:
        operation: Operation being performed
        component: Component where error occurred
        additional_info: Additional context information
    
    Returns:
        Error context dictionary
    """
    context = {
        "operation": operation,
        "component": component,
        "timestamp": logger.handlers[0].formatter.formatTime(logger.makeRecord(
            logger.name, logger.level, __file__, 0, "", (), None
        )) if logger.handlers else None
    }
    
    if additional_info:
        context.update(additional_info)
    
    return context


class ErrorCollector:
    """Utility class for collecting and managing multiple errors."""
    
    def __init__(self):
        """Initialize the error collector."""
        self.errors = []
    
    def add_error(self, error: Union[Exception, str], context: Optional[str] = None):
        """
        Add an error to the collection.
        
        Args:
            error: Error to add (Exception or string)
            context: Optional context information
        """
        if isinstance(error, str):
            error = StravaDataFetcherError(error)
        
        error_info = {
            "error": error,
            "context": context,
            "timestamp": logger.handlers[0].formatter.formatTime(logger.makeRecord(
                logger.name, logger.level, __file__, 0, "", (), None
            )) if logger.handlers else None
        }
        
        self.errors.append(error_info)
    
    def has_errors(self) -> bool:
        """Check if there are any errors."""
        return len(self.errors) > 0
    
    def get_error_count(self) -> int:
        """Get the number of errors."""
        return len(self.errors)
    
    def get_errors_by_severity(self, severity: ErrorSeverity) -> list:
        """Get errors by severity level."""
        return [
            error_info for error_info in self.errors
            if isinstance(error_info["error"], StravaDataFetcherError) 
            and error_info["error"].severity == severity
        ]
    
    def log_all_errors(self):
        """Log all collected errors."""
        for error_info in self.errors:
            error = error_info["error"]
            context = error_info["context"]
            
            message = str(error)
            if context:
                message = f"{context}: {message}"
            
            if isinstance(error, StravaDataFetcherError):
                if error.severity == ErrorSeverity.CRITICAL:
                    logger.critical(message)
                elif error.severity == ErrorSeverity.HIGH:
                    logger.error(message)
                elif error.severity == ErrorSeverity.MEDIUM:
                    logger.warning(message)
                else:
                    logger.info(message)
            else:
                logger.error(message)
    
    def clear(self):
        """Clear all collected errors."""
        self.errors.clear()
    
    def raise_if_errors(self, message: str = "Multiple errors occurred"):
        """
        Raise an exception if there are any errors.
        
        Args:
            message: Message for the raised exception
        
        Raises:
            StravaDataFetcherError: If there are any collected errors
        """
        if self.has_errors():
            error_messages = [str(error_info["error"]) for error_info in self.errors]
            full_message = f"{message}: {'; '.join(error_messages)}"
            
            # Determine severity based on highest severity error
            max_severity = ErrorSeverity.LOW
            for error_info in self.errors:
                error = error_info["error"]
                if isinstance(error, StravaDataFetcherError):
                    if error.severity.value == "critical":
                        max_severity = ErrorSeverity.CRITICAL
                        break
                    elif error.severity.value == "high" and max_severity.value != "critical":
                        max_severity = ErrorSeverity.HIGH
                    elif error.severity.value == "medium" and max_severity.value not in ["critical", "high"]:
                        max_severity = ErrorSeverity.MEDIUM
            
            raise StravaDataFetcherError(full_message, severity=max_severity)