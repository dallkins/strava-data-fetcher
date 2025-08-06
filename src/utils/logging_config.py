"""
Centralized logging configuration for Strava Data Fetcher.

This module provides consistent logging setup across all components
with proper formatting, file rotation, and log levels.
"""

import os
import logging
import logging.handlers
from datetime import datetime
from pathlib import Path
from typing import Optional


class StravaLogger:
    """Centralized logging configuration for the Strava Data Fetcher application"""
    
    def __init__(self, 
                 log_dir: str = "logs",
                 log_level: str = "INFO",
                 max_file_size: int = 10 * 1024 * 1024,  # 10MB
                 backup_count: int = 5):
        """
        Initialize the logging configuration.
        
        Args:
            log_dir: Directory to store log files
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            max_file_size: Maximum size of each log file in bytes
            backup_count: Number of backup log files to keep
        """
        self.log_dir = Path(log_dir)
        self.log_level = getattr(logging, log_level.upper(), logging.INFO)
        self.max_file_size = max_file_size
        self.backup_count = backup_count
        
        # Create log directory if it doesn't exist
        self.log_dir.mkdir(exist_ok=True)
        
        # Configure logging
        self._setup_logging()
    
    def _setup_logging(self):
        """Setup logging configuration with file rotation and console output"""
        
        # Create custom formatter
        formatter = logging.Formatter(
            fmt='%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Console formatter (simpler for readability)
        console_formatter = logging.Formatter(
            fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%H:%M:%S'
        )
        
        # Configure root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(self.log_level)
        
        # Clear any existing handlers
        root_logger.handlers.clear()
        
        # File handler with rotation
        file_handler = logging.handlers.RotatingFileHandler(
            filename=self.log_dir / "strava_fetcher.log",
            maxBytes=self.max_file_size,
            backupCount=self.backup_count,
            encoding='utf-8'
        )
        file_handler.setLevel(self.log_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(self.log_level)
        console_handler.setFormatter(console_formatter)
        root_logger.addHandler(console_handler)
        
        # Error file handler (separate file for errors and above)
        error_handler = logging.handlers.RotatingFileHandler(
            filename=self.log_dir / "strava_fetcher_errors.log",
            maxBytes=self.max_file_size,
            backupCount=self.backup_count,
            encoding='utf-8'
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(formatter)
        root_logger.addHandler(error_handler)
        
        # Webhook-specific handler
        webhook_handler = logging.handlers.RotatingFileHandler(
            filename=self.log_dir / "webhook.log",
            maxBytes=self.max_file_size,
            backupCount=self.backup_count,
            encoding='utf-8'
        )
        webhook_handler.setLevel(self.log_level)
        webhook_handler.setFormatter(formatter)
        
        # Add webhook handler to webhook logger
        webhook_logger = logging.getLogger('webhook')
        webhook_logger.addHandler(webhook_handler)
        webhook_logger.setLevel(self.log_level)
        webhook_logger.propagate = True  # Also send to root logger
        
        # Configure third-party loggers to reduce noise
        self._configure_third_party_loggers()
        
        # Log startup message
        logger = logging.getLogger(__name__)
        logger.info(f"Logging initialized - Level: {logging.getLevelName(self.log_level)}")
        logger.info(f"Log directory: {self.log_dir.absolute()}")
    
    def _configure_third_party_loggers(self):
        """Configure third-party library loggers to reduce noise"""
        
        # Reduce aiohttp logging noise
        logging.getLogger('aiohttp').setLevel(logging.WARNING)
        logging.getLogger('aiohttp.access').setLevel(logging.WARNING)
        
        # Reduce MySQL connector logging noise
        logging.getLogger('mysql.connector').setLevel(logging.WARNING)
        
        # Reduce requests logging noise
        logging.getLogger('urllib3').setLevel(logging.WARNING)
        logging.getLogger('requests').setLevel(logging.WARNING)
        
        # Reduce Flask logging noise in production
        if os.getenv('FLASK_ENV') != 'development':
            logging.getLogger('werkzeug').setLevel(logging.WARNING)
        
        # Reduce APScheduler logging noise
        logging.getLogger('apscheduler').setLevel(logging.WARNING)
    
    def get_logger(self, name: str) -> logging.Logger:
        """
        Get a logger instance for a specific module.
        
        Args:
            name: Logger name (usually __name__)
            
        Returns:
            Configured logger instance
        """
        return logging.getLogger(name)
    
    def set_level(self, level: str):
        """
        Change the logging level for all handlers.
        
        Args:
            level: New logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        """
        new_level = getattr(logging, level.upper(), logging.INFO)
        
        root_logger = logging.getLogger()
        root_logger.setLevel(new_level)
        
        for handler in root_logger.handlers:
            if not isinstance(handler, logging.handlers.RotatingFileHandler) or \
               'errors.log' not in str(handler.baseFilename):
                handler.setLevel(new_level)
        
        logger = logging.getLogger(__name__)
        logger.info(f"Logging level changed to: {level.upper()}")
    
    def add_performance_logging(self):
        """Add performance-specific logging configuration"""
        
        # Performance logger for timing operations
        perf_handler = logging.handlers.RotatingFileHandler(
            filename=self.log_dir / "performance.log",
            maxBytes=self.max_file_size,
            backupCount=self.backup_count,
            encoding='utf-8'
        )
        
        perf_formatter = logging.Formatter(
            fmt='%(asctime)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S.%f'
        )
        perf_handler.setFormatter(perf_formatter)
        
        perf_logger = logging.getLogger('performance')
        perf_logger.addHandler(perf_handler)
        perf_logger.setLevel(logging.INFO)
        perf_logger.propagate = False  # Don't send to root logger
        
        return perf_logger
    
    def cleanup_old_logs(self, days_to_keep: int = 30):
        """
        Clean up log files older than specified days.
        
        Args:
            days_to_keep: Number of days of logs to keep
        """
        import time
        
        cutoff_time = time.time() - (days_to_keep * 24 * 60 * 60)
        
        for log_file in self.log_dir.glob("*.log*"):
            try:
                if log_file.stat().st_mtime < cutoff_time:
                    log_file.unlink()
                    logger = logging.getLogger(__name__)
                    logger.info(f"Cleaned up old log file: {log_file}")
            except Exception as e:
                logger = logging.getLogger(__name__)
                logger.warning(f"Failed to clean up log file {log_file}: {e}")


def setup_logging(log_level: Optional[str] = None, 
                  log_dir: Optional[str] = None) -> StravaLogger:
    """
    Setup logging for the Strava Data Fetcher application.
    
    Args:
        log_level: Logging level from environment or default to INFO
        log_dir: Log directory from environment or default to 'logs'
        
    Returns:
        Configured StravaLogger instance
    """
    
    # Get configuration from environment variables
    log_level = log_level or os.getenv('LOG_LEVEL', 'INFO')
    log_dir = log_dir or os.getenv('LOG_DIR', 'logs')
    
    # Create and return logger instance
    strava_logger = StravaLogger(
        log_dir=log_dir,
        log_level=log_level,
        max_file_size=int(os.getenv('LOG_MAX_FILE_SIZE', 10 * 1024 * 1024)),
        backup_count=int(os.getenv('LOG_BACKUP_COUNT', 5))
    )
    
    return strava_logger


def get_logger(name: str) -> logging.Logger:
    """
    Convenience function to get a logger instance.
    
    Args:
        name: Logger name (usually __name__)
        
    Returns:
        Logger instance
    """
    return logging.getLogger(name)


class PerformanceTimer:
    """Context manager for timing operations and logging performance metrics"""
    
    def __init__(self, operation_name: str, logger: Optional[logging.Logger] = None):
        """
        Initialize performance timer.
        
        Args:
            operation_name: Name of the operation being timed
            logger: Logger instance (defaults to performance logger)
        """
        self.operation_name = operation_name
        self.logger = logger or logging.getLogger('performance')
        self.start_time = None
    
    def __enter__(self):
        """Start timing"""
        self.start_time = datetime.now()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """End timing and log result"""
        if self.start_time:
            duration = datetime.now() - self.start_time
            duration_ms = duration.total_seconds() * 1000
            
            if exc_type is None:
                self.logger.info(f"{self.operation_name} completed in {duration_ms:.2f}ms")
            else:
                self.logger.error(f"{self.operation_name} failed after {duration_ms:.2f}ms - {exc_type.__name__}: {exc_val}")


# Example usage and testing
if __name__ == '__main__':
    # Setup logging
    strava_logger = setup_logging(log_level='DEBUG')
    
    # Get loggers for different modules
    main_logger = get_logger(__name__)
    api_logger = get_logger('strava_api')
    db_logger = get_logger('database')
    
    # Test logging at different levels
    main_logger.debug("This is a debug message")
    main_logger.info("Application started successfully")
    main_logger.warning("This is a warning message")
    main_logger.error("This is an error message")
    
    # Test performance logging
    perf_logger = strava_logger.add_performance_logging()
    
    with PerformanceTimer("Test Operation", perf_logger):
        import time
        time.sleep(0.1)  # Simulate work
    
    # Test different module loggers
    api_logger.info("API request completed")
    db_logger.info("Database query executed")
    
    print(f"Logs written to: {strava_logger.log_dir.absolute()}")