"""
Security utilities for the Strava Data Fetcher application.

This module provides security-related functionality including input validation,
secret management, and security best practices enforcement.
"""

import os
import re
import hashlib
import secrets
import base64
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta

from .logging_config import get_logger
from .error_handling import ValidationError, StravaDataFetcherError

logger = get_logger(__name__)


class SecurityValidator:
    """
    Security validation utilities for input sanitization and validation.
    """
    
    # Regex patterns for validation
    EMAIL_PATTERN = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
    ATHLETE_ID_PATTERN = re.compile(r'^\d{1,15}$')  # Reasonable range for athlete IDs
    TOKEN_PATTERN = re.compile(r'^[a-zA-Z0-9_-]{20,}$')  # Basic token format
    
    @classmethod
    def validate_email(cls, email: str) -> bool:
        """
        Validate email address format.
        
        Args:
            email: Email address to validate
            
        Returns:
            True if valid, False otherwise
        """
        if not email or len(email) > 254:  # RFC 5321 limit
            return False
        return bool(cls.EMAIL_PATTERN.match(email))
    
    @classmethod
    def validate_athlete_id(cls, athlete_id: int) -> bool:
        """
        Validate athlete ID format and range.
        
        Args:
            athlete_id: Athlete ID to validate
            
        Returns:
            True if valid, False otherwise
        """
        if not isinstance(athlete_id, int):
            return False
        return 1 <= athlete_id <= 999999999999999  # Reasonable range
    
    @classmethod
    def validate_token(cls, token: str) -> bool:
        """
        Validate API token format.
        
        Args:
            token: Token to validate
            
        Returns:
            True if valid, False otherwise
        """
        if not token or len(token) < 20 or len(token) > 200:
            return False
        return bool(cls.TOKEN_PATTERN.match(token))
    
    @classmethod
    def sanitize_string(cls, value: str, max_length: int = 255) -> str:
        """
        Sanitize string input by removing potentially dangerous characters.
        
        Args:
            value: String to sanitize
            max_length: Maximum allowed length
            
        Returns:
            Sanitized string
        """
        if not isinstance(value, str):
            raise ValidationError("Value must be a string")
        
        # Remove null bytes and control characters
        sanitized = ''.join(char for char in value if ord(char) >= 32 or char in '\t\n\r')
        
        # Truncate to max length
        if len(sanitized) > max_length:
            sanitized = sanitized[:max_length]
            logger.warning(f"String truncated to {max_length} characters")
        
        return sanitized.strip()
    
    @classmethod
    def validate_database_name(cls, name: str) -> bool:
        """
        Validate database name to prevent injection.
        
        Args:
            name: Database name to validate
            
        Returns:
            True if valid, False otherwise
        """
        if not name or len(name) > 64:
            return False
        
        # Allow only alphanumeric, underscore, and hyphen
        pattern = re.compile(r'^[a-zA-Z0-9_-]+$')
        return bool(pattern.match(name))
    
    @classmethod
    def validate_webhook_signature(cls, payload: bytes, signature: str, secret: str) -> bool:
        """
        Validate webhook signature for authenticity.
        
        Args:
            payload: Raw webhook payload
            signature: Provided signature
            secret: Webhook secret
            
        Returns:
            True if signature is valid, False otherwise
        """
        if not signature or not secret:
            return False
        
        try:
            # Calculate expected signature
            expected_signature = hashlib.sha256(
                (secret + payload.decode('utf-8')).encode('utf-8')
            ).hexdigest()
            
            # Compare signatures securely
            return secrets.compare_digest(signature, expected_signature)
            
        except Exception as e:
            logger.error(f"Error validating webhook signature: {e}")
            return False


class SecretManager:
    """
    Secure management of sensitive configuration data.
    """
    
    def __init__(self):
        """Initialize the secret manager."""
        self.logger = get_logger(__name__)
    
    def get_secret(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """
        Securely retrieve a secret from environment variables.
        
        Args:
            key: Environment variable key
            default: Default value if not found
            
        Returns:
            Secret value or default
        """
        value = os.getenv(key, default)
        
        if value:
            # Log that secret was retrieved (but not the value)
            self.logger.debug(f"Retrieved secret for key: {key}")
        else:
            self.logger.warning(f"Secret not found for key: {key}")
        
        return value
    
    def validate_required_secrets(self, required_keys: List[str]) -> Dict[str, bool]:
        """
        Validate that all required secrets are present.
        
        Args:
            required_keys: List of required environment variable keys
            
        Returns:
            Dictionary mapping keys to presence status
        """
        results = {}
        missing_secrets = []
        
        for key in required_keys:
            value = os.getenv(key)
            is_present = bool(value and value.strip())
            results[key] = is_present
            
            if not is_present:
                missing_secrets.append(key)
        
        if missing_secrets:
            self.logger.error(f"Missing required secrets: {', '.join(missing_secrets)}")
        
        return results
    
    def mask_secret(self, secret: str, visible_chars: int = 4) -> str:
        """
        Mask a secret for safe logging/display.
        
        Args:
            secret: Secret to mask
            visible_chars: Number of characters to show at the end
            
        Returns:
            Masked secret string
        """
        if not secret or len(secret) <= visible_chars:
            return "*" * 8
        
        return "*" * (len(secret) - visible_chars) + secret[-visible_chars:]


class SecurityAuditor:
    """
    Security auditing utilities for the application.
    """
    
    def __init__(self):
        """Initialize the security auditor."""
        self.logger = get_logger(__name__)
        self.validator = SecurityValidator()
        self.secret_manager = SecretManager()
    
    def audit_configuration(self, config_data: Dict[str, Any]) -> Dict[str, List[str]]:
        """
        Audit configuration for security issues.
        
        Args:
            config_data: Configuration data to audit
            
        Returns:
            Dictionary with security findings by category
        """
        findings = {
            'critical': [],
            'high': [],
            'medium': [],
            'low': [],
            'info': []
        }
        
        # Check for hardcoded secrets
        self._check_hardcoded_secrets(config_data, findings)
        
        # Validate email addresses
        self._validate_emails(config_data, findings)
        
        # Check token formats
        self._validate_tokens(config_data, findings)
        
        # Check database configuration
        self._validate_database_config(config_data, findings)
        
        # Check for insecure defaults
        self._check_insecure_defaults(config_data, findings)
        
        return findings
    
    def _check_hardcoded_secrets(self, config_data: Dict[str, Any], findings: Dict[str, List[str]]):
        """Check for hardcoded secrets in configuration."""
        sensitive_keys = ['password', 'token', 'key', 'secret', 'api_key']
        
        def check_dict(data: Dict[str, Any], path: str = ""):
            for key, value in data.items():
                current_path = f"{path}.{key}" if path else key
                
                if isinstance(value, dict):
                    check_dict(value, current_path)
                elif isinstance(value, str) and any(sensitive in key.lower() for sensitive in sensitive_keys):
                    if value and not value.startswith('${') and not value.startswith('$'):
                        findings['high'].append(
                            f"Potential hardcoded secret in {current_path}: {self.secret_manager.mask_secret(value)}"
                        )
        
        check_dict(config_data)
    
    def _validate_emails(self, config_data: Dict[str, Any], findings: Dict[str, List[str]]):
        """Validate email addresses in configuration."""
        def check_emails(data: Dict[str, Any], path: str = ""):
            for key, value in data.items():
                current_path = f"{path}.{key}" if path else key
                
                if isinstance(value, dict):
                    check_emails(value, current_path)
                elif isinstance(value, str) and 'email' in key.lower():
                    if value and not self.validator.validate_email(value):
                        findings['medium'].append(f"Invalid email format in {current_path}: {value}")
        
        check_emails(config_data)
    
    def _validate_tokens(self, config_data: Dict[str, Any], findings: Dict[str, List[str]]):
        """Validate API tokens in configuration."""
        def check_tokens(data: Dict[str, Any], path: str = ""):
            for key, value in data.items():
                current_path = f"{path}.{key}" if path else key
                
                if isinstance(value, dict):
                    check_tokens(value, current_path)
                elif isinstance(value, str) and 'token' in key.lower():
                    if value and not self.validator.validate_token(value):
                        findings['medium'].append(
                            f"Invalid token format in {current_path}: {self.secret_manager.mask_secret(value)}"
                        )
        
        check_tokens(config_data)
    
    def _validate_database_config(self, config_data: Dict[str, Any], findings: Dict[str, List[str]]):
        """Validate database configuration security."""
        db_config = config_data.get('database', {})
        
        # Check for default passwords
        password = db_config.get('password', '')
        if password in ['password', '123456', 'admin', 'root', '']:
            findings['critical'].append("Weak or default database password detected")
        
        # Check database name
        db_name = db_config.get('database', '')
        if db_name and not self.validator.validate_database_name(db_name):
            findings['medium'].append(f"Invalid database name format: {db_name}")
        
        # Check for localhost in production
        host = db_config.get('host', '')
        if host == 'localhost' and os.getenv('APP_ENV') == 'production':
            findings['low'].append("Using localhost for database in production environment")
    
    def _check_insecure_defaults(self, config_data: Dict[str, Any], findings: Dict[str, List[str]]):
        """Check for insecure default configurations."""
        # Check for debug mode in production
        if os.getenv('APP_ENV') == 'production':
            if os.getenv('DEBUG', '').lower() in ['true', '1', 'yes']:
                findings['high'].append("Debug mode enabled in production environment")
        
        # Check for weak encryption settings
        if 'encryption' in config_data:
            encryption_config = config_data['encryption']
            if encryption_config.get('algorithm') in ['md5', 'sha1']:
                findings['high'].append("Weak encryption algorithm detected")
    
    def audit_environment(self) -> Dict[str, List[str]]:
        """
        Audit environment variables for security issues.
        
        Returns:
            Dictionary with security findings by category
        """
        findings = {
            'critical': [],
            'high': [],
            'medium': [],
            'low': [],
            'info': []
        }
        
        # Check for required secrets
        required_secrets = [
            'DB_PASSWORD',
            'BREVO_API_KEY',
            'DOMINIC_ACCESS_TOKEN',
            'DOMINIC_REFRESH_TOKEN'
        ]
        
        secret_status = self.secret_manager.validate_required_secrets(required_secrets)
        missing_secrets = [key for key, present in secret_status.items() if not present]
        
        if missing_secrets:
            findings['critical'].extend([
                f"Missing required secret: {secret}" for secret in missing_secrets
            ])
        
        # Check for insecure environment settings
        if os.getenv('SSL_VERIFY', '').lower() == 'false':
            findings['high'].append("SSL verification disabled")
        
        if os.getenv('LOG_LEVEL', '').upper() == 'DEBUG' and os.getenv('APP_ENV') == 'production':
            findings['medium'].append("Debug logging enabled in production")
        
        return findings
    
    def generate_security_report(self) -> str:
        """
        Generate a comprehensive security audit report.
        
        Returns:
            Formatted security report
        """
        report_lines = [
            "🔒 SECURITY AUDIT REPORT",
            "=" * 50,
            f"Generated: {datetime.now().isoformat()}",
            ""
        ]
        
        # Audit environment
        env_findings = self.audit_environment()
        
        report_lines.extend([
            "📋 ENVIRONMENT AUDIT",
            "-" * 30
        ])
        
        total_issues = 0
        for severity, issues in env_findings.items():
            if issues:
                report_lines.append(f"\n{severity.upper()} ({len(issues)} issues):")
                for issue in issues:
                    report_lines.append(f"  • {issue}")
                total_issues += len(issues)
        
        if total_issues == 0:
            report_lines.append("✅ No security issues found in environment")
        
        # Security recommendations
        report_lines.extend([
            "",
            "🛡️ SECURITY RECOMMENDATIONS",
            "-" * 30,
            "• Use strong, unique passwords for all accounts",
            "• Rotate API tokens regularly",
            "• Enable SSL/TLS for all connections",
            "• Use environment variables for all secrets",
            "• Implement proper input validation",
            "• Monitor logs for suspicious activity",
            "• Keep dependencies updated",
            "• Use webhook signature validation",
            ""
        ])
        
        return "\n".join(report_lines)


def create_security_headers() -> Dict[str, str]:
    """
    Create security headers for HTTP responses.
    
    Returns:
        Dictionary of security headers
    """
    return {
        'X-Content-Type-Options': 'nosniff',
        'X-Frame-Options': 'DENY',
        'X-XSS-Protection': '1; mode=block',
        'Strict-Transport-Security': 'max-age=31536000; includeSubDomains',
        'Content-Security-Policy': "default-src 'self'",
        'Referrer-Policy': 'strict-origin-when-cross-origin'
    }


def generate_secure_token(length: int = 32) -> str:
    """
    Generate a cryptographically secure random token.
    
    Args:
        length: Length of the token in bytes
        
    Returns:
        Base64-encoded secure token
    """
    token_bytes = secrets.token_bytes(length)
    return base64.urlsafe_b64encode(token_bytes).decode('utf-8').rstrip('=')


def hash_sensitive_data(data: str, salt: Optional[str] = None) -> str:
    """
    Hash sensitive data with optional salt.
    
    Args:
        data: Data to hash
        salt: Optional salt (generated if not provided)
        
    Returns:
        Hashed data as hex string
    """
    if salt is None:
        salt = secrets.token_hex(16)
    
    hash_obj = hashlib.sha256()
    hash_obj.update((salt + data).encode('utf-8'))
    
    return f"{salt}:{hash_obj.hexdigest()}"


def verify_hashed_data(data: str, hashed_data: str) -> bool:
    """
    Verify data against its hash.
    
    Args:
        data: Original data
        hashed_data: Hashed data with salt
        
    Returns:
        True if data matches hash, False otherwise
    """
    try:
        salt, hash_value = hashed_data.split(':', 1)
        expected_hash = hash_sensitive_data(data, salt)
        return secrets.compare_digest(hashed_data, expected_hash)
    except (ValueError, AttributeError):
        return False


# Global security auditor instance
security_auditor = SecurityAuditor()


def get_security_auditor() -> SecurityAuditor:
    """Get the global security auditor instance."""
    return security_auditor