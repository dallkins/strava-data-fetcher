"""
Unit tests for email notification service module.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import requests

from src.notifications.email_service import EmailService
from src.utils.config import EmailConfig


class TestEmailService:
    """Test EmailService class"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.email_config = EmailConfig(
            api_key="test_api_key",
            from_email="test@example.com",
            from_name="Test Sender"
        )
        self.email_service = EmailService(self.email_config)
    
    def test_init(self):
        """Test EmailService initialization"""
        assert self.email_service.config == self.email_config
        assert self.email_service.api_url == "https://api.brevo.com/v3/smtp/email"
    
    def test_format_activity_email_basic(self):
        """Test formatting basic activity email"""
        activity_data = {
            "id": 12345,
            "name": "Morning Ride",
            "athlete_name": "Test Athlete",
            "distance_km": 25.5,
            "elevation_m": 300,
            "calories": 500,
            "start_date_local": "2024-01-01 08:00:00"
        }
        
        subject, html_content = self.email_service.format_activity_email(
            activity_data, "create"
        )
        
        assert "🆕 Morning Ride" in subject
        assert "Test Athlete" in html_content
        assert "25.5 km" in html_content
        assert "300 m" in html_content
        assert "500" in html_content
        assert "Morning Ride" in html_content
    
    def test_format_activity_email_update(self):
        """Test formatting activity update email"""
        activity_data = {
            "id": 12345,
            "name": "Updated Ride",
            "athlete_name": "Test Athlete",
            "distance_km": 30.0,
            "elevation_m": 400,
            "calories": 600,
            "start_date_local": "2024-01-01 08:00:00"
        }
        
        subject, html_content = self.email_service.format_activity_email(
            activity_data, "update"
        )
        
        assert "🔄 Updated Ride" in subject
        assert "Activity Updated" in html_content
    
    def test_format_activity_email_missing_data(self):
        """Test formatting email with missing data"""
        activity_data = {
            "id": 12345,
            "name": "Minimal Ride",
            "athlete_name": "Test Athlete",
            "distance_km": 0,
            "elevation_m": 0,
            "calories": 0,
            "start_date_local": None
        }
        
        subject, html_content = self.email_service.format_activity_email(
            activity_data, "create"
        )
        
        assert "🆕 Minimal Ride" in subject
        assert "0 km" in html_content
        assert "0 m" in html_content
        assert "0" in html_content  # calories
    
    @patch('src.notifications.email_service.requests.post')
    def test_send_email_success(self, mock_post):
        """Test successful email sending"""
        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.json.return_value = {"messageId": "test-message-id"}
        mock_post.return_value = mock_response
        
        result = self.email_service.send_email(
            to_email="recipient@example.com",
            subject="Test Subject",
            html_content="<p>Test content</p>"
        )
        
        assert result is True
        mock_post.assert_called_once()
        
        # Verify request parameters
        call_args = mock_post.call_args
        assert call_args[0][0] == "https://api.brevo.com/v3/smtp/email"
        
        # Verify headers
        headers = call_args[1]['headers']
        assert headers['api-key'] == "test_api_key"
        assert headers['Content-Type'] == "application/json"
        
        # Verify payload
        payload = call_args[1]['json']
        assert payload['sender']['email'] == "test@example.com"
        assert payload['sender']['name'] == "Test Sender"
        assert payload['to'] == [{"email": "recipient@example.com"}]
        assert payload['subject'] == "Test Subject"
        assert payload['htmlContent'] == "<p>Test content</p>"
    
    @patch('src.notifications.email_service.requests.post')
    def test_send_email_multiple_recipients(self, mock_post):
        """Test sending email to multiple recipients"""
        mock_response = Mock()
        mock_response.status_code = 201
        mock_post.return_value = mock_response
        
        recipients = ["user1@example.com", "user2@example.com"]
        
        result = self.email_service.send_email(
            to_email=recipients,
            subject="Test Subject",
            html_content="<p>Test content</p>"
        )
        
        assert result is True
        
        # Verify payload has multiple recipients
        payload = mock_post.call_args[1]['json']
        expected_to = [{"email": "user1@example.com"}, {"email": "user2@example.com"}]
        assert payload['to'] == expected_to
    
    @patch('src.notifications.email_service.requests.post')
    def test_send_email_api_error(self, mock_post):
        """Test email sending with API error"""
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.text = "Bad Request"
        mock_post.return_value = mock_response
        
        result = self.email_service.send_email(
            to_email="recipient@example.com",
            subject="Test Subject",
            html_content="<p>Test content</p>"
        )
        
        assert result is False
    
    @patch('src.notifications.email_service.requests.post')
    def test_send_email_network_error(self, mock_post):
        """Test email sending with network error"""
        mock_post.side_effect = requests.RequestException("Network error")
        
        result = self.email_service.send_email(
            to_email="recipient@example.com",
            subject="Test Subject",
            html_content="<p>Test content</p>"
        )
        
        assert result is False
    
    def test_send_email_invalid_recipient(self):
        """Test sending email with invalid recipient"""
        result = self.email_service.send_email(
            to_email="",
            subject="Test Subject",
            html_content="<p>Test content</p>"
        )
        
        assert result is False
    
    def test_send_email_no_api_key(self):
        """Test sending email without API key"""
        config_no_key = EmailConfig(
            api_key="",
            from_email="test@example.com",
            from_name="Test Sender"
        )
        service = EmailService(config_no_key)
        
        result = service.send_email(
            to_email="recipient@example.com",
            subject="Test Subject",
            html_content="<p>Test content</p>"
        )
        
        assert result is False
    
    @patch('src.notifications.email_service.requests.post')
    def test_send_activity_notification_success(self, mock_post):
        """Test sending activity notification"""
        mock_response = Mock()
        mock_response.status_code = 201
        mock_post.return_value = mock_response
        
        activity_data = {
            "id": 12345,
            "name": "Morning Ride",
            "athlete_name": "Test Athlete",
            "distance_km": 25.5,
            "elevation_m": 300,
            "calories": 500,
            "start_date_local": "2024-01-01 08:00:00"
        }
        
        result = self.email_service.send_activity_notification(
            activity_data=activity_data,
            event_type="create",
            to_email="athlete@example.com"
        )
        
        assert result is True
        mock_post.assert_called_once()
        
        # Verify the email content was formatted correctly
        payload = mock_post.call_args[1]['json']
        assert "🆕 Morning Ride" in payload['subject']
        assert "Test Athlete" in payload['htmlContent']
    
    def test_send_activity_notification_missing_data(self):
        """Test sending activity notification with missing data"""
        result = self.email_service.send_activity_notification(
            activity_data=None,
            event_type="create",
            to_email="athlete@example.com"
        )
        
        assert result is False
    
    def test_send_activity_notification_invalid_email(self):
        """Test sending activity notification with invalid email"""
        activity_data = {
            "id": 12345,
            "name": "Morning Ride",
            "athlete_name": "Test Athlete",
            "distance_km": 25.5,
            "elevation_m": 300,
            "calories": 500,
            "start_date_local": "2024-01-01 08:00:00"
        }
        
        result = self.email_service.send_activity_notification(
            activity_data=activity_data,
            event_type="create",
            to_email=""
        )
        
        assert result is False
    
    def test_validate_email_valid(self):
        """Test email validation with valid emails"""
        valid_emails = [
            "test@example.com",
            "user.name@domain.co.uk",
            "user+tag@example.org",
            "123@example.com"
        ]
        
        for email in valid_emails:
            assert self.email_service._validate_email(email) is True
    
    def test_validate_email_invalid(self):
        """Test email validation with invalid emails"""
        invalid_emails = [
            "",
            "invalid-email",
            "@example.com",
            "user@",
            "user@.com",
            "user space@example.com",
            None
        ]
        
        for email in invalid_emails:
            assert self.email_service._validate_email(email) is False
    
    def test_format_distance(self):
        """Test distance formatting"""
        assert self.email_service._format_distance(0) == "0.0 km"
        assert self.email_service._format_distance(1000) == "1.0 km"
        assert self.email_service._format_distance(1500) == "1.5 km"
        assert self.email_service._format_distance(25500) == "25.5 km"
        assert self.email_service._format_distance(None) == "0.0 km"
    
    def test_format_elevation(self):
        """Test elevation formatting"""
        assert self.email_service._format_elevation(0) == "0 m"
        assert self.email_service._format_elevation(100) == "100 m"
        assert self.email_service._format_elevation(1500) == "1,500 m"
        assert self.email_service._format_elevation(None) == "0 m"
    
    def test_format_calories(self):
        """Test calories formatting"""
        assert self.email_service._format_calories(0) == "0"
        assert self.email_service._format_calories(500) == "500"
        assert self.email_service._format_calories(1500) == "1,500"
        assert self.email_service._format_calories(None) == "0"
    
    def test_format_date(self):
        """Test date formatting"""
        # Test with string date
        date_str = "2024-01-01 08:30:00"
        formatted = self.email_service._format_date(date_str)
        assert "January 1, 2024" in formatted
        assert "8:30 AM" in formatted
        
        # Test with None
        assert self.email_service._format_date(None) == "Unknown date"
        
        # Test with empty string
        assert self.email_service._format_date("") == "Unknown date"
    
    def test_get_event_emoji_and_title(self):
        """Test getting event emoji and title"""
        emoji, title = self.email_service._get_event_emoji_and_title("create")
        assert emoji == "🆕"
        assert title == "New Activity"
        
        emoji, title = self.email_service._get_event_emoji_and_title("update")
        assert emoji == "🔄"
        assert title == "Activity Updated"
        
        # Test unknown event type
        emoji, title = self.email_service._get_event_emoji_and_title("unknown")
        assert emoji == "📝"
        assert title == "Activity Event"
    
    @patch('src.notifications.email_service.requests.post')
    def test_send_summary_email(self, mock_post):
        """Test sending summary email"""
        mock_response = Mock()
        mock_response.status_code = 201
        mock_post.return_value = mock_response
        
        summary_data = {
            "period": "weekly",
            "total_activities": 5,
            "total_distance_km": 125.5,
            "total_elevation_m": 1500,
            "total_calories": 2500,
            "start_date": "2024-01-01",
            "end_date": "2024-01-07"
        }
        
        result = self.email_service.send_summary_email(
            summary_data=summary_data,
            to_email="athlete@example.com"
        )
        
        assert result is True
        mock_post.assert_called_once()
        
        # Verify the email content
        payload = mock_post.call_args[1]['json']
        assert "Weekly Summary" in payload['subject']
        assert "5" in payload['htmlContent']  # total activities
        assert "125.5 km" in payload['htmlContent']  # total distance
    
    def test_send_summary_email_missing_data(self):
        """Test sending summary email with missing data"""
        result = self.email_service.send_summary_email(
            summary_data=None,
            to_email="athlete@example.com"
        )
        
        assert result is False
    
    def test_format_summary_email_weekly(self):
        """Test formatting weekly summary email"""
        summary_data = {
            "period": "weekly",
            "total_activities": 3,
            "total_distance_km": 75.0,
            "total_elevation_m": 900,
            "total_calories": 1500,
            "start_date": "2024-01-01",
            "end_date": "2024-01-07"
        }
        
        subject, html_content = self.email_service._format_summary_email(summary_data)
        
        assert "📊 Weekly Summary" in subject
        assert "3" in html_content
        assert "75.0 km" in html_content
        assert "900 m" in html_content
        assert "1,500" in html_content
    
    def test_format_summary_email_monthly(self):
        """Test formatting monthly summary email"""
        summary_data = {
            "period": "monthly",
            "total_activities": 15,
            "total_distance_km": 375.0,
            "total_elevation_m": 4500,
            "total_calories": 7500,
            "start_date": "2024-01-01",
            "end_date": "2024-01-31"
        }
        
        subject, html_content = self.email_service._format_summary_email(summary_data)
        
        assert "📊 Monthly Summary" in subject
        assert "15" in html_content
        assert "375.0 km" in html_content