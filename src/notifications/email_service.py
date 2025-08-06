"""
Email notification service using Brevo API.

This module provides email functionality for sending activity notifications
and summary reports with proper formatting and error handling.
"""

import re
from datetime import datetime
from typing import List, Union, Optional, Dict, Any, Tuple

import requests

from ..utils.config import EmailConfig
from ..utils.logging_config import get_logger, PerformanceTimer
from ..utils.error_handling import EmailError, ValidationError, handle_errors, validate_email_format

logger = get_logger(__name__)


class EmailService:
    """
    Email service for sending Strava activity notifications and summaries.
    
    Uses the Brevo API for reliable email delivery with proper formatting
    and error handling.
    """
    
    def __init__(self, config: EmailConfig):
        """
        Initialize the email service.
        
        Args:
            config: Email configuration
        """
        self.config = config
        self.api_url = "https://api.brevo.com/v3/smtp/email"
    
    def _validate_email(self, email: str) -> bool:
        """
        Validate email address format.
        
        Args:
            email: Email address to validate
            
        Returns:
            True if valid, False otherwise
        """
        if not email:
            return False
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email))
    
    def _format_distance(self, distance_m: Optional[float]) -> str:
        """Format distance in kilometers."""
        if distance_m is None:
            return "0.0 km"
        return f"{distance_m / 1000:.1f} km"
    
    def _format_elevation(self, elevation_m: Optional[float]) -> str:
        """Format elevation in meters."""
        if elevation_m is None:
            return "0 m"
        return f"{int(elevation_m):,} m"
    
    def _format_calories(self, calories: Optional[int]) -> str:
        """Format calories with comma separators."""
        if calories is None:
            return "0"
        return f"{int(calories):,}"
    
    def _format_date(self, date_str: Optional[str]) -> str:
        """Format date string for display."""
        if not date_str:
            return "Unknown date"
        
        try:
            # Parse the date string (assuming ISO format)
            if 'T' in date_str:
                dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            else:
                dt = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
            
            return dt.strftime('%A, %B %d, %Y at %I:%M %p')
        except (ValueError, TypeError):
            return date_str or "Unknown date"
    
    def _get_event_emoji_and_title(self, event_type: str) -> Tuple[str, str]:
        """Get emoji and title for event type."""
        event_map = {
            'create': ('🆕', 'New Activity'),
            'update': ('🔄', 'Activity Updated')
        }
        return event_map.get(event_type, ('📝', 'Activity Event'))
    
    def send_email(
        self,
        to_email: Union[str, List[str]],
        subject: str,
        html_content: str
    ) -> bool:
        """
        Send an email using the Brevo API.
        
        Args:
            to_email: Recipient email address(es)
            subject: Email subject
            html_content: HTML email content
            
        Returns:
            True if sent successfully, False otherwise
        """
        with PerformanceTimer(f"Send email to {len(to_email) if isinstance(to_email, list) else 1} recipients"):
            if not self.config.api_key:
                raise EmailError("No API key configured for email service")
            
            # Normalize recipients to list
            if isinstance(to_email, str):
                recipients = [to_email]
            else:
                recipients = to_email
            
            # Validate recipients
            valid_recipients = []
            for email in recipients:
                try:
                    validate_email_format(email)
                    valid_recipients.append(email)
                except ValidationError:
                    logger.warning(f"Invalid email address: {email}")
            
            if not valid_recipients:
                raise EmailError("No valid recipient email addresses")
            
            try:
                headers = {
                    'api-key': self.config.api_key,
                    'Content-Type': 'application/json'
                }
                
                payload = {
                    'sender': {
                        'name': self.config.from_name,
                        'email': self.config.from_email
                    },
                    'to': [{'email': email} for email in valid_recipients],
                    'subject': subject,
                    'htmlContent': html_content
                }
                
                response = requests.post(self.api_url, json=payload, headers=headers, timeout=30)
                
                if response.status_code == 201:
                    logger.info(f"Email sent successfully to {len(valid_recipients)} recipients")
                    return True
                else:
                    logger.error(f"Email send failed: {response.status_code} - {response.text}")
                    return False
                    
            except requests.RequestException as e:
                raise EmailError(f"Email request error: {e}", original_error=e)
            except Exception as e:
                raise EmailError(f"Unexpected email error: {e}", original_error=e)
    
    def format_activity_email(
        self, 
        activity_data: Dict[str, Any], 
        event_type: str
    ) -> Tuple[str, str]:
        """
        Format activity data into email subject and content.
        
        Args:
            activity_data: Activity data dictionary
            event_type: Type of event ('create' or 'update')
            
        Returns:
            Tuple of (subject, html_content)
        """
        emoji, title = self._get_event_emoji_and_title(event_type)
        
        subject = f"{emoji} {activity_data.get('name', 'Activity')}"
        
        html_content = f"""
        <html>
        <body style="font-family: Arial, sans-serif; color: #333; line-height: 1.6; max-width: 600px; margin: 0 auto;">
            <div style="background: linear-gradient(135deg, #FC4C02, #FF6B35); padding: 20px; border-radius: 8px 8px 0 0;">
                <h1 style="color: white; margin: 0; text-align: center;">{emoji} {title}</h1>
                <p style="color: white; margin: 10px 0 0 0; text-align: center; opacity: 0.9;">
                    {activity_data.get('athlete_name', 'Athlete')}
                </p>
            </div>
            
            <div style="background: white; padding: 30px; border-radius: 0 0 8px 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1);">
                <h2 style="color: #FC4C02; margin-top: 0; border-bottom: 2px solid #FC4C02; padding-bottom: 10px;">
                    📍 Activity Details
                </h2>
                
                <div style="background: #f8f9fa; padding: 20px; border-radius: 8px; margin: 20px 0;">
                    <table style="width: 100%; border-collapse: collapse;">
                        <tr>
                            <td style="padding: 8px 0; font-weight: bold; color: #555;">Activity:</td>
                            <td style="padding: 8px 0;">{activity_data.get('name', 'Unknown')}</td>
                        </tr>
                        <tr>
                            <td style="padding: 8px 0; font-weight: bold; color: #555;">Date:</td>
                            <td style="padding: 8px 0;">{self._format_date(activity_data.get('start_date_local'))}</td>
                        </tr>
                        <tr>
                            <td style="padding: 8px 0; font-weight: bold; color: #555;">Distance:</td>
                            <td style="padding: 8px 0;">{self._format_distance(activity_data.get('distance_km', 0) * 1000)}</td>
                        </tr>
                        <tr>
                            <td style="padding: 8px 0; font-weight: bold; color: #555;">Elevation:</td>
                            <td style="padding: 8px 0;">{self._format_elevation(activity_data.get('elevation_m', 0))}</td>
                        </tr>
                        <tr>
                            <td style="padding: 8px 0; font-weight: bold; color: #555;">Calories:</td>
                            <td style="padding: 8px 0;">{self._format_calories(activity_data.get('calories', 0))}</td>
                        </tr>
                    </table>
                </div>
                
                <div style="background: #e8f5e8; padding: 15px; border-radius: 8px; margin: 20px 0; text-align: center;">
                    <p style="margin: 0; color: #2e7d32; font-weight: bold;">
                        🎉 Great job on your {activity_data.get('sport_type', 'activity').lower()}!
                    </p>
                </div>
                
                <div style="text-align: center; margin-top: 30px; padding-top: 20px; border-top: 1px solid #eee;">
                    <p style="color: #666; font-size: 12px; margin: 0;">
                        <em>🤖 Strava Data Fetcher - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</em>
                    </p>
                </div>
            </div>
        </body>
        </html>
        """
        
        return subject, html_content
    
    def send_activity_notification(
        self,
        activity_data: Dict[str, Any],
        event_type: str,
        to_email: Union[str, List[str]]
    ) -> bool:
        """
        Send an activity notification email.
        
        Args:
            activity_data: Activity data dictionary
            event_type: Type of event ('create' or 'update')
            to_email: Recipient email address(es)
            
        Returns:
            True if sent successfully, False otherwise
        """
        with PerformanceTimer(f"Send activity notification ({event_type})"):
            if not activity_data:
                raise EmailError("No activity data provided for notification", email_type="activity_notification")
            
            try:
                subject, html_content = self.format_activity_email(activity_data, event_type)
                return self.send_email(to_email, subject, html_content)
            except Exception as e:
                if isinstance(e, EmailError):
                    raise
                raise EmailError(f"Error sending activity notification: {e}", email_type="activity_notification", original_error=e)
    
    def send_activity_email(self, activity, to_email: str) -> bool:
        """
        Send activity notification email for StravaActivity object.
        
        Args:
            activity: StravaActivity object
            to_email: Recipient email address
            
        Returns:
            True if sent successfully, False otherwise
        """
        try:
            # Convert StravaActivity to dict format expected by notification method
            activity_data = {
                'id': activity.id,
                'name': activity.name,
                'athlete_name': activity.athlete_name,
                'start_date_local': activity.start_date_local,
                'distance_km': activity.distance / 1000 if activity.distance else 0,
                'elevation_m': activity.total_elevation_gain or 0,
                'calories': activity.calories or 0,
                'sport_type': activity.sport_type or activity.type
            }
            
            return self.send_activity_notification(activity_data, 'create', to_email)
            
        except Exception as e:
            if isinstance(e, EmailError):
                raise
            raise EmailError(f"Error sending activity email: {e}", email_type="activity_email", original_error=e)
    
    def _format_summary_email(self, summary_data: Dict[str, Any]) -> Tuple[str, str]:
        """
        Format summary data into email subject and content.
        
        Args:
            summary_data: Summary statistics
            
        Returns:
            Tuple of (subject, html_content)
        """
        period = summary_data.get('period', 'Summary').title()
        subject = f"📊 {period} Summary"
        
        html_content = f"""
        <html>
        <body style="font-family: Arial, sans-serif; color: #333; line-height: 1.6; max-width: 600px; margin: 0 auto;">
            <div style="background: linear-gradient(135deg, #1976d2, #42a5f5); padding: 20px; border-radius: 8px 8px 0 0;">
                <h1 style="color: white; margin: 0; text-align: center;">📊 {period} Summary</h1>
                <p style="color: white; margin: 10px 0 0 0; text-align: center; opacity: 0.9;">
                    {summary_data.get('start_date', '')} - {summary_data.get('end_date', '')}
                </p>
            </div>
            
            <div style="background: white; padding: 30px; border-radius: 0 0 8px 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1);">
                <h2 style="color: #1976d2; margin-top: 0; border-bottom: 2px solid #1976d2; padding-bottom: 10px;">
                    🏆 Your Achievements
                </h2>
                
                <div style="background: #f8f9fa; padding: 20px; border-radius: 8px; margin: 20px 0;">
                    <table style="width: 100%; border-collapse: collapse;">
                        <tr>
                            <td style="padding: 8px 0; font-weight: bold; color: #555;">Total Activities:</td>
                            <td style="padding: 8px 0;">{summary_data.get('total_activities', 0)}</td>
                        </tr>
                        <tr>
                            <td style="padding: 8px 0; font-weight: bold; color: #555;">Total Distance:</td>
                            <td style="padding: 8px 0;">{summary_data.get('total_distance_km', 0):.1f} km</td>
                        </tr>
                        <tr>
                            <td style="padding: 8px 0; font-weight: bold; color: #555;">Total Elevation:</td>
                            <td style="padding: 8px 0;">{self._format_elevation(summary_data.get('total_elevation_m', 0))}</td>
                        </tr>
                        <tr>
                            <td style="padding: 8px 0; font-weight: bold; color: #555;">Total Calories:</td>
                            <td style="padding: 8px 0;">{self._format_calories(summary_data.get('total_calories', 0))}</td>
                        </tr>
                    </table>
                </div>
                
                <div style="background: #e3f2fd; padding: 15px; border-radius: 8px; margin: 20px 0; text-align: center;">
                    <p style="margin: 0; color: #1976d2; font-weight: bold;">
                        🎯 Keep up the great work!
                    </p>
                </div>
                
                <div style="text-align: center; margin-top: 30px; padding-top: 20px; border-top: 1px solid #eee;">
                    <p style="color: #666; font-size: 12px; margin: 0;">
                        <em>📊 {period} Summary - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</em>
                    </p>
                </div>
            </div>
        </body>
        </html>
        """
        
        return subject, html_content
    
    def send_summary_email(
        self,
        summary_data: Dict[str, Any],
        to_email: Union[str, List[str]]
    ) -> bool:
        """
        Send a summary statistics email.
        
        Args:
            summary_data: Summary statistics
            to_email: Recipient email address(es)
            
        Returns:
            True if sent successfully, False otherwise
        """
        with PerformanceTimer(f"Send summary email ({summary_data.get('period', 'unknown')})"):
            if not summary_data:
                raise EmailError("No summary data provided for email", email_type="summary_email")
            
            try:
                subject, html_content = self._format_summary_email(summary_data)
                return self.send_email(to_email, subject, html_content)
            except Exception as e:
                if isinstance(e, EmailError):
                    raise
                raise EmailError(f"Error sending summary email: {e}", email_type="summary_email", original_error=e)