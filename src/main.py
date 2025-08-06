#!/usr/bin/env python3
"""
Main entry point for the Strava Data Fetcher application.

This module provides the CLI interface and coordinates all other components
using the modular architecture.
"""

import asyncio
import sys
from datetime import datetime, timedelta
from typing import Optional

import click

# Import our modular components
from .utils.config import Config
from .utils.logging_config import setup_logging, get_logger, PerformanceTimer
from .utils.error_handling import (
    StravaDataFetcherError, ConfigurationError, DatabaseError, APIError,
    EmailError, handle_errors, safe_execute_async
)
from .utils.cache import get_cache_manager
from .utils.security import get_security_auditor
from .api.strava_client import StravaAPIClient
from .database.manager import DatabaseManager
from .notifications.email_service import EmailService


class StravaDataFetcher:
    """Main application class that coordinates all components"""
    
    def __init__(self, config: Config):
        """
        Initialize the Strava Data Fetcher.
        
        Args:
            config: Application configuration
        """
        self.config = config
        self.logger = get_logger(__name__)
        
        # Initialize components
        self.db_manager = DatabaseManager(config.database)
        self.email_service = EmailService(config.email)
        self.cache_manager = get_cache_manager()
        
        self.logger.info("StravaDataFetcher initialized")
    
    async def fetch_activities_for_athlete(self, athlete_config, limit: Optional[int] = None) -> int:
        """
        Fetch activities for a specific athlete.
        
        Args:
            athlete_config: Athlete configuration
            limit: Maximum number of activities to fetch
            
        Returns:
            Number of activities fetched and saved
        
        Raises:
            APIError: If API operations fail
            DatabaseError: If database operations fail
        """
        with PerformanceTimer(f"Fetch activities for {athlete_config.name}"):
            try:
                self.logger.info(f"Fetching activities for {athlete_config.name} (limit: {limit})")
                
                async with StravaAPIClient(athlete_config) as client:
                    # Fetch and parse activities
                    activities = await client.fetch_and_parse_activities(limit=limit)
                    
                    if not activities:
                        self.logger.info(f"No activities found for {athlete_config.name}")
                        return 0
                    
                    # Save to database
                    affected_rows = self.db_manager.save_activities(activities)
                    
                    self.logger.info(f"Fetched {len(activities)} activities for {athlete_config.name}, "
                                   f"saved {affected_rows} rows")
                    
                    return len(activities)
                    
            except (APIError, DatabaseError) as e:
                self.logger.error(f"Error fetching activities for {athlete_config.name}: {e}")
                raise
            except Exception as e:
                self.logger.error(f"Unexpected error fetching activities for {athlete_config.name}: {e}")
                raise StravaDataFetcherError(f"Unexpected error: {e}", original_error=e)
    
    async def fetch_all_activities(self, limit: Optional[int] = None) -> dict:
        """
        Fetch activities for all configured athletes.
        
        Args:
            limit: Maximum number of activities to fetch per athlete
            
        Returns:
            Dictionary with results per athlete
        """
        results = {}
        
        for athlete_config in self.config.athletes:
            try:
                count = await self.fetch_activities_for_athlete(athlete_config, limit)
                results[athlete_config.name] = count
            except Exception as e:
                self.logger.error(f"Failed to fetch activities for {athlete_config.name}: {e}")
                results[athlete_config.name] = 0
        
        return results
    
    def get_activity_summary(self, 
                           athlete_name: Optional[str] = None,
                           days: Optional[int] = None) -> dict:
        """
        Get activity summary statistics.
        
        Args:
            athlete_name: Filter by athlete name
            days: Number of days to look back
            
        Returns:
            Activity summary data
        """
        try:
            start_date = None
            end_date = None
            
            if days:
                end_date = datetime.now().date()
                start_date = end_date - timedelta(days=days)
            
            summary = self.db_manager.get_activity_summary(
                athlete_name=athlete_name,
                start_date=start_date.isoformat() if start_date else None,
                end_date=end_date.isoformat() if end_date else None
            )
            
            self.logger.info(f"Retrieved activity summary for {athlete_name or 'all athletes'}")
            return summary
            
        except Exception as e:
            self.logger.error(f"Error getting activity summary: {e}")
            return {}
    
    def send_summary_email(self, period: str = "weekly") -> bool:
        """
        Send summary email to all configured athletes.
        
        Args:
            period: Summary period (weekly, monthly)
            
        Returns:
            True if successful
        """
        try:
            # Calculate date range based on period
            today = datetime.now().date()
            
            if period == "weekly":
                # Last 7 days
                start_date = today - timedelta(days=6)
                end_date = today
            elif period == "monthly":
                # Current month
                start_date = today.replace(day=1)
                end_date = today
            else:
                self.logger.error(f"Invalid period: {period}")
                return False
            
            # Get summary data
            summary_data = self.db_manager.get_activity_summary(
                start_date=start_date.isoformat(),
                end_date=end_date.isoformat()
            )
            
            if not summary_data:
                self.logger.info(f"No activity data for {period} summary")
                return True
            
            # Format summary data
            formatted_data = {
                "period": period,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                **summary_data
            }
            
            # Send to all configured athletes
            success_count = 0
            for athlete_config in self.config.athletes:
                if athlete_config.email:
                    try:
                        success = self.email_service.send_summary_email(
                            summary_data=formatted_data,
                            to_email=athlete_config.email
                        )
                        if success:
                            success_count += 1
                    except Exception as e:
                        self.logger.error(f"Failed to send summary email to {athlete_config.name}: {e}")
            
            self.logger.info(f"Sent {period} summary emails to {success_count} athletes")
            return success_count > 0
            
        except Exception as e:
            self.logger.error(f"Error sending {period} summary email: {e}")
            return False
    
    @handle_errors(default_return=False, log_errors=True)
    def test_database_connection(self) -> bool:
        """Test database connectivity"""
        try:
            result = self.db_manager.test_connection()
            if result:
                self.logger.info("Database connection test successful")
            else:
                self.logger.error("Database connection test failed")
            return result
        except DatabaseError as e:
            self.logger.error(f"Database connection test error: {e}")
            return False


# CLI Interface
@click.group()
@click.option('--log-level', default='INFO', 
              type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']),
              help='Set logging level')
@click.option('--config-file', help='Path to configuration file')
@click.pass_context
def cli(ctx, log_level, config_file):
    """Strava Data Fetcher - Modern modular application for fetching Strava activity data"""
    
    # Setup logging
    setup_logging(log_level=log_level)
    logger = get_logger(__name__)
    
    try:
        # Load configuration
        if config_file:
            config = Config.from_file(config_file)
        else:
            config = Config.from_env()
        
        config.validate()
        logger.info("Configuration loaded and validated successfully")
        
        # Store in context for subcommands
        ctx.ensure_object(dict)
        ctx.obj['config'] = config
        ctx.obj['fetcher'] = StravaDataFetcher(config)
        
    except Exception as e:
        logger.error(f"Initialization failed: {e}")
        sys.exit(1)


@cli.command()
@click.option('--athlete', help='Fetch activities for specific athlete')
@click.option('--limit', type=int, help='Maximum number of activities to fetch')
@click.pass_context
def fetch(ctx, athlete, limit):
    """Fetch activities from Strava API"""
    fetcher = ctx.obj['fetcher']
    logger = get_logger(__name__)
    
    async def run_fetch():
        try:
            if athlete:
                # Find athlete config
                athlete_config = ctx.obj['config'].get_athlete_by_name(athlete)
                if not athlete_config:
                    logger.error(f"Athlete '{athlete}' not found in configuration")
                    return
                
                count = await fetcher.fetch_activities_for_athlete(athlete_config, limit)
                click.echo(f"✅ Fetched {count} activities for {athlete}")
            else:
                results = await fetcher.fetch_all_activities(limit)
                total = sum(results.values())
                click.echo(f"✅ Fetched {total} total activities:")
                for name, count in results.items():
                    click.echo(f"  • {name}: {count} activities")
        
        except Exception as e:
            logger.error(f"Fetch command failed: {e}")
            click.echo(f"❌ Fetch failed: {e}")
            sys.exit(1)
    
    asyncio.run(run_fetch())


@cli.command()
@click.option('--athlete', help='Get summary for specific athlete')
@click.option('--days', type=int, help='Number of days to look back')
@click.pass_context
def summary(ctx, athlete, days):
    """Get activity summary statistics"""
    fetcher = ctx.obj['fetcher']
    
    try:
        summary_data = fetcher.get_activity_summary(athlete_name=athlete, days=days)
        
        if not summary_data:
            click.echo("No activity data found")
            return
        
        click.echo("📊 Activity Summary:")
        click.echo("=" * 50)
        
        for athlete_name, stats in summary_data.items():
            click.echo(f"\n🚴‍♂️ {athlete_name.title()}:")
            click.echo(f"  Activities: {stats.get('total_activities', 0)}")
            click.echo(f"  Distance: {stats.get('total_distance_km', 0):.1f} km")
            click.echo(f"  Elevation: {stats.get('total_elevation_m', 0):.0f} m")
            click.echo(f"  Calories: {stats.get('total_calories', 0):,.0f}")
    
    except Exception as e:
        click.echo(f"❌ Summary failed: {e}")
        sys.exit(1)


@cli.command()
@click.option('--period', type=click.Choice(['weekly', 'monthly']), 
              default='weekly', help='Summary period')
@click.pass_context
def email_summary(ctx, period):
    """Send summary email to configured athletes"""
    fetcher = ctx.obj['fetcher']
    
    try:
        success = fetcher.send_summary_email(period)
        if success:
            click.echo(f"✅ {period.title()} summary emails sent successfully")
        else:
            click.echo(f"❌ Failed to send {period} summary emails")
            sys.exit(1)
    
    except Exception as e:
        click.echo(f"❌ Email summary failed: {e}")
        sys.exit(1)


@cli.command()
@click.pass_context
def test_db(ctx):
    """Test database connection"""
    fetcher = ctx.obj['fetcher']
    
    try:
        success = fetcher.test_database_connection()
        if success:
            click.echo("✅ Database connection successful")
        else:
            click.echo("❌ Database connection failed")
            sys.exit(1)
    
    except Exception as e:
        click.echo(f"❌ Database test failed: {e}")
        sys.exit(1)


@cli.command()
@click.pass_context
def status(ctx):
    """Show application status and configuration"""
    config = ctx.obj['config']
    fetcher = ctx.obj['fetcher']
    
    click.echo("🚀 Strava Data Fetcher Status")
    click.echo("=" * 40)
    
    # Configuration status
    click.echo(f"\n📋 Configuration:")
    click.echo(f"  Athletes: {len(config.athletes)}")
    for athlete in config.athletes:
        click.echo(f"    • {athlete.name} (ID: {athlete.athlete_id})")
    
    click.echo(f"  Database: {config.database.host}:{config.database.port}/{config.database.database}")
    click.echo(f"  Email: {'✅ Configured' if config.email.api_key else '❌ Not configured'}")
    
    # Database status
    try:
        db_status = fetcher.test_database_connection()
        click.echo(f"  Database Connection: {'✅ Connected' if db_status else '❌ Failed'}")
    except Exception as e:
        click.echo(f"  Database Connection: ❌ Error - {e}")
    
    # Recent activity count
    try:
        recent_activities = fetcher.db_manager.get_recent_activities(limit=10)
        click.echo(f"  Recent Activities: {len(recent_activities)}")
    except Exception as e:
        click.echo(f"  Recent Activities: ❌ Error - {e}")

@cli.command()
@click.option('--output', '-o', help='Output file for security report')
@click.pass_context
def security_audit(ctx, output):
    """Run comprehensive security audit"""
    try:
        security_auditor = get_security_auditor()
        
        click.echo("🔒 Running Security Audit...")
        click.echo("=" * 40)
        
        # Generate security report
        report = security_auditor.generate_security_report()
        
        if output:
            # Write to file
            with open(output, 'w') as f:
                f.write(report)
            click.echo(f"✅ Security report written to: {output}")
        else:
            # Display to console
            click.echo(report)
        
        # Quick summary
        env_findings = security_auditor.audit_environment()
        total_issues = sum(len(issues) for issues in env_findings.values())
        
        if total_issues == 0:
            click.echo("✅ No security issues found!")
        else:
            click.echo(f"⚠️  Found {total_issues} security issues. Review the report for details.")
            
            # Show critical issues immediately
            critical_issues = env_findings.get('critical', [])
            if critical_issues:
                click.echo("\n🚨 CRITICAL ISSUES:")
                for issue in critical_issues:
                    click.echo(f"  • {issue}")
    
    except Exception as e:
        click.echo(f"❌ Security audit failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    cli()