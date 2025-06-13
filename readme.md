# Strava Data Fetcher

A comprehensive solution for fetching, storing, and managing Strava ride data for multiple athletes. This application handles OAuth authentication, respects API rate limits, provides error handling, and includes automated email notifications.

## Features

- **Multi-athlete support** - Fetch data for both Dominic and Clare
- **Comprehensive data collection** - All requested Strava activity fields
- **Dual storage** - SQLite database + CSV export
- **Rate limiting** - Respects Strava API limits
- **Error handling** - Graceful failure and recovery
- **Email notifications** - Daily summaries and error alerts
- **Webhook support** - Real-time activity updates
- **Test mode** - Safe development with limited API calls
- **Containerized** - Docker support for easy deployment

## Quick Start

### 1. Setup

```bash
# Clone and setup
chmod +x setup.sh
./setup.sh

# Edit configuration
nano .env
```

### 2. Get Strava API Credentials

1. Go to https://www.strava.com/settings/api
2. Create an application
3. Note your Client ID and Client Secret
4. Add them to your `.env` file

### 3. Setup Gmail Notifications

1. Go to https://myaccount.google.com/apppasswords
2. Generate an app password for "Mail"
3. Add it to your `.env` file as `GMAIL_APP_PASSWORD`

### 4. Authenticate Athletes

```bash
# Authenticate Dominic
python3 strava_fetcher.py setup dominic

# Authenticate Clare
python3 strava_fetcher.py setup clare
```

### 5. Test the Setup

```bash
# Test mode (max 5 activities)
python3 strava_fetcher.py test

# Check status
python3 strava_fetcher.py status
```

### 6. Run Full Fetch

```bash
# Fetch all activities
python3 strava_fetcher.py fetch
```

## Commands

| Command | Description |
|---------|-------------|
| `python3 strava_fetcher.py setup <athlete>` | Set up OAuth for athlete |
| `python3 strava_fetcher.py fetch` | Fetch all activities |
| `python3 strava_fetcher.py test` | Test fetch (max 5 activities) |
| `python3 strava_fetcher.py status` | Show current status |

## Configuration

### .env File

```bash
# Strava API
STRAVA_CLIENT_ID=your_client_id
STRAVA_CLIENT_SECRET=your_client_secret

# Gmail notifications
GMAIL_APP_PASSWORD=your_app_password

# Test mode
TEST_MODE=false

# OAuth tokens (generated during setup)
DOMINIC_ACCESS_TOKEN=...
DOMINIC_REFRESH_TOKEN=...
DOMINIC_TOKEN_EXPIRES=...

CLARE_ACCESS_TOKEN=...
CLARE_REFRESH_TOKEN=...
CLARE_TOKEN_EXPIRES=...
```

### Rate Limiting

The application respects Strava's API limits:
- 100 requests per 15 minutes
- 1,000 requests per day

### Test Mode

Set `TEST_MODE=true` to limit API calls during development (max 5 activities per athlete).

## Data Fields

The following fields are collected for each activity:

- `id` - Strava activity ID
- `name` - Activity name
- `start_date_local` - Local start time
- `start_date` - UTC start time
- `utc_offset` - Timezone offset
- `gear_id` - Equipment ID
- `gear_name` - Equipment name
- `distance` - Distance in meters
- `elapsed_time` - Total time in seconds
- `moving_time` - Moving time in seconds
- `calories` - Calories burned
- `average_heartrate` - Average heart rate
- `max_heartrate` - Maximum heart rate
- `average_watts` - Average power
- `max_watts` - Maximum power
- `average_speed` - Average speed
- `max_speed` - Maximum speed
- `type` - Activity type
- `sport_type` - Sport type
- `total_elevation_gain` - Elevation gain
- `kudos_count` - Number of kudos
- `weighted_average_watts` - Normalized power
- `average_cadence` - Average cadence
- `trainer` - Indoor trainer flag
- `map` - Route polyline
- `device_name` - Recording device
- `timezone` - Timezone
- `start_latlng` - Start coordinates
- `end_latlng` - End coordinates

## Storage

### Database

Activities are stored in SQLite (`strava_data.db`) with:
- Duplicate detection (by activity ID and athlete)
- Automatic timestamps
- Efficient indexing

### CSV Export

Activities are also exported to CSV (`strava_rides.csv`) for:
- Excel analysis
- Data backup
- Easy sharing

## Email Notifications

### Daily Summary

Sent to `dominic@allkins.com` at 23:00 with:
- New activities count
- Total statistics
- Per-athlete breakdown
- CSV download link

### Error Notifications

Immediate emails for:
- Authentication failures
- API errors
- System errors

## Webhook Support

For real-time updates:

1. **Setup webhook** with Strava pointing to `https://your-domain.com/webhook`
2. **Set verify token** in `.env` as `STRAVA_WEBHOOK_VERIFY_TOKEN`
3. **Run webhook server**:
   ```bash
   python3 webhook_server.py
   ```

## Docker Deployment

### Development

```bash
# Build and run
docker-compose up -d

# View logs
docker-compose logs -f

# Stop
docker-compose down
```

### Production

```bash
# Build for production
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

The Docker setup includes:
- **Main service** - Webhook server
- **Scheduler** - Automated fetches
- **Persistent storage** - Data and logs
- **Health checks** - Monitoring

## Architecture

```
┌─────────────────┐    ┌──────────────┐    ┌─────────────┐
│   Strava API    │───▶│   Application│───▶│  Database   │
└─────────────────┘    └──────────────┘    └─────────────┘
                              │                    │
                              ▼                    ▼
                       ┌──────────────┐    ┌─────────────┐
                       │ Email Alerts │    │ CSV Export  │
                       └──────────────┘    └─────────────┘
```

### Components

- **StravaAPI** - Handles authentication and API calls
- **DatabaseManager** - SQLite operations
- **CSVExporter** - CSV file management
- **EmailNotifier** - Gmail notifications
- **WebhookServer** - Real-time updates
- **Scheduler** - Automated tasks

## Monitoring

### Health Checks

```bash
# Application status
curl http://localhost:5000/health

# Database status
python3 strava_fetcher.py status
```

### Logs

- `strava_fetcher.log` - Main application
- `webhook.log` - Webhook events
- `scheduler.log` - Scheduled tasks

## Development

### Adding New Athletes

1. Add to `Config.athletes` dictionary
2. Add environment variables to `.env`
3. Run setup: `python3 strava_fetcher.py setup new_athlete`

### Custom Fields

Modify `StravaActivity` dataclass and update:
- Database schema in `DatabaseManager.init_database()`
- CSV headers in `CSVExporter`
- Parsing logic in `parse_strava_activity()`

### Testing

```bash
# Test mode
TEST_MODE=true python3 strava_fetcher.py fetch

# Manual webhook test
curl -X POST http://localhost:5000/trigger_fetch/dominic
```

## Troubleshooting

### Authentication Issues

```bash
# Check token status
python3 strava_fetcher.py status

# Re-authenticate
python3 strava_fetcher.py setup dominic
```

**Common issues:**
- Expired tokens (automatically refreshed)
- Invalid client credentials
- Scope permissions (need `activity:read_all`)

### API Rate Limiting

**Symptoms:**
- 429 errors in logs
- Slow data fetching

**Solutions:**
- Application automatically handles rate limits
- In test mode, only 5 activities fetched
- Spread fetches across day using scheduler

### Database Issues

```bash
# Check database
sqlite3 strava_data.db ".tables"
sqlite3 strava_data.db "SELECT COUNT(*) FROM strava_activities;"

# Reset database (caution: deletes all data)
rm strava_data.db
python3 strava_fetcher.py status  # Recreates DB
```

### Email Issues

**Gmail authentication:**
- Use App Password, not regular password
- Enable 2FA first
- Check spam folder

**SMTP errors:**
- Verify `GMAIL_APP_PASSWORD` in `.env`
- Check firewall/network restrictions

### Webhook Issues

**Setup verification:**
```bash
# Test webhook endpoint
curl http://localhost:5000/health

# Manual trigger
curl -X POST http://localhost:5000/trigger_fetch/dominic
```

**Common problems:**
- Incorrect verify token
- Firewall blocking port 5000
- SSL certificate issues (use HTTPS in production)

### Performance

**Large datasets:**
- CSV files can become large over time
- Consider periodic archiving
- Database remains performant with indexes

**Memory usage:**
- Application processes activities in batches
- Rate limiting prevents memory spikes

## Production Deployment

### Server Setup

1. **Droplet configuration:**
   ```bash
   # Update system
   sudo apt update && sudo apt upgrade -y
   
   # Install Docker
   curl -fsSL https://get.docker.com -o get-docker.sh
   sh get-docker.sh
   sudo usermod -aG docker $USER
   
   # Install Docker Compose
   sudo curl -L "https://github.com/docker/compose/releases/download/v2.20.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
   sudo chmod +x /usr/local/bin/docker-compose
   ```

2. **Application deployment:**
   ```bash
   # Clone repository
   git clone <your-repo>
   cd strava-data-fetcher
   
   # Setup environment
   cp .env.template .env
   nano .env  # Add your credentials
   
   # Deploy
   docker-compose up -d
   ```

3. **SSL/HTTPS setup** (for webhooks):
   ```bash
   # Using Let's Encrypt with nginx
   sudo apt install nginx certbot python3-certbot-nginx
   sudo certbot --nginx -d your-domain.com
   ```

### Backup Strategy

```bash
# Backup script
#!/bin/bash
DATE=$(date +%Y%m%d_%H%M%S)
tar -czf backup_$DATE.tar.gz data/ logs/ .env
aws s3 cp backup_$DATE.tar.gz s3://your-backup-bucket/
```

### Monitoring

**System monitoring:**
```bash
# Resource usage
docker stats

# Application logs
docker-compose logs -f --tail=100
```

**Automated monitoring:**
- Set up log aggregation (ELK stack)
- Monitor email delivery
- Track API usage

## API Usage Patterns

### Initial Setup
- First run fetches all historical activities
- Can take time for active athletes
- Respects rate limits automatically

### Ongoing Operation
- Webhook provides real-time updates
- Scheduled fetches catch missed activities
- Daily emails provide summaries

### Data Analysis Considerations

**Raw vs. Processed Data:**
- Raw Strava data stored as-is
- Frontend should handle formatting/conversion
- Enables flexible aggregation and filtering

**Performance Optimization:**
- Database indexes on date fields
- Pagination for large datasets
- Consider data archiving for old activities

## Security Considerations

### Credentials Management
- Never commit `.env` file
- Use app passwords for Gmail
- Rotate tokens periodically

### API Security
- Webhook verify tokens
- HTTPS for production webhooks
- Rate limiting protection

### Data Privacy
- Store only necessary fields
- Consider GDPR compliance
- Secure backup practices

## Extending the Application

### Adding New Data Sources
1. Create new API client class
2. Extend database schema
3. Update data processing pipeline
4. Add to scheduler

### Custom Analytics
```python
# Example: Custom metrics calculation
def calculate_fitness_metrics(athlete_name):
    conn = sqlite3.connect('strava_data.db')
    # Your custom analysis here
    return metrics
```

### Integration with Other Tools
- Power BI/Tableau connectors
- Jupyter notebook analysis
- REST API for external access

## Support

### Getting Help
- Check logs first: `docker-compose logs`
- Review this README
- Test in TEST_MODE first

### Reporting Issues
Include:
- Error messages from logs
- Configuration (without secrets)
- Steps to reproduce
- Expected vs actual behavior

### Contributing
1. Fork repository
2. Create feature branch
3. Add tests for new functionality
4. Submit pull request

## License

[Your chosen license]

---

**Created for Dominic & Clare's Strava data analysis needs** 🚴‍♂️🚴‍♀️