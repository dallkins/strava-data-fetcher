# Strava Data Fetcher

A modern, modular Python application for fetching, storing, and managing Strava activity data with real-time webhook processing and email notifications.

## 🚀 Features

- **Modular Architecture**: Clean separation of concerns with dedicated modules for API, database, email, and configuration management
- **Async API Client**: High-performance async Strava API integration with proper rate limiting
- **Real-time Webhooks**: Flask-based webhook server for real-time activity processing
- **Email Notifications**: Beautiful HTML email notifications for new activities and summaries
- **Comprehensive Logging**: Structured logging with file rotation and performance monitoring
- **Robust Error Handling**: Custom exception hierarchy with detailed error context
- **Extensive Testing**: 92+ unit tests and integration tests for reliable operation
- **CLI Interface**: User-friendly command-line interface with multiple commands

## 📋 Table of Contents

- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Architecture](#architecture)
- [API Reference](#api-reference)
- [Testing](#testing)
- [Development](#development)
- [Contributing](#contributing)
- [License](#license)

## 🛠 Installation

### Prerequisites

- Python 3.8+
- MySQL/MariaDB database
- Strava API application (for API keys)
- Brevo account (for email notifications)

### Quick Start

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/strava-data-fetcher.git
   cd strava-data-fetcher
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

4. **Initialize the database**
   ```bash
   # Run the database migration script
   python scripts/migrate_database.py
   ```

5. **Test the installation**
   ```bash
   python -m src.main status
   ```

## ⚙️ Configuration

The application supports configuration via environment variables or JSON files.

### Environment Variables

```bash
# Athlete Configuration (Dominic)
DOMINIC_NAME="Dominic"
DOMINIC_ATHLETE_ID=12345
DOMINIC_ACCESS_TOKEN="your_access_token"
DOMINIC_REFRESH_TOKEN="your_refresh_token"
DOMINIC_TOKEN_EXPIRES=1234567890
DOMINIC_EMAIL="dominic@example.com"

# Athlete Configuration (Clare)
CLARE_NAME="Clare"
CLARE_ATHLETE_ID=67890
CLARE_ACCESS_TOKEN="clare_access_token"
CLARE_REFRESH_TOKEN="clare_refresh_token"
CLARE_TOKEN_EXPIRES=1234567890
CLARE_EMAIL="clare@example.com"

# Database Configuration
DB_HOST="localhost"
DB_PORT=3306
DB_USER="strava_user"
DB_PASSWORD="your_password"
DB_NAME="strava_data"

# Email Configuration (Brevo)
BREVO_API_KEY="your_brevo_api_key"
BREVO_FROM_EMAIL="noreply@yourdomain.com"
BREVO_FROM_NAME="Strava Data Fetcher"

# Webhook Configuration
STRAVA_WEBHOOK_VERIFY_TOKEN="your_webhook_verify_token"
```

### JSON Configuration

Alternatively, create a `config.json` file:

```json
{
  "athletes": [
    {
      "name": "Dominic",
      "athlete_id": 12345,
      "access_token": "your_access_token",
      "refresh_token": "your_refresh_token",
      "expires_at": 1234567890,
      "email": "dominic@example.com"
    }
  ],
  "database": {
    "host": "localhost",
    "port": 3306,
    "user": "strava_user",
    "password": "your_password",
    "database": "strava_data"
  },
  "email": {
    "api_key": "your_brevo_api_key",
    "from_email": "noreply@yourdomain.com",
    "from_name": "Strava Data Fetcher"
  }
}
```

## 🎯 Usage

### Command Line Interface

The application provides a comprehensive CLI with multiple commands:

```bash
# Show application status
python -m src.main status

# Fetch activities for all athletes
python -m src.main fetch

# Fetch activities for a specific athlete
python -m src.main fetch --athlete "Dominic" --limit 10

# Get activity summary
python -m src.main summary --days 30

# Send weekly summary email
python -m src.main email-summary --period weekly

# Test database connection
python -m src.main test-db
```

### Webhook Server

Start the webhook server to process real-time Strava events:

```bash
# Start webhook server
python -m src.webhook_server --host 0.0.0.0 --port 5000

# Start with debug mode
python -m src.webhook_server --debug --log-level DEBUG
```

### Programmatic Usage

```python
from src.main import StravaDataFetcher
from src.utils.config import Config

# Load configuration
config = Config.from_env()

# Create fetcher instance
fetcher = StravaDataFetcher(config)

# Fetch activities
results = await fetcher.fetch_all_activities(limit=50)

# Get summary
summary = fetcher.get_activity_summary(days=7)

# Send summary email
success = fetcher.send_summary_email("weekly")
```

## 🏗 Architecture

The application follows a modular architecture with clear separation of concerns:

```
src/
├── main.py                 # Main CLI application
├── webhook_server.py       # Webhook server
├── api/                    # Strava API integration
│   ├── strava_client.py    # Async API client
│   └── models.py           # Data models
├── database/               # Database operations
│   └── manager.py          # Database manager
├── notifications/          # Email notifications
│   └── email_service.py    # Email service
└── utils/                  # Utilities
    ├── config.py           # Configuration management
    ├── logging_config.py   # Logging setup
    └── error_handling.py   # Error handling
```

### Key Components

#### 1. **Strava API Client** (`src/api/strava_client.py`)
- Async HTTP client with aiohttp
- Rate limiting (100 requests/15 minutes)
- Token refresh handling
- Comprehensive error handling

#### 2. **Database Manager** (`src/database/manager.py`)
- MySQL/MariaDB integration
- Connection pooling and context managers
- CRUD operations for activities
- Summary statistics generation

#### 3. **Email Service** (`src/notifications/email_service.py`)
- Brevo API integration
- HTML email templates
- Activity and summary notifications
- Email validation and error handling

#### 4. **Configuration Management** (`src/utils/config.py`)
- Environment variable loading
- JSON file support
- Comprehensive validation
- Type-safe dataclasses

#### 5. **Error Handling** (`src/utils/error_handling.py`)
- Custom exception hierarchy
- Error severity levels
- Decorators for safe execution
- Error collection and reporting

#### 6. **Logging** (`src/utils/logging_config.py`)
- Structured logging with rotation
- Performance monitoring
- Separate log files by component
- Configurable log levels

## 📊 API Reference

### Main Application

```python
class StravaDataFetcher:
    async def fetch_activities_for_athlete(athlete_config, limit=None) -> int
    async def fetch_all_activities(limit=None) -> dict
    def get_activity_summary(athlete_name=None, days=None) -> dict
    def send_summary_email(period="weekly") -> bool
    def test_database_connection() -> bool
```

### Strava API Client

```python
class StravaAPIClient:
    async def fetch_and_parse_activities(limit=None) -> List[StravaActivity]
    async def fetch_activity_by_id(activity_id) -> Optional[StravaActivity]
    async def get_activities(per_page=50, page=1) -> Optional[List[Dict]]
    async def refresh_token(client_id, client_secret) -> Optional[Dict]
```

### Database Manager

```python
class DatabaseManager:
    def save_activities(activities: List[StravaActivity]) -> int
    def get_activity_summary(athlete_name=None, start_date=None, end_date=None) -> dict
    def get_recent_activities(limit=10, athlete_name=None) -> List[dict]
    def activity_exists(activity_id: int) -> bool
    def delete_activity(activity_id: int) -> bool
```

### Email Service

```python
class EmailService:
    def send_activity_notification(activity_data, event_type, to_email) -> bool
    def send_summary_email(summary_data, to_email) -> bool
    def send_email(to_email, subject, html_content) -> bool
```

## 🧪 Testing

The application includes comprehensive test coverage with both unit and integration tests.

### Running Tests

```bash
# Run all tests
pytest

# Run only unit tests
pytest tests/unit/

# Run only integration tests
pytest tests/integration/

# Run with coverage
pytest --cov=src --cov-report=html

# Run specific test file
pytest tests/unit/test_strava_client.py

# Run tests with specific markers
pytest -m "not slow"
pytest -m "integration"
```

### Test Structure

```
tests/
├── unit/                   # Unit tests (92 tests)
│   ├── test_config.py      # Configuration tests
│   ├── test_models.py      # Data model tests
│   ├── test_strava_client.py # API client tests
│   ├── test_database_manager.py # Database tests
│   ├── test_email_service.py # Email service tests
│   └── test_error_handling.py # Error handling tests
├── integration/            # Integration tests
│   ├── test_api_integration.py # End-to-end API tests
│   └── conftest.py         # Test configuration
└── fixtures/               # Test data fixtures
```

### Test Coverage

Current test coverage: **88/92 tests passing** (96% success rate)

- Unit tests: 88 passing, 4 failing (async mocking issues)
- Integration tests: Comprehensive component interaction testing
- Error handling: Complete exception path coverage

## 🔧 Development

### Setting Up Development Environment

1. **Install development dependencies**
   ```bash
   pip install -r requirements-dev.txt
   ```

2. **Set up pre-commit hooks**
   ```bash
   pre-commit install
   ```

3. **Run code quality checks**
   ```bash
   # Format code
   black src/ tests/
   
   # Sort imports
   isort src/ tests/
   
   # Lint code
   flake8 src/ tests/
   
   # Type checking
   mypy src/
   ```

### Project Structure

```
strava-data-fetcher/
├── src/                    # Source code
├── tests/                  # Test suite
├── docs/                   # Documentation
├── scripts/                # Utility scripts
├── config/                 # Configuration files
├── requirements.txt        # Production dependencies
├── requirements-dev.txt    # Development dependencies
├── pytest.ini            # Test configuration
├── .env.example           # Environment template
└── README.md              # This file
```

### Adding New Features

1. **Create feature branch**
   ```bash
   git checkout -b feature/new-feature
   ```

2. **Implement feature with tests**
   - Add unit tests in `tests/unit/`
   - Add integration tests in `tests/integration/`
   - Update documentation

3. **Run test suite**
   ```bash
   pytest
   ```

4. **Submit pull request**

### Database Schema

The application uses the following main table:

```sql
CREATE TABLE strava_activities (
    id BIGINT PRIMARY KEY,
    athlete_id INT NOT NULL,
    athlete_name VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL,
    start_date_local DATETIME NOT NULL,
    start_date DATETIME NOT NULL,
    utc_offset FLOAT,
    gear_id VARCHAR(50),
    gear_name VARCHAR(255),
    distance FLOAT,
    elapsed_time INT,
    moving_time INT,
    calories FLOAT,
    average_heartrate FLOAT,
    max_heartrate FLOAT,
    average_watts FLOAT,
    max_watts FLOAT,
    average_speed FLOAT,
    max_speed FLOAT,
    type VARCHAR(50),
    sport_type VARCHAR(50),
    total_elevation_gain FLOAT,
    kudos_count INT,
    weighted_average_watts FLOAT,
    average_cadence FLOAT,
    trainer BOOLEAN,
    map_polyline TEXT,
    device_name VARCHAR(255),
    timezone VARCHAR(100),
    start_latlng JSON,
    end_latlng JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_athlete_id (athlete_id),
    INDEX idx_start_date (start_date_local),
    INDEX idx_type (type)
);
```

## 🔒 Security Considerations

- **API Keys**: Store securely in environment variables
- **Database**: Use dedicated user with minimal privileges
- **Webhooks**: Verify webhook signatures (implement as needed)
- **Logging**: Avoid logging sensitive data
- **Error Handling**: Don't expose internal details in error messages

## 📈 Performance

- **Async Operations**: Non-blocking API calls and database operations
- **Rate Limiting**: Respects Strava API limits (100 requests/15 minutes)
- **Connection Pooling**: Efficient database connection management
- **Logging**: Minimal performance impact with structured logging
- **Caching**: Consider implementing Redis for frequently accessed data

## 🐛 Troubleshooting

### Common Issues

1. **Database Connection Errors**
   ```bash
   # Test database connectivity
   python -m src.main test-db
   ```

2. **API Rate Limiting**
   - Check logs for rate limit messages
   - Reduce fetch frequency
   - Monitor API usage

3. **Email Delivery Issues**
   - Verify Brevo API key
   - Check email configuration
   - Review email service logs

4. **Webhook Processing**
   - Verify webhook URL is accessible
   - Check webhook verification token
   - Monitor webhook logs

### Logging

Logs are written to multiple files:
- `logs/strava_fetcher.log` - Main application logs
- `logs/strava_fetcher_errors.log` - Error logs only
- `logs/webhook.log` - Webhook processing logs
- `logs/performance.log` - Performance metrics

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes with tests
4. Ensure all tests pass
5. Submit a pull request

Please read our [Contributing Guidelines](CONTRIBUTING.md) for more details.

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- [Strava API](https://developers.strava.com/) for providing the data platform
- [Brevo](https://www.brevo.com/) for email delivery services
- The Python community for excellent libraries and tools

## 📞 Support

For support, please:
1. Check the [troubleshooting section](#troubleshooting)
2. Review the [documentation](docs/)
3. Open an issue on GitHub
4. Contact the maintainers

---

**Built with ❤️ for the Strava community**