#!/bin/bash

# Strava Data Fetcher Setup Script
# This script helps you set up the Strava data fetcher application

set -e

echo "🚴 Strava Data Fetcher Setup"
echo "=============================="

# Check if Python 3.8+ is available
echo "Checking Python version..."
python_version=$(python3 --version 2>&1 | grep -oP '\d+\.\d+' | head -1)
if [[ $(echo "$python_version >= 3.8" | bc -l) -eq 0 ]]; then
    echo "❌ Python 3.8 or higher is required. Current version: $python_version"
    exit 1
fi
echo "✅ Python $python_version detected"

# Create virtual environment
echo "Creating virtual environment..."
python3 -m venv venv
source venv/bin/activate
echo "✅ Virtual environment created and activated"

# Install requirements
echo "Installing Python packages..."
pip install --upgrade pip
pip install -r requirements.txt
echo "✅ Python packages installed"

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    echo "Creating .env file from template..."
    cp .env.template .env
    echo "✅ .env file created"
    echo "⚠️  Please edit .env file with your actual credentials"
else
    echo "✅ .env file already exists"
fi

# Create data directories
echo "Creating data directories..."
mkdir -p data logs
echo "✅ Data directories created"

# Set up database
echo "Initializing database..."
python3 -c "
from strava_fetcher import Config, DatabaseManager
config = Config.from_env()
db = DatabaseManager(config.db_path)
print('✅ Database initialized')
"

echo ""
echo "🎉 Setup complete!"
echo ""
echo "Next steps:"
echo "1. Edit the .env file with your Strava API credentials"
echo "2. Get your Strava API credentials from: https://www.strava.com/settings/api"
echo "3. Set up Gmail App Password: https://myaccount.google.com/apppasswords"
echo "4. Authenticate athletes:"
echo "   python3 strava_fetcher.py setup dominic"
echo "   python3 strava_fetcher.py setup clare"
echo "5. Test the setup:"
echo "   python3 strava_fetcher.py test"
echo "6. Run a full fetch:"
echo "   python3 strava_fetcher.py fetch"
echo ""
echo "Available commands:"
echo "  python3 strava_fetcher.py setup <athlete>  - Set up OAuth for athlete"
echo "  python3 strava_fetcher.py fetch            - Fetch all activities"
echo "  python3 strava_fetcher.py test             - Test fetch (max 5 activities)"
echo "  python3 strava_fetcher.py status           - Show current status"
echo ""
echo "For containerized deployment:"
echo "  docker-compose up -d"
echo ""
echo "For webhook setup, you'll need to:"
echo "1. Set up a webhook subscription with Strava"
echo "2. Point it to: https://your-domain.com/webhook"
echo "3. Set STRAVA_WEBHOOK_VERIFY_TOKEN in your .env"
