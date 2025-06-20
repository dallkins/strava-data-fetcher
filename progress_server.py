from flask import Flask, jsonify, render_template_string
import json
import os
from datetime import datetime

app = Flask(__name__)

@app.route('/progress')
def progress_page():
    progress_file = "/home/dom1n1c4/projects/strava_data/progress.json"
    
    if os.path.exists(progress_file):
        with open(progress_file, 'r') as f:
            data = json.load(f)
    else:
        data = {'status': 'not_started'}
    
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>🚴‍♂️ Strava Fetch Progress</title>
        <meta http-equiv="refresh" content="30">
        <style>
            body { font-family: Arial; margin: 40px; }
            .progress-bar { width: 100%; background: #f0f0f0; border-radius: 10px; margin: 20px 0; }
            .progress-fill { height: 30px; background: #4CAF50; border-radius: 10px; text-align: center; line-height: 30px; color: white; transition: width 0.3s; }
            table { width: 100%; border-collapse: collapse; margin-top: 20px; }
            th, td { border: 1px solid #ddd; padding: 12px; text-align: left; }
            th { background-color: #f2f2f2; }
            .status { padding: 10px; border-radius: 5px; margin: 10px 0; }
            .running { background-color: #e8f5e8; }
            .complete { background-color: #d4edda; }
        </style>
    </head>
    <body>
        <h1>🚴‍♂️ Strava Historical Data Fetch Progress</h1>
        
        {% if data.status == 'running' %}
            <div class="status running">
                <strong>Status:</strong> Fetching historical data...
            </div>
            
            <div class="progress-bar">
                <div class="progress-fill" style="width: {{ data.percentage }}%">
                    {{ "%.1f"|format(data.percentage) }}%
                </div>
            </div>
            
            <table>
                <tr><th>Field</th><th>Value</th></tr>
                <tr><td>Athlete</td><td>{{ data.athlete.title() }}</td></tr>
                <tr><td>Progress</td><td>{{ data.processed }} of {{ data.total_estimated }} activities</td></tr>
                <tr><td>Completion</td><td>{{ "%.1f"|format(data.percentage) }}%</td></tr>
                <tr><td>Latest Activity</td><td>{{ data.latest_activity }}</td></tr>
                <tr><td>Activity Date</td><td>{{ data.latest_date }}</td></tr>
                <tr><td>Last Updated</td><td>{{ data.timestamp }}</td></tr>
            </table>
            
            <p><em>Page auto-refreshes every 30 seconds</em></p>
            
        {% elif data.status == 'complete' %}
            <div class="status complete">
                <strong>Status:</strong> Fetch completed! 🎉
            </div>
            <table>
                <tr><th>Field</th><th>Value</th></tr>
                <tr><td>Total Activities</td><td>{{ data.total_processed }}</td></tr>
                <tr><td>Completed At</td><td>{{ data.timestamp }}</td></tr>
            </table>
            
        {% else %}
            <div class="status">
                <strong>Status:</strong> No fetch currently running
            </div>
            <p>Start a fetch to see progress here!</p>
        {% endif %}
    </body>
    </html>
    """
    
    return render_template_string(html, data=data)

@app.route('/api/progress')
def progress_api():
    """API endpoint for progress data"""
    progress_file = "/home/dom1n1c4/projects/strava_data/progress.json"
    
    if os.path.exists(progress_file):
        with open(progress_file, 'r') as f:
            return jsonify(json.load(f))
    else:
        return jsonify({'status': 'not_started'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=False)