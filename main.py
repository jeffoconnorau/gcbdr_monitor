import os
import logging
from flask import Flask, request, jsonify
from analyzer import analyze_backup_jobs

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import io
import csv

def format_csv(results):
    """
    Formats the results as a CSV string.
    """
    output = io.StringIO()
    writer = csv.writer(output)
    
    # Header
    writer.writerow(['Resource Name', 'Type', 'Source', 'Total Size (GiB)', 'Daily Change (GB)', 'Daily Change (%)', 'Job Count'])
    
    # Data
    # Combine vault and appliance stats
    all_stats = results.get('vault_workloads', {}).get('resource_stats', []) + \
                results.get('appliance_workloads', {}).get('resource_stats', [])
                
    for r in all_stats:
        writer.writerow([
            r.get('resource_name'),
            r.get('resource_type'),
            r.get('job_source'),
            r.get('total_resource_size_gb'),
            r.get('current_daily_change_gb'),
            r.get('current_daily_change_pct'),
            r.get('backup_job_count')
        ])
    
    # Anomalies Section
    anomalies = results.get('anomalies', [])
    if anomalies:
        writer.writerow([])
        writer.writerow(['ANOMALIES DETECTED'])
        writer.writerow(['Job ID', 'Resource', 'Date', 'Time', 'Change (GB)', 'Avg (GB)', 'Duration (s)', 'Avg Duration (s)', 'Reasons'])
        for a in anomalies:
            writer.writerow([
                a.get('job_id'),
                a.get('resource'),
                a.get('date'),
                a.get('time'),
                a.get('gib_transferred'),
                a.get('avg_gib'),
                a.get('duration_seconds'),
                f"{a.get('avg_duration_seconds', 0):.1f}",
                a.get('reasons')
            ])
        
    return output.getvalue()

def format_html(results):
    """
    Formats the results as a simple HTML page.
    """
    summary = results.get('summary', {})
    vault_stats = results.get('vault_workloads', {}).get('resource_stats', [])
    appliance_stats = results.get('appliance_workloads', {}).get('resource_stats', [])
    all_stats = vault_stats + appliance_stats
    anomalies = results.get('anomalies', [])
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>GCBDR Monitor Report</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            h1 {{ color: #333; }}
            .summary {{ background-color: #f5f5f5; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
            table {{ border-collapse: collapse; width: 100%; margin-bottom: 20px; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background-color: #4CAF50; color: white; }}
            tr:nth-child(even) {{ background-color: #f2f2f2; }}
            .anomaly-table th {{ background-color: #d32f2f; }}
            .anomaly-row {{ background-color: #ffebee !important; }}
        </style>
    </head>
    <body>
        <h1>GCBDR Monitor Report</h1>
        
        <div class="summary">
            <h2>Summary</h2>
            <p><strong>Total Jobs:</strong> {summary.get('total_jobs')}</p>
            <p><strong>Successful:</strong> {summary.get('successful_jobs')}</p>
            <p><strong>Failed:</strong> {summary.get('failed_jobs')}</p>
            <p><strong>Anomalies:</strong> {summary.get('anomalies_count')}</p>
            <p><strong>Total Size:</strong> {summary.get('total_resource_size_gb')} GiB</p>
            <p><strong>Total Daily Change:</strong> {summary.get('current_daily_change_gb')} GB ({summary.get('current_daily_change_pct')}%)</p>
        </div>
    """
    
    if anomalies:
        html += """
        <h2>Anomalies Detected</h2>
        <table class="anomaly-table">
            <tr>
                <th>Job ID</th>
                <th>Resource</th>
                <th>Date</th>
                <th>Change (GB)</th>
                <th>Avg (GB)</th>
                <th>Duration (s)</th>
                <th>Reasons</th>
            </tr>
        """
        for a in anomalies:
            html += f"""
            <tr class="anomaly-row">
                <td>{a.get('job_id')}</td>
                <td>{a.get('resource')}</td>
                <td>{a.get('date')} {a.get('time')}</td>
                <td>{a.get('gib_transferred')}</td>
                <td>{a.get('avg_gib')}</td>
                <td>{a.get('duration_seconds')}</td>
                <td>{a.get('reasons')}</td>
            </tr>
            """
        html += "</table>"
        
    html += """
        <h2>Resources</h2>
        <table>
            <tr>
                <th>Resource Name</th>
                <th>Type</th>
                <th>Source</th>
                <th>Total Size (GiB)</th>
                <th>Daily Change (GB)</th>
                <th>Daily Change (%)</th>
                <th>Job Count</th>
            </tr>
    """
    
    for r in all_stats:
        html += f"""
            <tr>
                <td>{r.get('resource_name')}</td>
                <td>{r.get('resource_type')}</td>
                <td>{r.get('job_source')}</td>
                <td>{r.get('total_resource_size_gb')}</td>
                <td>{r.get('current_daily_change_gb')}</td>
                <td>{r.get('current_daily_change_pct')}</td>
                <td>{r.get('backup_job_count')}</td>
            </tr>
        """
        
    html += """
        </table>
    </body>
    </html>
    """
    return html

@app.route("/", methods=["POST", "GET"])
def index():
    """
    Entry point for the Cloud Run service.
    Triggered by Cloud Scheduler or manually.
    """
    try:
        # Parse optional parameters from request
        # e.g., ?days=7 to analyze past 7 days for stats
        days = int(request.args.get('days', 7))
        filter_name = request.args.get('filter_name')
        source_type = request.args.get('source_type', 'all')
        output_format = request.args.get('format', 'json').lower()
        
        project_id = os.environ.get('GOOGLE_CLOUD_PROJECT')
        
        if not project_id:
            # Fallback for local testing if env var not set
            # In Cloud Run, this is usually set automatically or we can pass it
            return "GOOGLE_CLOUD_PROJECT environment variable not set", 500

        logger.info(f"Starting GCBDR analysis for project {project_id} with {days} days history")
        
        results = analyze_backup_jobs(project_id, days, filter_name=filter_name, source_type=source_type)
        
        if output_format == 'csv':
            return format_csv(results), 200, {'Content-Type': 'text/csv'}
        elif output_format == 'html':
            return format_html(results), 200, {'Content-Type': 'text/html'}
        else:
            return jsonify(results), 200
        
    except Exception as e:
        logger.exception("Error during analysis")
        return f"Internal Server Error: {str(e)}", 500

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
