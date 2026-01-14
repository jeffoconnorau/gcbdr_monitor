import os
import logging
from flask import Flask, request, jsonify, render_template
from analyzer import analyze_backup_jobs
from formatters import format_csv
from notifier import NotificationManager

app = Flask(__name__)
VERSION = "2.0.0"

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.route("/", methods=["POST", "GET"])
def index():
    """
    Entry point for the Cloud Run service.
    Triggered by Cloud Scheduler or manually.
    """
    try:
        # Parse optional parameters from request
        days = int(request.args.get('days', 7))
        filter_name = request.args.get('filter_name')
        source_type = request.args.get('source_type', 'all')
        output_format = request.args.get('format', 'json').lower()
        
        notify_param = request.args.get('notify', 'true').lower()
        should_notify = notify_param == 'true'
        
        project_id = os.environ.get('GOOGLE_CLOUD_PROJECT')
        
        if not project_id:
            return "GOOGLE_CLOUD_PROJECT environment variable not set", 500

        logger.info(f"Starting GCBDR analysis v{VERSION} for project {project_id} with {days} days history")
        
        results = analyze_backup_jobs(project_id, days, filter_name=filter_name, source_type=source_type)
        
        anomalies = results.get('anomalies', [])
        if anomalies and should_notify:
            logger.info(f"Sending notifications for {len(anomalies)} anomalies...")
            nm = NotificationManager(project_id)
            nm.send_notifications(anomalies)
        
        if output_format == 'csv':
            return format_csv(results), 200, {'Content-Type': 'text/csv'}
        elif output_format == 'html':
            vault_stats = results.get('vault_workloads', {}).get('resource_stats', [])
            appliance_stats = results.get('appliance_workloads', {}).get('resource_stats', [])
            all_stats = vault_stats + appliance_stats
            
            zero_size_vault_count = sum(1 for r in vault_stats if r.get('total_resource_size_gb', 0) == 0)
            total_vault_count = len(vault_stats)
            show_permission_warning = total_vault_count > 0 and (zero_size_vault_count / total_vault_count) > 0.8

            return render_template('report.html', 
                                   summary=results.get('summary', {}),
                                   anomalies=anomalies,
                                   all_stats=all_stats,
                                   daily_baselines=results.get('daily_baselines', []),
                                   show_permission_warning=show_permission_warning,
                                   zero_size_vault_count=zero_size_vault_count,
                                   total_vault_count=total_vault_count,
                                   zero_size_pct= (zero_size_vault_count / total_vault_count) if total_vault_count > 0 else 0)
        else:
            return jsonify(results), 200
        
    except Exception as e:
        logger.exception("Error during analysis")
        return f"Internal Server Error: {str(e)}", 500

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
