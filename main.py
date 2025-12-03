import os
import logging
from flask import Flask, request, jsonify
from analyzer import analyze_backup_jobs

app = Flask(__name__)

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
        # e.g., ?days=7 to analyze past 7 days for stats
        days = int(request.args.get('days', 7))
        project_id = os.environ.get('GOOGLE_CLOUD_PROJECT')
        
        if not project_id:
            # Fallback for local testing if env var not set
            # In Cloud Run, this is usually set automatically or we can pass it
            return "GOOGLE_CLOUD_PROJECT environment variable not set", 500

        logger.info(f"Starting GCBDR analysis for project {project_id} with {days} days history")
        
        results = analyze_backup_jobs(project_id, days)
        
        return jsonify(results), 200
        
    except Exception as e:
        logger.exception("Error during analysis")
        return f"Internal Server Error: {str(e)}", 500

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
