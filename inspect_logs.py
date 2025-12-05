import logging
import os
from google.cloud import logging as cloud_logging
from datetime import datetime, timedelta, timezone

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def inspect_logs(project_id, days=1):
    client = cloud_logging.Client(project=project_id)
    
    now = datetime.now(timezone.utc)
    start_time = now - timedelta(days=days)
    
    log_filter = f"""
    timestamp >= "{start_time.isoformat()}"
    logName="projects/{project_id}/logs/backupdr.googleapis.com%2Fbdr_backup_restore_jobs"
    """
    
    logger.info(f"Querying logs with filter: {log_filter}")
    
    try:
        # Fetch just 5 entries to inspect
        for entry in client.list_entries(filter_=log_filter, page_size=5):
            payload = entry.payload
            if payload:
                print("\n--- Log Entry Payload Keys ---")
                for key, value in payload.items():
                    print(f"{key}: {value} (Type: {type(value)})")
                return # Just need one good sample
    except Exception as e:
        logger.error(f"Failed to fetch logs: {e}")

if __name__ == "__main__":
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
    if not project_id:
        print("Please set GOOGLE_CLOUD_PROJECT environment variable.")
    else:
        inspect_logs(project_id)
