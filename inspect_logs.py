import logging
import os
import argparse
from google.cloud import logging as cloud_logging
from datetime import datetime, timedelta, timezone

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def inspect_logs(project_id, days=1, log_type="vault"):
    client = cloud_logging.Client(project=project_id)
    
    now = datetime.now(timezone.utc)
    start_time = now - timedelta(days=days)
    
    if log_type == "appliance":
        log_filter = f"""
        timestamp >= "{start_time.isoformat()}"
        logName="projects/{project_id}/logs/backupdr.googleapis.com%2Fbackup_recovery_appliance_events"
        jsonPayload.eventId = (44003)
        """
    else:
        log_filter = f"""
        timestamp >= "{start_time.isoformat()}"
        logName="projects/{project_id}/logs/backupdr.googleapis.com%2Fbdr_backup_restore_jobs"
        """
    
    logger.info(f"Querying logs with filter: {log_filter}")
    
    try:
        # Fetch just 5 entries to inspect
        entries = list(client.list_entries(filter_=log_filter, page_size=5))
        if not entries:
            logger.info("No logs found.")
            return

        for entry in entries:
            payload = entry.payload
            if payload:
                print("\n--- Log Entry Payload Keys ---")
                for key, value in payload.items():
                    print(f"{key}: {value} (Type: {type(value)})")
                # Print full payload for deep inspection
                print("\n--- Full Payload ---")
                print(payload)
                return # Just need one good sample
    except Exception as e:
        logger.error(f"Failed to fetch logs: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inspect GCBDR logs.")
    parser.add_argument("--type", choices=["vault", "appliance"], default="vault", help="Type of logs to inspect")
    parser.add_argument("--days", type=int, default=1, help="Days of history to inspect")
    args = parser.parse_args()

    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
    if not project_id:
        print("Please set GOOGLE_CLOUD_PROJECT environment variable.")
    else:
        inspect_logs(project_id, days=args.days, log_type=args.type)
