
import logging
from google.cloud import logging as cloud_logging
from datetime import datetime, timedelta, timezone
import os

def debug_fetch():
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT", "argo-svc-gcbdr")
    print(f"Project ID: {project_id}")
    
    client = cloud_logging.Client(project=project_id)
    
    days = 7
    now = datetime.now(timezone.utc)
    # Match the Go implementation's time exactly if possible, or close enough
    # Go: time.Now().AddDate(0, 0, -days)
    start_time = now - timedelta(days=days)
    
    # 1. Vault Filter
    vault_filter = f"""logName="projects/{project_id}/logs/backupdr.googleapis.com%2Fbdr_backup_restore_jobs" AND timestamp >= "{start_time.isoformat()}" """
    # vault_filter = f"""
    # timestamp >= "{start_time.isoformat()}"
    # logName="projects/{project_id}/logs/backupdr.googleapis.com%2Fbdr_backup_restore_jobs"
    # """
    
    # Note: Python implementation used multiline string, which adds newlines. 
    # Let's try to match the Go single line first.
    
    print(f"\n[Vault] Querying logs with filter:\n{vault_filter}")
    
    try:
        entries = list(client.list_entries(filter_=vault_filter, page_size=10))
        print(f"[Vault] Found {len(entries)} entries")
        if entries:
            print(f"[Vault] First entry timestamp: {entries[0].timestamp}")
            print(f"[Vault] First entry payload keys: {entries[0].payload.keys() if entries[0].payload else 'None'}")
    except Exception as e:
        print(f"[Vault] Error: {e}")

    # 2. Appliance Filter
    appliance_filter = f"""logName="projects/{project_id}/logs/backupdr.googleapis.com%2Fbackup_recovery_appliance_events" AND jsonPayload.eventId=44003 AND timestamp >= "{start_time.isoformat()}" """
    
    print(f"\n[Appliance] Querying logs with filter:\n{appliance_filter}")
    
    try:
        entries = list(client.list_entries(filter_=appliance_filter, page_size=10))
        print(f"[Appliance] Found {len(entries)} entries")
        if entries:
             print(f"[Appliance] First entry timestamp: {entries[0].timestamp}")
    except Exception as e:
        print(f"[Appliance] Error: {e}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    debug_fetch()
