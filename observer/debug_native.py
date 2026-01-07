import google.cloud.logging
from datetime import datetime, timedelta, timezone
import sys

def probe_logs(project_id, lookback_hours=336): # Look back 14 days
    print(f"Probing Cloud Logging for ALL Backup events in project: {project_id}")
    client = google.cloud.logging.Client(project=project_id)
    
    now = datetime.now(timezone.utc)
    time_filter = now - timedelta(hours=lookback_hours)
    timestamp_str = time_filter.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    
    # Wide filter to catch anything relevant
    filter_str = (
        f'timestamp >= "{timestamp_str}" AND '
        f'('
        f' resource.type:"backupdr" OR '
        f' logName:"backup" OR '
        f' jsonPayload.jobType:"BACKUP" OR '
        f' jsonPayload.jobCategory:"BACKUP"'
        f')'
    )
    
    print(f"Filter: {filter_str}")
    
    try:
        entries = client.list_entries(filter_=filter_str, page_size=500)
        
        counts = {}
        
        print("\n--- SCANNING LOGS ---")
        for i, entry in enumerate(entries):
            if i > 2000: break # Safety limit
            
            r_type = entry.resource.type
            log_name = entry.log_name
            payload = entry.payload if isinstance(entry.payload, dict) else {}
            
            # Extract potential AlloyDB identifiers
            src_name = payload.get('sourceResourceName', 'N/A')
            job_category = payload.get('jobCategory', 'N/A')
            
            key = f"ResType:{r_type} | Log:{log_name.split('/')[-1]} | Cat:{job_category}"
            if key not in counts:
                counts[key] = 0
            counts[key] += 1
            
            # Print explicit AlloyDB matches immediately
            is_alloy = "alloy" in str(payload).lower() or "alloy" in r_type.lower() or "alloy" in src_name.lower()
            
            # ALSO print one sample of the generic "BackupDRProject" jobs to see what they are
            is_generic_job = "bdr_backup_restore_jobs" in log_name and "BackupDRProject" in r_type
            
            if is_alloy or (is_generic_job and counts[key] == 1):
                print(f"\n!!! MATCH FOUND ({'ALLOY' if is_alloy else 'GENERIC JOB'}) !!!")
                print(f"Key: {key}")
                print(f"Payload: {payload}")
                print("-" * 50)

        print("\n--- SUMMARY OF FOUND LOG TYPES ---")
        for k, v in counts.items():
            print(f"{k} => {v} entries")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python debug_native.py <PROJECT_ID>")
        sys.exit(1)
    probe_logs(sys.argv[1])
