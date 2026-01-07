import google.cloud.logging
from datetime import datetime, timedelta, timezone
import sys

def probe_logs(project_id, lookback_hours=168): # Look back 7 days
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
        entries = client.list_entries(filter_=filter_str, page_size=20)
        found_types = set()
        
        print("\n--- SAMPLE ENTRIES ---")
        for entry in entries:
            r_type = entry.resource.type
            log_name = entry.log_name
            payload = entry.payload
            
            # Print if we haven't seen this combo before
            key = f"{r_type}|{log_name}"
            if key not in found_types:
                found_types.add(key)
                print(f"\nType: {r_type}")
                print(f"Log: {log_name}")
                print(f"Payload: {payload}")
                
                # Check for AlloyDB specific strings
                if "alloy" in str(payload).lower() or "alloy" in r_type.lower():
                    print("!!! FOUND ALLOYDB MATCH !!!")
                    
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python debug_native.py <PROJECT_ID>")
        sys.exit(1)
    probe_logs(sys.argv[1])
