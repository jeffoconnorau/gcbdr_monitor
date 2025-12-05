import logging
import os
import re
from google.cloud import logging as cloud_logging
from google.cloud import compute_v1
from datetime import datetime, timedelta, timezone

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_gce_instance_details(project_id, resource_name):
    print(f"\n--- Debugging Resource: {resource_name} ---")
    
    # Regex to extract project, zone, and instance
    match = re.search(r'projects/([^/]+)/zones/([^/]+)/instances/([^/]+)', resource_name)
    
    target_project = project_id
    target_zone = None
    instance_name = resource_name
    
    if match:
        target_project = match.group(1)
        target_zone = match.group(2)
        instance_name = match.group(3)
        print(f"Parsed: Project={target_project}, Zone={target_zone}, Instance={instance_name}")
    else:
        print("Regex did not match full path.")
        proj_match = re.search(r'projects/([^/]+)', resource_name)
        if proj_match:
            target_project = proj_match.group(1)
            print(f"Parsed Project from partial path: {target_project}")
        
        if '/' in instance_name and not match:
             instance_name = resource_name.split('/')[-1]
             print(f"Extracted instance name from path: {instance_name}")

    try:
        client = compute_v1.InstancesClient()
        
        if target_zone:
            print(f"Attempting direct get in zone {target_zone}...")
            try:
                instance = client.get(project=target_project, zone=target_zone, instance=instance_name)
                size = _calculate_disk_size(instance)
                print(f"SUCCESS: Found instance. Total Size: {size} GB")
                return size
            except Exception as e:
                print(f"Direct get failed: {e}")

        print(f"Attempting AggregatedList in project {target_project} for name {instance_name}...")
        request = compute_v1.AggregatedListInstancesRequest(project=target_project)
        request.filter = f"name = {instance_name}"
        
        found = False
        for zone, response in client.aggregated_list(request=request):
            if response.instances:
                for instance in response.instances:
                    if instance.name == instance_name:
                        size = _calculate_disk_size(instance)
                        print(f"SUCCESS: Found instance in {zone}. Total Size: {size} GB")
                        return size
        if not found:
            print("FAILED: Instance not found in AggregatedList.")
            
    except Exception as e:
        print(f"API Error: {e}")
        return 0

def _calculate_disk_size(instance):
    total_gb = 0
    if instance.disks:
        for disk in instance.disks:
            total_gb += disk.disk_size_gb
    return total_gb

def debug_logs(project_id, days=1):
    client = cloud_logging.Client(project=project_id)
    now = datetime.now(timezone.utc)
    start_time = now - timedelta(days=days)
    
    log_filter = f"""
    timestamp >= "{start_time.isoformat()}"
    logName="projects/{project_id}/logs/backupdr.googleapis.com%2Fbdr_backup_restore_jobs"
    """
    
    print(f"Querying logs...")
    entries = list(client.list_entries(filter_=log_filter, page_size=50))
    print(f"Found {len(entries)} entries.")
    
    seen_resources = set()
    
    for entry in entries:
        payload = entry.payload
        if not payload: continue
        
        res_name = payload.get('sourceResourceName')
        res_type = payload.get('resourceType')
        
        if res_name and res_name not in seen_resources:
            seen_resources.add(res_name)
            print(f"\nFound Resource: Name='{res_name}', Type='{res_type}'")
            
            # Try to fetch details
            fetch_gce_instance_details(project_id, res_name)

if __name__ == "__main__":
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
    if not project_id:
        print("Please set GOOGLE_CLOUD_PROJECT environment variable.")
    else:
        debug_logs(project_id)
