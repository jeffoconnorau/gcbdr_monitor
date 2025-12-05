import logging
from google.cloud import logging as cloud_logging
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

def fetch_backup_logs(project_id, days):
    """
    Queries Cloud Logging for GCBDR job logs using the specific log name.
    Returns a list of relevant log entries.
    """
    client = cloud_logging.Client(project=project_id)
    
    # Calculate time range
    now = datetime.now(timezone.utc)
    start_time = now - timedelta(days=days)
    
    # Construct filter for GCBDR backup jobs based on the SQL query
    log_filter = f"""
    timestamp >= "{start_time.isoformat()}"
    logName="projects/{project_id}/logs/backupdr.googleapis.com%2Fbdr_backup_restore_jobs"
    """
    
    logger.info(f"Querying logs with filter: {log_filter}")
    
    entries = []
    try:
        for entry in client.list_entries(filter_=log_filter, page_size=1000):
            entries.append(entry)
    except Exception as e:
        logger.error(f"Failed to fetch logs: {e}")
        raise

    return entries

def parse_job_data(entry):
    """
    Extracts relevant data from a log entry's jsonPayload.
    Returns a dict with structured data.
    """
    payload = entry.payload
    if not payload:
        return None

    # Extract fields as per SQL query
    # Try to find total size for percentage calculation
    total_size_bytes = 0
    
    # Check top-level fields
    if payload.get('sourceResourceSizeBytes'):
        total_size_bytes = int(payload.get('sourceResourceSizeBytes'))
    elif payload.get('usedStorageGib'):
        total_size_bytes = int(float(payload.get('usedStorageGib')) * 1024 * 1024 * 1024)
    elif payload.get('sourceResourceDataSizeGib'):
        total_size_bytes = int(float(payload.get('sourceResourceDataSizeGib')) * 1024 * 1024 * 1024)
    
    # Check nested protectedResourceDetails if not found
    if total_size_bytes == 0:
        protected_details = payload.get('protectedResourceDetails', {})
        if protected_details.get('sourceResourceSizeBytes'):
            total_size_bytes = int(protected_details.get('sourceResourceSizeBytes'))
        elif protected_details.get('usedStorageGib'):
            total_size_bytes = int(float(protected_details.get('usedStorageGib')) * 1024 * 1024 * 1024)
        elif protected_details.get('sourceResourceDataSizeGib'):
            total_size_bytes = int(float(protected_details.get('sourceResourceDataSizeGib')) * 1024 * 1024 * 1024)

    # Ensure incrementalBackupSizeGib is float
    inc_size_gib = float(payload.get('incrementalBackupSizeGib', 0))

    return {
        'jobId': payload.get('jobId'),
        'jobStatus': payload.get('jobStatus'), # RUNNING, SKIPPED, SUCCESSFUL, FAILED
        'startTime': payload.get('startTime'),
        'endTime': payload.get('endTime'),
        'jobCategory': payload.get('jobCategory'),
        'resourceType': payload.get('resourceType'),
        'sourceResourceName': payload.get('sourceResourceName'),
        'bytes_transferred': int(inc_size_gib * 1024 * 1024 * 1024), # Convert GiB to bytes
        'total_resource_size_bytes': total_size_bytes,
        'timestamp': entry.timestamp,
        'json_payload': payload # Keep original payload for reference if needed
    }

def process_jobs(parsed_logs):
    """
    Aggregates logs by jobId and determines the final status.
    Logic mimics the SQL:
    - Group by jobId
    - Priority: FAILED (4) > SUCCESSFUL (3) > SKIPPED (2) > RUNNING (1)
    """
    jobs_map = {}
    
    # Status priority map
    status_priority = {
        "RUNNING": 1,
        "SKIPPED": 2,
        "SUCCESSFUL": 3,
        "FAILED": 4
    }

    for log in parsed_logs:
        if not log or not log.get('jobId'):
            continue
            
        job_id = log['jobId']
        status = log.get('jobStatus', 'UNKNOWN')
        priority = status_priority.get(status, 0)
        
        if job_id not in jobs_map:
            jobs_map[job_id] = {
                'jobId': job_id,
                'max_priority': priority,
                'final_status': status,
                'logs': [log]
            }
        else:
            jobs_map[job_id]['logs'].append(log)
            if priority > jobs_map[job_id]['max_priority']:
                jobs_map[job_id]['max_priority'] = priority
                jobs_map[job_id]['final_status'] = status

    # Now construct the final list of jobs, using the log entry that matches the final status
    # (or the latest one if multiple match, though SQL joins on status)
    final_jobs = []
    for job_id, data in jobs_map.items():
        final_status = data['final_status']
        
        # Find the log entry that corresponds to this final status
        # In SQL: JOIN ... ON ... AND json_payload.jobStatus = finalStatus
        # We'll take the first one that matches, or just the last one if none match (fallback)
        matching_log = next((l for l in data['logs'] if l.get('jobStatus') == final_status), None)
        
        if not matching_log:
            # Fallback: use the latest log
            matching_log = sorted(data['logs'], key=lambda x: x['timestamp'], reverse=True)[0]
            
        # Add derived fields
        job_data = matching_log.copy()
        job_data['status'] = final_status # Standardize key for existing logic
        job_data['resource_name'] = job_data.get('sourceResourceName', 'unknown')
        
        final_jobs.append(job_data)
        
    return final_jobs

    for res in all_resources:
        h_data = history_stats.get(res, {})
        c_data = current_stats.get(res, {})
        
        # Defaults
        avg_daily_change_gb = h_data.get('avg_daily_change_gb', 0)
        current_daily_change_gb = c_data.get('avg_daily_change_gb', 0)
        current_daily_change_pct = c_data.get('avg_daily_change_pct', 0)
        resource_type = h_data.get('resource_type') or c_data.get('resource_type') or 'UNKNOWN'
        
        # Calculate average total size (prefer current, fallback to history)
        # total_size_sum / count = avg_total_size_bytes
        # We need to recalculate or extract it from stats if we want it exact
        # Let's add avg_total_size_gb to calculate_statistics first? 
        # Actually, calculate_statistics doesn't return it directly, let's add it there or compute here.
        # Wait, calculate_statistics returns 'avg_daily_change_pct' which uses total size.
        # Let's modify calculate_statistics to return avg_total_size_gb as well.
        
        # RE-READING calculate_statistics implementation in previous turn:
        # It calculates avg_total_size but doesn't return it in the result dict.
        # I need to modify calculate_statistics first.
        pass

def calculate_statistics(job_history):
    """
    Computes average change rate per resource.
    Returns a dict with resource stats.
    """
    stats = {}
    for job in job_history:
        resource = job['resource_name']
        if resource not in stats:
            stats[resource] = {
                'total_bytes': 0, 
                'count': 0, 
                'total_size_sum': 0,
                'resource_type': job.get('resourceType', 'UNKNOWN'),
                'timestamps': []
            }
        
        stats[resource]['total_bytes'] += job['bytes_transferred']
        stats[resource]['total_size_sum'] += job.get('total_resource_size_bytes', 0)
        stats[resource]['count'] += 1
        stats[resource]['timestamps'].append(job['timestamp'])
    
    results = {}
    for resource, data in stats.items():
        if data['count'] > 0:
            avg_bytes = data['total_bytes'] / data['count']
            avg_total_size = data['total_size_sum'] / data['count']
            
            avg_daily_change_pct = 0
            if avg_total_size > 0:
                avg_daily_change_pct = (avg_bytes / avg_total_size) * 100
                
            results[resource] = {
                'avg_bytes': avg_bytes,
                'avg_daily_change_gb': avg_bytes / (1024 * 1024 * 1024),
                'avg_daily_change_pct': avg_daily_change_pct,
                'avg_total_size_gb': avg_total_size / (1024 * 1024 * 1024),
                'resource_type': data['resource_type'],
                'data_points': data['count']
            }
    return results

def detect_anomalies(current_jobs, stats, threshold_factor=1.5):
    """
    Identifies jobs that exceed the average by a certain factor.
    """
    anomalies = []
    for job in current_jobs:
        resource = job['resource_name']
        if resource in stats:
            avg = stats[resource]['avg_bytes']
            if avg > 0 and job['bytes_transferred'] > (avg * threshold_factor):
                anomalies.append({
                    'job_id': job['jobId'],
                    'resource': resource,
                    'bytes': job['bytes_transferred'],
                    'avg_bytes': avg,
                    'factor': job['bytes_transferred'] / avg
                })
    return anomalies

def analyze_backup_jobs(project_id, days=7):
    """
    Main orchestration function.
    """
    # 1. Fetch logs
    all_logs = fetch_backup_logs(project_id, days)
    parsed_logs = [parse_job_data(e) for e in all_logs]
    
    # 2. Process and deduplicate jobs
    unique_jobs = process_jobs(parsed_logs)
    
    # Filter by status
    successful_jobs = [j for j in unique_jobs if j['status'] == 'SUCCESSFUL']
    failed_jobs = [j for j in unique_jobs if j['status'] == 'FAILED']
    other_jobs = [j for j in unique_jobs if j['status'] not in ('SUCCESSFUL', 'FAILED')]
    
    logger.info(f"Total unique jobs: {len(unique_jobs)}")
    logger.info(f"Successful: {len(successful_jobs)}")
    logger.info(f"Failed: {len(failed_jobs)}")
    
    if not successful_jobs:
        logger.info("No successful jobs found for analysis.")
        return {
            "status": "no_successful_data",
            "total_jobs": len(unique_jobs),
            "successful_count": len(successful_jobs),
            "failed_count": len(failed_jobs),
            "other_count": len(other_jobs)
        }

    # Split into 'today' (or most recent) and 'history'
    # Using successful jobs only for analysis
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=1)
    
    current_jobs = [j for j in successful_jobs if j['timestamp'] > cutoff]
    history_jobs = [j for j in successful_jobs if j['timestamp'] <= cutoff]
    
    # 3. Calculate stats
    history_stats = calculate_statistics(history_jobs)
    current_stats = calculate_statistics(current_jobs)
    
    # 4. Detect anomalies in current jobs (using individual job data against history)
    anomalies = detect_anomalies(current_jobs, history_stats)
    
    # Format resource stats for output - merging current and history
    resource_stats_list = []
    
    # Get all resources from both sets
    all_resources = set(history_stats.keys()) | set(current_stats.keys())
    
    for res in all_resources:
        h_data = history_stats.get(res, {})
        c_data = current_stats.get(res, {})
        
        # Defaults
        avg_daily_change_gb = h_data.get('avg_daily_change_gb', 0)
        current_daily_change_gb = c_data.get('avg_daily_change_gb', 0)
        current_daily_change_pct = c_data.get('avg_daily_change_pct', 0)
        # Prefer current total size, fallback to history
        total_resource_size_gb = c_data.get('avg_total_size_gb') or h_data.get('avg_total_size_gb') or 0
        
        # Fallback to GCE API if size is still 0 and it looks like a GCE instance
        if total_resource_size_gb == 0 and resource_type == 'GCE_INSTANCE':
            try:
                # Assuming resource_name format: projects/{project}/zones/{zone}/instances/{instance}
                # But log might have it as just the instance name or full path.
                # Let's check what we have. The logs usually have 'sourceResourceName' which might be full path.
                # If it's just name, we might need to guess or search (expensive).
                # Let's assume it's a full path or we can construct it if we know the project/zone.
                # Wait, the log 'sourceResourceName' is often just the instance name or a partial path.
                # The 'resource_name' key in our stats comes from 'sourceResourceName'.
                # Let's try to parse it.
                
                # We need a cache to avoid repeated calls
                if not hasattr(analyze_backup_jobs, 'gce_cache'):
                    analyze_backup_jobs.gce_cache = {}
                
                if res in analyze_backup_jobs.gce_cache:
                    total_resource_size_gb = analyze_backup_jobs.gce_cache[res]
                else:
                    # We need to find a sample job to get more details if needed, 
                    # but we only have the resource name here.
                    # Let's try to fetch details.
                    size_gb = fetch_gce_instance_details(project_id, res)
                    if size_gb > 0:
                        total_resource_size_gb = size_gb
                        analyze_backup_jobs.gce_cache[res] = size_gb
            except Exception as e:
                logger.warning(f"Failed to fetch GCE details for {res}: {e}")

        resource_stats_list.append({
            "resource_name": res,
            "resource_type": resource_type,
            "total_resource_size_gb": round(total_resource_size_gb, 2),
            "avg_daily_change_gb": round(avg_daily_change_gb, 2),
            "current_daily_change_gb": round(current_daily_change_gb, 2),
            "current_daily_change_pct": round(current_daily_change_pct, 2),
            "growth_rate_pct": round(growth_rate_pct, 2)
        })
    
    logger.info(f"Found {len(anomalies)} anomalies.")
    
    return {
        "analyzed_jobs_count": len(current_jobs),
        "anomalies": anomalies,
        "resource_stats": resource_stats_list,
        "total_jobs_found": len(unique_jobs),
        "successful_count": len(successful_jobs),
        "failed_count": len(failed_jobs),
        "other_count": len(other_jobs)
    }

def fetch_gce_instance_details(project_id, resource_name):
    """
    Fetches instance details from GCE to get disk size.
    resource_name might be a full path or just a name.
    If it's just a name, we might need to search or we might fail if zone is unknown.
    However, GCBDR logs usually contain full resource URL in some fields, 
    but 'sourceResourceName' might be short.
    Let's assume for now we can try to find it if we have zone info, 
    but we don't strictly have zone here unless we extracted it earlier.
    
    Actually, let's try to use the AggregatedList to find the instance by name if we don't have zone.
    Or if resource_name is a URL like projects/p/zones/z/instances/i, we parse it.
    """
    from google.cloud import compute_v1
    
    # Try to parse project/zone/instance from resource_name
    # Format: //compute.googleapis.com/projects/{project}/zones/{zone}/instances/{instance}
    # OR: projects/{project}/zones/{zone}/instances/{instance}
    
    import re
    match = re.search(r'projects/([^/]+)/zones/([^/]+)/instances/([^/]+)', resource_name)
    
    target_project = project_id
    target_zone = None
    instance_name = resource_name
    
    if match:
        target_project = match.group(1)
        target_zone = match.group(2)
        instance_name = match.group(3)
    
    try:
        if target_zone:
            client = compute_v1.InstancesClient()
            instance = client.get(project=target_project, zone=target_zone, instance=instance_name)
            return _calculate_disk_size(instance)
        else:
            # If we don't have zone, we might need to search all zones (expensive)
            # or just fail. For now, let's log warning and return 0 if no zone.
            # But wait, maybe we can use AggregatedList?
            client = compute_v1.InstancesClient()
            request = compute_v1.AggregatedListInstancesRequest(project=project_id)
            # Filter by name? 'name = instance_name'
            # The filter syntax is specific.
            request.filter = f"name = {instance_name}"
            
            # This might be slow for many instances.
            # Let's try to be efficient.
            # If we really can't parse it, maybe we skip for now to avoid performance hit?
            # User said "resource shows it's project name in the resource_name".
            # If it's just project, we still need zone.
            
            # Let's try AggregatedList with filter.
            for zone, response in client.aggregated_list(request=request):
                if response.instances:
                    for instance in response.instances:
                        if instance.name == instance_name:
                            return _calculate_disk_size(instance)
            
            return 0
            
    except Exception as e:
        logger.warning(f"Error fetching GCE details: {e}")
        return 0

def _calculate_disk_size(instance):
    total_gb = 0
    # Boot disk
    for disk in instance.disks:
        total_gb += disk.disk_size_gb
    return total_gb
