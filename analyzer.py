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
    # 'sourceResourceSizeBytes' is a common field for total size in bytes
    # 'usedStorageGib' might also be available
    total_size_bytes = 0
    if payload.get('sourceResourceSizeBytes'):
        total_size_bytes = int(payload.get('sourceResourceSizeBytes'))
    elif payload.get('usedStorageGib'):
        total_size_bytes = int(float(payload.get('usedStorageGib')) * 1024 * 1024 * 1024)

    return {
        'jobId': payload.get('jobId'),
        'jobStatus': payload.get('jobStatus'), # RUNNING, SKIPPED, SUCCESSFUL, FAILED
        'startTime': payload.get('startTime'),
        'endTime': payload.get('endTime'),
        'jobCategory': payload.get('jobCategory'),
        'resourceType': payload.get('resourceType'),
        'sourceResourceName': payload.get('sourceResourceName'),
        'bytes_transferred': int(payload.get('incrementalBackupSizeGib', 0) * 1024 * 1024 * 1024) if payload.get('incrementalBackupSizeGib') else 0, # Convert GiB to bytes for compatibility
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
    
    # 3. Calculate stats from history
    stats = calculate_statistics(history_jobs)
    
    # 4. Detect anomalies in current jobs
    anomalies = detect_anomalies(current_jobs, stats)
    
    # Format resource stats for output
    resource_stats_list = []
    for res, data in stats.items():
        resource_stats_list.append({
            "resource_name": res,
            "resource_type": data['resource_type'],
            "avg_daily_change_gb": round(data['avg_daily_change_gb'], 2),
            "avg_daily_change_pct": round(data['avg_daily_change_pct'], 2)
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
