import logging
from google.cloud import logging as cloud_logging
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

def fetch_backup_logs(project_id, days):
    """
    Queries Cloud Logging for GCBDR job completion logs.
    Returns a list of relevant log entries.
    """
    client = cloud_logging.Client(project=project_id)
    
    # Calculate time range
    now = datetime.now(timezone.utc)
    start_time = now - timedelta(days=days)
    
    # Construct filter for GCBDR backup jobs
    # Note: This is a placeholder filter. We need to refine this based on actual GCBDR log structure.
    # Typically looking for "Job succeeded" or similar in jsonPayload
    # and resource.type="gce_instance" or specific GCBDR resource types.
    # For now, we'll assume a generic filter that we might need to adjust.
    log_filter = f"""
    timestamp >= "{start_time.isoformat()}"
    jsonPayload.message =~ "Backup.*succeeded" OR jsonPayload.event_type = "BACKUP_FINISHED"
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
    Extracts relevant data from a log entry.
    Returns a dict with job_id, resource_name, change_rate_bytes, etc.
    """
    payload = entry.payload
    # This extraction logic depends heavily on the actual log format.
    # We will assume some standard fields for now.
    
    # Mock structure for demonstration
    return {
        'job_id': payload.get('job_id'),
        'resource_name': payload.get('resource_name', 'unknown-resource'),
        'bytes_transferred': int(payload.get('bytes_transferred', 0)),
        'timestamp': entry.timestamp,
        'status': payload.get('status')
    }

def calculate_statistics(job_history):
    """
    Computes average change rate per resource.
    """
    stats = {}
    for job in job_history:
        resource = job['resource_name']
        if resource not in stats:
            stats[resource] = {'total_bytes': 0, 'count': 0, 'timestamps': []}
        
        stats[resource]['total_bytes'] += job['bytes_transferred']
        stats[resource]['count'] += 1
        stats[resource]['timestamps'].append(job['timestamp'])
    
    results = {}
    for resource, data in stats.items():
        if data['count'] > 0:
            results[resource] = {
                'avg_bytes': data['total_bytes'] / data['count'],
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
                    'job_id': job['job_id'],
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
    # We might need to fetch a bit more than 'days' to ensure we have the current day's jobs too
    # or separate the 'history' fetch from 'current' fetch.
    # For simplicity, let's fetch everything in range and split by time.
    
    all_logs = fetch_backup_logs(project_id, days)
    parsed_jobs = [parse_job_data(e) for e in all_logs if e.payload]
    
    if not parsed_jobs:
        logger.info("No jobs found.")
        return {"status": "no_data"}

    # Split into 'today' (or most recent) and 'history'
    # This is a simplification. In production, 'current' might be "jobs from the last 24h"
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=1)
    
    current_jobs = [j for j in parsed_jobs if j['timestamp'] > cutoff]
    history_jobs = [j for j in parsed_jobs if j['timestamp'] <= cutoff]
    
    # 2. Calculate stats from history
    stats = calculate_statistics(history_jobs)
    
    # 3. Detect anomalies in current jobs
    anomalies = detect_anomalies(current_jobs, stats)
    
    logger.info(f"Found {len(anomalies)} anomalies.")
    
    return {
        "analyzed_jobs_count": len(current_jobs),
        "anomalies": anomalies
    }
