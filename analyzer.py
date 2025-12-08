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

def fetch_appliance_logs(project_id, days):
    """
    Queries Cloud Logging for GCBDR appliance events (Management Console workloads).
    Specifically targets eventId 44003 (Successful Job).
    """
    client = cloud_logging.Client(project=project_id)
    
    # Calculate time range
    now = datetime.now(timezone.utc)
    start_time = now - timedelta(days=days)
    
    # Construct filter for appliance events
    log_filter = f"""
    timestamp >= "{start_time.isoformat()}"
    logName="projects/{project_id}/logs/backupdr.googleapis.com%2Fbackup_recovery_appliance_events"
    jsonPayload.eventId = (44003)
    """
    
    logger.info(f"Querying appliance logs with filter: {log_filter}")
    
    entries = []
    try:
        for entry in client.list_entries(filter_=log_filter, page_size=1000):
            entries.append(entry)
    except Exception as e:
        logger.error(f"Failed to fetch appliance logs: {e}")
        # Don't raise here, just return empty so we don't block vault logs if this fails
        pass

    return entries

def fetch_gcb_jobs_logs(project_id, days):
    """
    Queries Cloud Logging for GCBDR GCB backup recovery jobs.
    Used to enrich appliance logs with missing data (e.g. size).
    """
    client = cloud_logging.Client(project=project_id)
    
    # Calculate time range
    now = datetime.now(timezone.utc)
    start_time = now - timedelta(days=days)
    
    # Construct filter for GCB jobs
    log_filter = f"""
    timestamp >= "{start_time.isoformat()}"
    logName="projects/{project_id}/logs/backupdr.googleapis.com%2Fgcb_backup_recovery_jobs"
    """
    
    logger.info(f"Querying GCB jobs logs with filter: {log_filter}")
    
    entries = []
    try:
        for entry in client.list_entries(filter_=log_filter, page_size=1000):
            entries.append(entry)
    except Exception as e:
        logger.error(f"Failed to fetch GCB jobs logs: {e}")
        pass

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

def parse_appliance_job_data(entry):
    """
    Extracts relevant data from an appliance log entry (eventId 44003).
    Returns a dict with structured data.
    """
    payload = entry.payload
    if not payload:
        return None

    # Mapping fields based on common appliance log structure and assumptions
    # eventId 44003 implies success
    
    # Try to find bytes transferred
    bytes_transferred = 0
    if payload.get('dataCopiedInBytes'):
        bytes_transferred = int(payload.get('dataCopiedInBytes'))
    elif payload.get('bytesWritten'):
        bytes_transferred = int(payload.get('bytesWritten'))
    elif payload.get('transferSize'):
        bytes_transferred = int(payload.get('transferSize'))

    # Try to find total size
    total_size_bytes = 0
    if payload.get('sourceSize'):
        total_size_bytes = int(payload.get('sourceSize'))
    elif payload.get('appSize'):
        total_size_bytes = int(payload.get('appSize'))
    
    # Job ID might be jobName or srcid
    job_id = payload.get('jobName') or payload.get('srcid') or 'unknown_job'
    
    return {
        'jobId': job_id,
        'jobStatus': 'SUCCESSFUL', # 44003 is success
        'startTime': payload.get('eventTime') or entry.timestamp.isoformat(), # Use eventTime if available
        'endTime': payload.get('eventTime') or entry.timestamp.isoformat(), # Point in time event usually
        'jobCategory': 'ApplianceBackup',
        'resourceType': payload.get('appType', 'ApplianceWorkload'),
        'sourceResourceName': payload.get('appName', 'unknown_app'),
        'bytes_transferred': bytes_transferred,
        'total_resource_size_bytes': total_size_bytes,
        'timestamp': entry.timestamp,
        'json_payload': payload,
        'job_source': 'appliance'
    }

def parse_gcb_job_data(entry):
    """
    Extracts relevant data from a GCB job log entry.
    Returns a dict with structured data for enrichment.
    """
    payload = entry.payload
    if not payload:
        return None

    # Extract fields
    # Use job_name from payload as the primary key for matching
    job_name = payload.get('job_name')
    
    # Fallback to parsing insertId if job_name is missing, though user sample shows it in payload
    if not job_name:
        insert_id = entry.insert_id or payload.get('insertId')
        if insert_id and '_' in insert_id:
            # insertId format in sample: "19750232_142253982799" -> this looks like srcid_applianceId?
            # User said: "first part is the jobName". But sample job_name is "Job_19729093".
            # Sample insertId: "19750232_142253982799".
            # Sample srcid in appliance log: "19750233".
            # Sample jobName in appliance log: "Job_19729093".
            # So job_name "Job_19729093" is the common key.
            pass

    total_size_bytes = 0
    
    # Priority: resource_data_size_in_gib > snapshot_disk_size_in_gib > sourceResourceSizeBytes > usedStorageGib
    if payload.get('resource_data_size_in_gib'):
        total_size_bytes = int(float(payload.get('resource_data_size_in_gib')) * 1024 * 1024 * 1024)
    elif payload.get('snapshot_disk_size_in_gib'):
        total_size_bytes = int(float(payload.get('snapshot_disk_size_in_gib')) * 1024 * 1024 * 1024)
    elif payload.get('sourceResourceSizeBytes'):
        total_size_bytes = int(payload.get('sourceResourceSizeBytes'))
    elif payload.get('usedStorageGib'):
        total_size_bytes = int(float(payload.get('usedStorageGib')) * 1024 * 1024 * 1024)
        
    bytes_transferred = 0
    if payload.get('data_copied_in_gib'):
        bytes_transferred = int(float(payload.get('data_copied_in_gib')) * 1024 * 1024 * 1024)
    elif payload.get('onvault_pool_storage_consumed_in_gib'):
        bytes_transferred = int(float(payload.get('onvault_pool_storage_consumed_in_gib')) * 1024 * 1024 * 1024)

    return {
        'jobName': job_name,
        'total_resource_size_bytes': total_size_bytes,
        'bytes_transferred': bytes_transferred
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
    Returns a list of anomalies with detailed metadata.
    """
    anomalies = []
    for job in current_jobs:
        resource = job['resource_name']
        if resource in stats:
            avg = stats[resource]['avg_bytes']
            if avg > 0 and job['bytes_transferred'] > (avg * threshold_factor):
                # Format timestamp
                ts = job['timestamp']
                # Ensure ts is a datetime object
                if isinstance(ts, str):
                    try:
                        ts = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                    except ValueError:
                        pass # Keep original if parsing fails, or handle error
                
                date_str = ts.strftime('%Y-%m-%d') if isinstance(ts, datetime) else 'unknown'
                time_str = ts.strftime('%H:%M:%S UTC') if isinstance(ts, datetime) else 'unknown'

                anomalies.append({
                    'job_id': job['jobId'],
                    'resource': resource,
                    'resource_type': job.get('resourceType', 'UNKNOWN'),
                    'total_resource_size_gb': round(job.get('total_resource_size_bytes', 0) / (1024**3), 2),
                    'date': date_str,
                    'time': time_str,
                    'bytes': job['bytes_transferred'],
                    'avg_bytes': avg,
                    'gib_transferred': round(job['bytes_transferred'] / (1024**3), 4),
                    'avg_gib': round(avg / (1024**3), 4),
                    'factor': round(job['bytes_transferred'] / avg, 2)
                })
    return anomalies

def analyze_backup_jobs(project_id, days=7):
    """
    Main orchestration function.
    """
    # 1. Fetch logs
    # 1. Fetch logs
    vault_logs = fetch_backup_logs(project_id, days)
    appliance_logs = fetch_appliance_logs(project_id, days)
    gcb_jobs_logs = fetch_gcb_jobs_logs(project_id, days)
    
    parsed_vault_logs = [parse_job_data(e) for e in vault_logs]
    parsed_appliance_logs = [parse_appliance_job_data(e) for e in appliance_logs]
    parsed_gcb_jobs = [parse_gcb_job_data(e) for e in gcb_jobs_logs]
    
    # Create lookup map for GCB jobs data using jobName
    gcb_job_data_map = {}
    for job in parsed_gcb_jobs:
        if job and job.get('jobName'):
            gcb_job_data_map[job['jobName']] = job
    
    # Filter out None values
    parsed_vault_logs = [p for p in parsed_vault_logs if p]
    parsed_appliance_logs = [p for p in parsed_appliance_logs if p]
    
    # 2. Process and deduplicate jobs
    # Vault jobs need deduplication logic (RUNNING -> SUCCESSFUL etc)
    unique_vault_jobs = process_jobs(parsed_vault_logs)
    
    # Appliance jobs (44003) are point-in-time success events, so we treat them as unique successful jobs directly
    # But we should still standardize them
    unique_appliance_jobs = []
    for job in parsed_appliance_logs:
        job['status'] = 'SUCCESSFUL'
        job['resource_name'] = job.get('sourceResourceName', 'unknown')
        
        # Enrich with GCB job data if missing
        job_name = job.get('json_payload', {}).get('jobName')
        if job_name and job_name in gcb_job_data_map:
            gcb_data = gcb_job_data_map[job_name]
            
            if job.get('total_resource_size_bytes', 0) == 0 and gcb_data.get('total_resource_size_bytes', 0) > 0:
                job['total_resource_size_bytes'] = gcb_data['total_resource_size_bytes']
                logger.info(f"Enriched job {job_name} with size {job['total_resource_size_bytes']} from GCB logs")
                
            if job.get('bytes_transferred', 0) == 0 and gcb_data.get('bytes_transferred', 0) > 0:
                job['bytes_transferred'] = gcb_data['bytes_transferred']
                logger.info(f"Enriched job {job_name} with bytes transferred {job['bytes_transferred']} from GCB logs")
        
        unique_appliance_jobs.append(job)
        
    # Combine for total counts, but we might want to analyze them separately or together?
    # User said: "splitting out the reporting, yet also combining the data for aggregate totals"
    
    all_unique_jobs = unique_vault_jobs + unique_appliance_jobs
    
    # Filter by status
    successful_jobs = [j for j in all_unique_jobs if j['status'] == 'SUCCESSFUL']
    failed_jobs = [j for j in all_unique_jobs if j['status'] == 'FAILED']
    other_jobs = [j for j in all_unique_jobs if j['status'] not in ('SUCCESSFUL', 'FAILED')]
    
    logger.info(f"Total unique jobs: {len(all_unique_jobs)}")
    logger.info(f"Successful: {len(successful_jobs)}")
    logger.info(f"Failed: {len(failed_jobs)}")
    
    if not successful_jobs:
        logger.info("No successful jobs found for analysis.")
        return {
            "status": "no_successful_data",
            "total_jobs": len(all_unique_jobs),
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
    # Format resource stats for output    # Calculate statistics for the full period for reporting
    period_stats = calculate_statistics(successful_jobs)
    
    # Calculate statistics for history (baseline) for anomaly detection
    history_stats = calculate_statistics(history_jobs)
    
    anomalies = detect_anomalies(current_jobs, history_stats)

    resource_stats_list = []
    all_resources = set(period_stats.keys())
    
    # Initialize cache for GCE lookups
    gce_cache = {}

    for res in all_resources:
        p_data = period_stats.get(res, {})
        
        # Use period stats for reporting
        avg_daily_change_gb = p_data.get('avg_daily_change_gb', 0)
        total_resource_size_gb = p_data.get('avg_total_size_gb', 0)
        resource_type = p_data.get('resource_type', 'UNKNOWN')

        # Fallback to GCE API if size is still 0 and it looks like a GCE resource
        # resource_type in logs is often 'Compute Engine' or 'Disk'
        if total_resource_size_gb == 0:
            try:
                if resource_type in ('GCE_INSTANCE', 'Compute Engine'):
                    gce_size = 0
                    if res in gce_cache:
                        gce_size = gce_cache[res]
                    else:
                        gce_size = fetch_gce_instance_details(project_id, res)
                        gce_cache[res] = gce_size
                    
                    if gce_size > 0:
                        total_resource_size_gb = gce_size
                elif resource_type in ('Disk', 'Persistent Disk'):
                    disk_size = fetch_gce_disk_details(project_id, res)
                    if disk_size > 0:
                        total_resource_size_gb = disk_size
                elif resource_type in ('Cloud SQL', 'CloudSQL', 'sql-instance', 'SqlInstance'):
                    sql_size = fetch_cloudsql_details(project_id, res)
                    if sql_size > 0:
                        total_resource_size_gb = sql_size
            except Exception as e:
                logger.warning(f"Failed to fetch details for {res} ({resource_type}): {e}")

        # Recalculate percentages if we have a valid total size (especially if it came from GCE)
        # We map 'avg_daily_change_gb' (period average) to 'current_daily_change_gb' output field
        # as per user request to have it reflect the reported period.
        current_daily_change_gb = avg_daily_change_gb
        current_daily_change_pct = 0
        
        if total_resource_size_gb > 0:
            if current_daily_change_gb > 0:
                current_daily_change_pct = (current_daily_change_gb / total_resource_size_gb) * 100

        resource_stats_list.append({
            "resource_name": res,
            "resource_type": resource_type,
            "total_resource_size_gb": round(total_resource_size_gb, 2),
            "current_daily_change_gb": round(current_daily_change_gb, 4),
            "current_daily_change_pct": round(current_daily_change_pct, 2),
            "backup_job_count": p_data.get('data_points', 0),
            "job_source": "vault" if res in [j['resource_name'] for j in unique_vault_jobs] else "appliance" 
        })
    
    # Separate stats for clearer reporting if needed, but 'resource_stats' contains all.
    # We can add a flag or type to resource_stats to distinguish.
    # I added 'job_source' above.
    
    # Split resource stats
    vault_resource_stats = [r for r in resource_stats_list if r['job_source'] == 'vault']
    appliance_resource_stats = [r for r in resource_stats_list if r['job_source'] == 'appliance']
    
    # Calculate counts for vault
    vault_jobs = unique_vault_jobs
    vault_successful = [j for j in vault_jobs if j['status'] == 'SUCCESSFUL']
    vault_failed = [j for j in vault_jobs if j['status'] == 'FAILED']
    
    # Calculate counts for appliance
    appliance_jobs = unique_appliance_jobs
    appliance_successful = [j for j in appliance_jobs if j['status'] == 'SUCCESSFUL']
    appliance_failed = [j for j in appliance_jobs if j['status'] == 'FAILED']
    
    # Calculate aggregate totals
    agg_total_size_gb = sum(r['total_resource_size_gb'] for r in resource_stats_list)
    agg_daily_change_gb = sum(r['current_daily_change_gb'] for r in resource_stats_list)
    agg_daily_change_pct = 0
    if agg_total_size_gb > 0:
        agg_daily_change_pct = (agg_daily_change_gb / agg_total_size_gb) * 100
    
    logger.info(f"Found {len(anomalies)} anomalies.")
    
    return {
        "summary": {
            "total_jobs": len(all_unique_jobs),
            "successful_jobs": len(successful_jobs),
            "failed_jobs": len(failed_jobs),
            "anomalies_count": len(anomalies),
            "total_resource_size_gb": round(agg_total_size_gb, 2),
            "current_daily_change_gb": round(agg_daily_change_gb, 4),
            "current_daily_change_pct": round(agg_daily_change_pct, 2)
        },
        "vault_workloads": {
            "total_jobs": len(vault_jobs),
            "successful_jobs": len(vault_successful),
            "failed_jobs": len(vault_failed),
            "resource_stats": vault_resource_stats
        },
        "appliance_workloads": {
            "total_jobs": len(appliance_jobs),
            "successful_jobs": len(appliance_successful),
            "failed_jobs": len(appliance_failed),
            "resource_stats": appliance_resource_stats
        },
        "anomalies": anomalies
    }

def fetch_gce_instance_details(project_id, resource_name):
    """
    Fetches instance details from GCE to get disk size.
    Parses the project ID from the resource_name if possible.
    """
    from google.cloud import compute_v1
    import re
    
    # Default to the monitoring project if parsing fails
    target_project = project_id
    target_zone = None
    instance_name = resource_name
    
    # Regex to extract project, zone, and instance
    # Supports:
    # - projects/{project}/zones/{zone}/instances/{instance}
    # - //compute.googleapis.com/projects/{project}/zones/{zone}/instances/{instance}
    match = re.search(r'projects/([^/]+)/zones/([^/]+)/instances/([^/]+)', resource_name)
    
    if match:
        target_project = match.group(1)
        target_zone = match.group(2)
        instance_name = match.group(3)
    else:
        # Fallback: try to find just project and instance if zone is missing
        proj_match = re.search(r'projects/([^/]+)', resource_name)
        if proj_match:
            target_project = proj_match.group(1)
            
        if '/' in instance_name and not match:
             instance_name = resource_name.split('/')[-1]

    logger.info(f"Fetching GCE details for: Project={target_project}, Zone={target_zone}, Instance={instance_name}")

    try:
        client = compute_v1.InstancesClient()
        
        if target_zone:
            try:
                instance = client.get(project=target_project, zone=target_zone, instance=instance_name)
                return _calculate_disk_size(instance)
            except Exception as e:
                logger.warning(f"Failed to get instance {instance_name} in zone {target_zone}: {e}")
                # Fallback to aggregated list if direct get fails
                pass

        # Use AggregatedList if zone is unknown or direct get failed
        logger.info(f"Attempting AggregatedList for {instance_name} in project {target_project}")
        request = compute_v1.AggregatedListInstancesRequest(project=target_project)
        request.filter = f"name = {instance_name}"
        
        for zone, response in client.aggregated_list(request=request):
            if response.instances:
                for instance in response.instances:
                    if instance.name == instance_name:
                        return _calculate_disk_size(instance)
        
        return 0
            
    except Exception as e:
        logger.warning(f"Error fetching GCE details for {instance_name} in {target_project}: {e}")
        return 0

    return total_gb

def fetch_gce_disk_details(project_id, resource_name):
    """
    Fetches disk details from GCE.
    """
    from google.cloud import compute_v1
    import re
    
    target_project = project_id
    target_zone = None
    disk_name = resource_name
    
    # Regex for disk
    # projects/{project}/zones/{zone}/disks/{disk}
    match = re.search(r'projects/([^/]+)/zones/([^/]+)/disks/([^/]+)', resource_name)
    
    if match:
        target_project = match.group(1)
        target_zone = match.group(2)
        disk_name = match.group(3)
    else:
         # Fallback logic if needed
         pass
         
    if not target_zone:
        logger.warning(f"Zone not found for disk {resource_name}, cannot fetch details.")
        return 0

    try:
        client = compute_v1.DisksClient()
        disk = client.get(project=target_project, zone=target_zone, disk=disk_name)
        return disk.size_gb
    except Exception as e:
        logger.warning(f"Failed to get disk {disk_name} in zone {target_zone}: {e}")
        return 0

def fetch_cloudsql_details(project_id, resource_name):
    """
    Fetches CloudSQL instance details.
    """
    from googleapiclient import discovery
    import re
    
    target_project = project_id
    instance_name = resource_name
    
    # Regex for CloudSQL
    # projects/{project}/instances/{instance}
    match = re.search(r'projects/([^/]+)/instances/([^/]+)', resource_name)
    
    if match:
        target_project = match.group(1)
        instance_name = match.group(2)
    
    try:
        service = discovery.build('sqladmin', 'v1beta4', cache_discovery=False)
        request = service.instances().get(project=target_project, instance=instance_name)
        response = request.execute()
        
        # dataDiskSizeGb is in settings
        if 'settings' in response and 'dataDiskSizeGb' in response['settings']:
            return int(response['settings']['dataDiskSizeGb'])
        
        return 0
    except Exception as e:
        logger.warning(f"Failed to get CloudSQL instance {instance_name}: {e}")
        return 0
