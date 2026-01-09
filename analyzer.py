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
    import statistics
    
    stats = {}
    for job in job_history:
        resource = job['resource_name']
        if resource not in stats:
            stats[resource] = {
                'total_bytes': 0, 
                'count': 0, 
                'total_size_sum': 0,
                'resource_type': job.get('resourceType', 'UNKNOWN'),
                'timestamps': [],
                'bytes_values': [],
                'duration_values': []
            }
        
        stats[resource]['total_bytes'] += job['bytes_transferred']
        stats[resource]['total_size_sum'] += job.get('total_resource_size_bytes', 0)
        stats[resource]['count'] += 1
        stats[resource]['timestamps'].append(job['timestamp'])
        stats[resource]['bytes_values'].append(job['bytes_transferred'])
        stats[resource]['duration_values'].append(job.get('duration_seconds', 0))
    
    results = {}
    for resource, data in stats.items():
        if data['count'] > 0:
            avg_bytes = data['total_bytes'] / data['count']
            avg_total_size = data['total_size_sum'] / data['count']
            
            # Calculate StDev
            stdev_bytes = 0
            if data['count'] > 1:
                stdev_bytes = statistics.stdev(data['bytes_values'])
            
            # Calculate Duration stats
            avg_duration = 0
            stdev_duration = 0
            if data['duration_values']:
                avg_duration = sum(data['duration_values']) / len(data['duration_values'])
                if len(data['duration_values']) > 1:
                    stdev_duration = statistics.stdev(data['duration_values'])
            
            avg_daily_change_pct = 0
            if avg_total_size > 0:
                avg_daily_change_pct = (avg_bytes / avg_total_size) * 100
                
            results[resource] = {
                'avg_bytes': avg_bytes,
                'stdev_bytes': stdev_bytes,
                'avg_daily_change_gb': avg_bytes / (1024 * 1024 * 1024),
                'avg_daily_change_pct': avg_daily_change_pct,
                'avg_total_size_gb': avg_total_size / (1024 * 1024 * 1024),
                'resource_type': data['resource_type'],
                'data_points': data['count'],
                'avg_duration': avg_duration,
                'stdev_duration': stdev_duration
            }
    return results

def detect_anomalies(current_jobs, stats, z_score_threshold=3.0, drop_off_threshold=0.1):
    """
    Identifies jobs that are statistical outliers.
    Checks:
    1. Size Z-Score > 3.0
    2. Size Drop-off < 10% of average (if average > 1GB to avoid noise)
    3. Duration Z-Score > 3.0
    """
    from datetime import datetime, timezone, timedelta
    anomalies = []
    for job in current_jobs:
        resource = job['resource_name']
        if resource in stats:
            s = stats[resource]
            avg_bytes = s['avg_bytes']
            stdev_bytes = s['stdev_bytes']
            avg_duration = s['avg_duration']
            stdev_duration = s['stdev_duration']
            
            bytes_transferred = job['bytes_transferred']
            duration = job.get('duration_seconds', 0)
            
            reasons = []
            
            # 1. Size Z-Score (High)
            # Only if we have variance (stdev > 0)
            if stdev_bytes > 0:
                z_score_size = (bytes_transferred - avg_bytes) / stdev_bytes
                if z_score_size > z_score_threshold:
                    reasons.append(f"Size Spike (Z={z_score_size:.1f})")
            elif avg_bytes > 0 and bytes_transferred > (avg_bytes * 1.5):
                # Fallback to simple factor if no variance history yet (e.g. only 1 previous job)
                reasons.append(f"Size Spike (Factor={bytes_transferred/avg_bytes:.1f}x)")

            # 2. Size Drop-off (Low)
            # Only check if average is significant (> 100MB) to avoid noise on tiny files
            if avg_bytes > 100 * 1024 * 1024:
                if bytes_transferred < (avg_bytes * drop_off_threshold):
                    reasons.append(f"Size Drop-off ({bytes_transferred/avg_bytes*100:.1f}% of avg)")

            # 3. Duration Z-Score (High)
            if stdev_duration > 0:
                z_score_duration = (duration - avg_duration) / stdev_duration
                if z_score_duration > z_score_threshold:
                    reasons.append(f"Duration Spike (Z={z_score_duration:.1f})")
            
            if reasons:
                # Format timestamp
                ts = job['timestamp']
                if isinstance(ts, str):
                    try:
                        ts = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                    except ValueError:
                        pass
                
                date_str = ts.strftime('%Y-%m-%d') if isinstance(ts, datetime) else 'unknown'
                time_str = ts.strftime('%H:%M:%S UTC') if isinstance(ts, datetime) else 'unknown'

                anomalies.append({
                    'job_id': job['jobId'],
                    'resource': resource,
                    'resource_type': job.get('resourceType', 'UNKNOWN'),
                    'total_resource_size_gb': round(job.get('total_resource_size_bytes', 0) / (1024**3), 2),
                    'date': date_str,
                    'time': time_str,
                    'bytes': bytes_transferred,
                    'avg_bytes': avg_bytes,
                    'gib_transferred': round(bytes_transferred / (1024**3), 4),
                    'avg_gib': round(avg_bytes / (1024**3), 4),
                    'duration_seconds': duration,
                    'avg_duration_seconds': avg_duration,
                    'reasons': ", ".join(reasons)
                })
    return anomalies

def matches_filter(name, pattern):
    """
    Checks if name matches the pattern.
    Case-insensitive.
    Supports wildcards via fnmatch.
    If no wildcards, does a substring check.
    """
    if not pattern:
        return True
    if not name:
        return False
        
    import fnmatch
    
    name = name.lower()
    pattern = pattern.lower()
    
    if any(c in pattern for c in ['*', '?', '[']):
        return fnmatch.fnmatch(name, pattern)
    else:
        return pattern in name

def analyze_backup_jobs(project_id, days=7, filter_name=None, source_type='all'):
    """
    Main orchestration function.
    source_type: 'all', 'vault', or 'appliance'
    """
    # Validate source_type
    if source_type not in ('all', 'vault', 'appliance'):
        logger.warning(f"Invalid source_type '{source_type}', defaulting to 'all'")
        source_type = 'all'

    # 1. Fetch logs
    # 1. Fetch logs
    vault_logs = []
    appliance_logs = []
    
    # Only fetch what we need
    if source_type in ('all', 'vault'):
        vault_logs = fetch_backup_logs(project_id, days)
    
    if source_type in ('all', 'appliance'):
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
    
    if not successful_jobs and not failed_jobs and not other_jobs:
        logger.info("No jobs found for analysis.")
        return {
            "status": "no_data",
            "total_jobs": 0,
            "successful_count": 0,
            "failed_count": 0,
            "other_count": 0
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
            resource_type_lower = resource_type.lower()
            try:
                # GCE Instances
                if 'gce' in resource_type_lower or 'compute' in resource_type_lower or 'vm' in resource_type_lower:
                    # Note: 'vm' might be broad but GCBDR usually labels on-prem as generic 'VM' or 'VMware'
                    # We'll try GCE lookup. If it fails (not found), it just returns 0.
                    gce_size = 0
                    if res in gce_cache:
                        gce_size = gce_cache[res]
                    else:
                        gce_size = fetch_gce_instance_details(project_id, res)
                        gce_cache[res] = gce_size
                    
                    if gce_size > 0:
                        total_resource_size_gb = gce_size
                        
                # Persistent Disks
                elif 'disk' in resource_type_lower:
                     disk_size = fetch_gce_disk_details(project_id, res)
                     if disk_size > 0:
                         total_resource_size_gb = disk_size

                # Cloud SQL
                elif 'sql' in resource_type_lower:
                    sql_size = fetch_cloudsql_details(project_id, res)
                    if sql_size > 0:
                        total_resource_size_gb = sql_size
            except Exception as e:
                logger.warning(f"Failed to fetch details for {res} ({resource_type}): {e}")
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
    
    # APPLY FILTERING HERE
    if filter_name:
        logger.info(f"Filtering results with pattern: {filter_name}")
        resource_stats_list = [r for r in resource_stats_list if matches_filter(r['resource_name'], filter_name)]
        anomalies = [a for a in anomalies if matches_filter(a['resource'], filter_name)]
    
    # Separate stats for clearer reporting if needed, but 'resource_stats' contains all.
    # We can add a flag or type to resource_stats to distinguish.
    # I added 'job_source' above.
    
    # Split resource stats
    vault_resource_stats = [r for r in resource_stats_list if r['job_source'] == 'vault']
    appliance_resource_stats = [r for r in resource_stats_list if r['job_source'] == 'appliance']
    
    # Calculate counts for vault
    # We need to filter the jobs lists too if we want the counts to match the filtered resources?
    # The user asked to "search for a specific resource... return in the results only".
    # So the counts should probably reflect the filtered view or we should clarify.
    # Usually "results" implies the list of resources. The summary counts might be confusing if they show totals but the list is empty.
    # Let's filter the jobs lists as well for consistency in the summary.
    
    if filter_name:
        unique_vault_jobs = _filter_jobs(unique_vault_jobs, filter_name)
        unique_appliance_jobs = _filter_jobs(unique_appliance_jobs, filter_name)
        
    all_unique_jobs = unique_vault_jobs + unique_appliance_jobs
    successful_jobs = [j for j in all_unique_jobs if j['status'] == 'SUCCESSFUL']
    failed_jobs = [j for j in all_unique_jobs if j['status'] == 'FAILED']

    # Recalculate Aggregates (filtered)
    agg_total_size_gb = sum(r['total_resource_size_gb'] for r in resource_stats_list)
    agg_daily_change_gb = sum(r['current_daily_change_gb'] for r in resource_stats_list)
    agg_daily_change_pct = 0
    if agg_total_size_gb > 0:
        agg_daily_change_pct = (agg_daily_change_gb / agg_total_size_gb) * 100

    logger.info(f"Analysis complete. Found {len(anomalies)} anomalies.")

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
            "total_jobs": len(unique_vault_jobs),
            "successful_jobs": len([j for j in unique_vault_jobs if j['status'] == 'SUCCESSFUL']),
            "failed_jobs": len([j for j in unique_vault_jobs if j['status'] == 'FAILED']),
            "resource_stats": vault_resource_stats
        },
        "appliance_workloads": {
            "total_jobs": len(unique_appliance_jobs),
            "successful_jobs": len([j for j in unique_appliance_jobs if j['status'] == 'SUCCESSFUL']),
            "failed_jobs": len([j for j in unique_appliance_jobs if j['status'] == 'FAILED']),
            "resource_stats": appliance_resource_stats
        },
        "anomalies": anomalies
    }

def _filter_jobs(jobs, pattern):
    """Filters a list of jobs based on a name pattern."""
    if not pattern:
        return jobs
    return [j for j in jobs if matches_filter(j.get('resource_name'), pattern)]

def _get_resource_details(project_id, resource_name, resource_type, cache):
    """Fetches resource details from GCE or CloudSQL if size is missing."""
    if resource_name in cache:
        return cache[resource_name]

    size_gb = 0
    resource_type_lower = resource_type.lower()
    try:
        if 'gce' in resource_type_lower or 'compute' in resource_type_lower or 'vm' in resource_type_lower:
            size_gb = fetch_gce_instance_details(project_id, resource_name)
        elif 'disk' in resource_type_lower:
            size_gb = fetch_gce_disk_details(project_id, resource_name)
        elif 'sql' in resource_type_lower:
            size_gb = fetch_cloudsql_details(project_id, resource_name)
    except Exception as e:
        logger.warning(f"Failed to fetch live details for {resource_name}: {e}")

    cache[resource_name] = size_gb
    return size_gb

def _calculate_resource_stats(period_stats, project_id, unique_vault_jobs):
    """Calculates and supplements statistics for each resource."""
    resource_stats_list = []
    gce_cache = {}
    
    for res, p_data in period_stats.items():
        total_resource_size_gb = p_data.get('avg_total_size_gb', 0)
        resource_type = p_data.get('resource_type', 'UNKNOWN')

        if total_resource_size_gb == 0:
            total_resource_size_gb = _get_resource_details(project_id, res, resource_type, gce_cache)
        
        current_daily_change_gb = p_data.get('avg_daily_change_gb', 0)
        current_daily_change_pct = (current_daily_change_gb / total_resource_size_gb) * 100 if total_resource_size_gb > 0 else 0

        resource_stats_list.append({
            "resource_name": res,
            "resource_type": resource_type,
            "total_resource_size_gb": round(total_resource_size_gb, 2),
            "current_daily_change_gb": round(current_daily_change_gb, 4),
            "current_daily_change_pct": round(current_daily_change_pct, 2),
            "backup_job_count": p_data.get('data_points', 0),
            "job_source": "vault" if res in {j['resource_name'] for j in unique_vault_jobs} else "appliance"
        })
    return resource_stats_list


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

def _calculate_disk_size(instance):
    total_gb = 0
    # Boot disk and attached disks
    if instance.disks:
        for disk in instance.disks:
            total_gb += disk.disk_size_gb
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
    else:
        # Fallback: assume resource_name is just the instance name if it contains no slashes
        if '/' not in resource_name:
             instance_name = resource_name
        else:
             # Try to handle //cloudsql.googleapis.com/ prefix or just project/instance
             parts = resource_name.split('/')
             if 'instances' in parts:
                 try:
                     idx = parts.index('instances')
                     if idx + 1 < len(parts):
                         instance_name = parts[idx+1]
                     if idx - 1 >= 0 and parts[idx-1] != 'zones' and parts[idx-1] != 'projects':
                         # Sometimes project is not immediately before, scanning...
                         pass
                     # Try to find project
                     if 'projects' in parts:
                         p_idx = parts.index('projects')
                         if p_idx + 1 < len(parts):
                             target_project = parts[p_idx+1]
                 except ValueError:
                     pass

    logger.info(f"Fetching CloudSQL details for: Project={target_project}, Instance={instance_name}")

    try:
        service = discovery.build('sqladmin', 'v1', cache_discovery=False)
        request = service.instances().get(project=target_project, instance=instance_name)
        response = request.execute()
        
        # Check settings for dataDiskSizeGb
        if 'settings' in response:
            settings = response['settings']
            if 'dataDiskSizeGb' in settings:
                return int(settings['dataDiskSizeGb'])
            else:
                logger.warning(f"CloudSQL 'dataDiskSizeGb' missing from settings for {instance_name}. Available keys: {list(settings.keys())}")
        else:
            logger.warning(f"CloudSQL 'settings' missing for {instance_name}. Response keys: {list(response.keys())}")
        
        return 0
    except Exception as e:
        logger.warning(f"Failed to get CloudSQL instance {instance_name}: {e}")
        return 0
