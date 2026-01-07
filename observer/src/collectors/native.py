from typing import List, Dict, Any
from datetime import datetime, timedelta, timezone
from google.cloud import logging as cloud_logging
from .base import BaseCollector, Metric
from config import Config

class NativeGCBDRCollector(BaseCollector):
    def __init__(self):
        super().__init__("native_gcbdr_collector")
        self.project_id = Config.GOOGLE_CLOUD_PROJECT
        self.client = None
        
        if self.project_id:
            try:
                self.client = cloud_logging.Client(project=self.project_id)
                self.logger.info(f"Initialized Cloud Logging client for project: {self.project_id}")
            except Exception as e:
                self.logger.error(f"Failed to initialize Cloud Logging client: {e}")
        else:
            self.logger.warning("Google Cloud Project ID not configured. Collector will be idle.")

    def _parse_job_payload(self, payload: Dict[str, Any], resource_type_str: str = None) -> Dict[str, Any]:
        """
        Extracts relevant data from a log entry's jsonPayload.
        Based on analyzer.py logic.
        """
        if not payload:
            return {}

        data = {}
        
        # 1. Total Size
        total_size_bytes = 0
        if payload.get('sourceResourceSizeBytes'):
            total_size_bytes = int(payload.get('sourceResourceSizeBytes'))
        elif payload.get('usedStorageGib'):
            total_size_bytes = int(float(payload.get('usedStorageGib')) * 1024 * 1024 * 1024)
        elif payload.get('sourceResourceDataSizeGib'):
            total_size_bytes = int(float(payload.get('sourceResourceDataSizeGib')) * 1024 * 1024 * 1024)
            
        # Check nested protectedResourceDetails if still 0
        if total_size_bytes == 0:
            protected_details = payload.get('protectedResourceDetails', {})
            if protected_details.get('sourceResourceSizeBytes'):
                total_size_bytes = int(protected_details.get('sourceResourceSizeBytes'))
            elif protected_details.get('usedStorageGib'):
                total_size_bytes = int(float(protected_details.get('usedStorageGib')) * 1024 * 1024 * 1024)

        data['total_resource_size_bytes'] = total_size_bytes

        # 2. Bytes Transferred (Incremental)
        inc_size_gib = float(payload.get('incrementalBackupSizeGib', 0))
        data['bytes_transferred'] = int(inc_size_gib * 1024 * 1024 * 1024)
        data['size_bytes'] = data['bytes_transferred'] # Alias for compatibility

        # 3. Duration
        start_time = payload.get('startTime')
        end_time = payload.get('endTime')
        duration = 0
        end = None
        if start_time and end_time:
            try:
                # Handle ISO formatting with Z
                start = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                end = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
                duration = int((end - start).total_seconds())
            except Exception:
                pass
        data['duration'] = duration
        data['endTime'] = end # Store datetime object or timestamp
        
        # 4. Other Metadata
        data['jobId'] = payload.get('jobId', 'unknown')
        data['jobStatus'] = payload.get('jobStatus', 'unknown')
        data['jobCategory'] = payload.get('jobCategory', 'unknown')
        
        # Robust Resource Type Extraction
        r_type = payload.get('resourceType')
        if not r_type:
            r_type = payload.get('protectedResourceDetails', {}).get('resourceType')
        
        # Fallback to sourceResourceName suffix or Cloud Logging resource type
        if not r_type and resource_type_str == "backupdr.googleapis.com/BackupDRProject":
             # Try to deduce from appName or sourceResourceName if available
             # e.g. "projects/p/locations/l/clusters/c/instances/i" -> "AlloyDB"
             src_name = payload.get('sourceResourceName', '')
             if 'alloydb' in src_name:
                 r_type = "AlloyDB"
             elif 'compute' in src_name:
                 r_type = "GCE"
             else:
                 r_type = "BackupDRProject" # Generic fallback

        data['resourceType'] = r_type if r_type else 'unknown'
        data['sourceResourceName'] = payload.get('sourceResourceName', 'unknown')

        return data

    def _parse_appliance_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extracts relevant data from appliance log entries (eventId 44003).
        """
        if not payload:
            return {}

        data = {}
        
        # 1. Bytes Transferred
        bytes_transferred = 0
        if payload.get('dataCopiedInBytes'):
            bytes_transferred = int(payload.get('dataCopiedInBytes'))
        elif payload.get('bytesWritten'):
            bytes_transferred = int(payload.get('bytesWritten'))
        elif payload.get('transferSize'):
            bytes_transferred = int(payload.get('transferSize'))
        
        data['bytes_transferred'] = bytes_transferred
        data['size_bytes'] = bytes_transferred # Alias

        # 2. Total Size
        total_size_bytes = 0
        if payload.get('sourceSize'):
            total_size_bytes = int(payload.get('sourceSize'))
        elif payload.get('appSize'):
            total_size_bytes = int(payload.get('appSize'))
            
        data['total_resource_size_bytes'] = total_size_bytes
        
        # 3. Duration (Appliance often has duration or eventTime)
        # 44003 is instant usually, but lets check for duration
        data['duration'] = int(payload.get('duration', 0))

        # 4. Other Metadata
        data['jobId'] = payload.get('jobName') or payload.get('srcid') or 'unknown_job'
        data['jobStatus'] = 'SUCCESSFUL' # 44003 implies success
        data['jobCategory'] = 'ApplianceBackup'
        data['resourceType'] = payload.get('appType', 'ApplianceWorkload')
        data['sourceResourceName'] = payload.get('appName', 'unknown_app')

        return data

    def collect(self) -> List[Metric]:
        if not self.client:
            return []

        metrics = []
        try:
            # Determine time window
            now = datetime.now(timezone.utc)
            
            # If this is the first run and INITIAL_HISTORY_MINUTES is set, use it
            if not hasattr(self, '_has_run_once'):
                lookback_minutes = Config.INITIAL_HISTORY_MINUTES
                if lookback_minutes > 0:
                    self.logger.info(f"First run: Fetching last {lookback_minutes} minutes of history.")
                    time_filter = now - timedelta(minutes=lookback_minutes)
                else:
                    time_filter = now - timedelta(seconds=Config.POLL_INTERVAL_SECONDS + 10)
                self._has_run_once = True
            else:
                 time_filter = now - timedelta(seconds=Config.POLL_INTERVAL_SECONDS + 10)

            timestamp_str = time_filter.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            
            # Construct Filter
            # Combined filter for Native Vault jobs and Appliance events
            # Added 'bdr_backup_restore_jobs' which was missing
            # Added 'BackupDRProject' to catch cross-project/AlloyDB jobs
            filter_str = (
                f'timestamp >= "{timestamp_str}" AND '
                f'('
                f' (resource.type="backupdr.googleapis.com/BackupVault") OR '
                f' (resource.type="backupdr.googleapis.com/ManagementServer") OR '
                f' (resource.type="backupdr.googleapis.com/BackupDRProject") OR '
                f' (logName:"bdr_backup_recovery_jobs") OR '
                f' (logName:"bdr_backup_restore_jobs") OR '
                f' (logName:"gcb_backup_recovery_jobs") OR '
                f' (logName:"backup_recovery_appliance_events" AND jsonPayload.eventId="44003")'
                f') AND '
                f'NOT jsonPayload.jobStatus="RUNNING"'
            )

            entries = self.client.list_entries(filter_=filter_str, page_size=100)
            
            for entry in entries:
                payload = entry.payload if isinstance(entry.payload, dict) else {}
                
                # Determine parser based on log name or content
                parsed_data = {}
                log_name = entry.log_name or ""
                
                if "backup_recovery_appliance_events" in log_name:
                    parsed_data = self._parse_appliance_payload(payload)
                else:
                    # Default to native vault/job parser
                    parsed_data = self._parse_job_payload(payload)
                
                # Determine effective Job ID for uniqueness
                # If payload has no jobId, use insertId (or timestamp hash) as fallback
                job_id = parsed_data.get('jobId')
                if not job_id or job_id == 'unknown':
                    if entry.insert_id:
                        job_id = entry.insert_id
                    else:
                        # Fallback to timestamp if even insert_id is missing (rare)
                        job_id = f"job_{int(entry.timestamp.timestamp())}"
                
                status = parsed_data.get('jobStatus', 'unknown')
                
                # If we still have unknown status but it's a severity error, mark it
                if status == 'unknown' and entry.severity == 'ERROR':
                    status = 'FAILED'
                
                # Skip irrelevant logs ONLY if we really can't identify them AND status is unknown
                # The user's SQL relies on logs that might not have explicit jobIds but have Status
                if job_id == 'unknown' and status == 'unknown':
                    continue

                # Use job endTime if available for best accuracy, else log timestamp
                if parsed_data.get('endTime'):
                     try:
                        # endTime is already parsed in _parse_job_payload but we didn't store the raw object
                        # Let's re-parse or store it in _parse_job_payload. 
                        # actually _parse_job_payload doesn't return endTime object, it calculates duration.
                        # Let's trust entry.timestamp for now as it's usually very close to endTime for native logs
                        # But wait, looking at line 58 of _parse_job_payload, it gets endTime string.
                        pass
                     except:
                        pass

                # Use job endTime if available for best accuracy, else log timestamp
                if parsed_data.get('endTime') and isinstance(parsed_data['endTime'], datetime):
                    ts = parsed_data['endTime'].timestamp()
                else:
                    ts = entry.timestamp.timestamp() if entry.timestamp else now.timestamp()
                
                metrics.append(Metric(
                    name="gcbdr_log_event",
                    tags={
                        "project": self.project_id,
                        "job_id": str(job_id),
                        "status": str(status),
                        "type": str(parsed_data.get('jobCategory', 'unknown')),
                        "resource_type": str(parsed_data.get('resourceType', 'unknown')),
                        "source_resource": str(parsed_data.get('sourceResourceName', 'unknown'))
                    },
                    fields={
                        "bytes_transferred": int(parsed_data.get('bytes_transferred', 0)),
                        "size_bytes": int(parsed_data.get('size_bytes', 0)),
                        "total_resource_size_bytes": int(parsed_data.get('total_resource_size_bytes', 0)),
                        "duration": int(parsed_data.get('duration', 0)),
                        "message": str(payload)
                    },
                    timestamp=ts
                ))
                
        except Exception as e:
            self.logger.error(f"Error collecting logs: {e}")

        return metrics
