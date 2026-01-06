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

    def _parse_job_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
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

        # 3. Other Metadata
        data['jobId'] = payload.get('jobId', 'unknown')
        data['jobStatus'] = payload.get('jobStatus', 'unknown')
        data['jobCategory'] = payload.get('jobCategory', 'unknown')
        data['resourceType'] = payload.get('resourceType', 'unknown')
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

        # 2. Total Size
        total_size_bytes = 0
        if payload.get('sourceSize'):
            total_size_bytes = int(payload.get('sourceSize'))
        elif payload.get('appSize'):
            total_size_bytes = int(payload.get('appSize'))
            
        data['total_resource_size_bytes'] = total_size_bytes

        # 3. Other Metadata
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
            # Calculate time window (last POLL_INTERVAL_SECONDS + buffer)
            now = datetime.now(timezone.utc)
            seconds = Config.POLL_INTERVAL_SECONDS + 10
            time_filter = now - timedelta(seconds=seconds)
            timestamp_str = time_filter.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            
            # Construct Filter
            # Combined filter for Native Vault jobs and Appliance events
            filter_str = (
                f'timestamp >= "{timestamp_str}" AND '
                f'('
                f' (resource.type="backupdr.googleapis.com/BackupVault") OR '
                f' (resource.type="backupdr.googleapis.com/ManagementServer") OR '
                f' (logName:"gcb_backup_recovery_jobs") OR '
                f' (logName:"backup_recovery_appliance_events" AND jsonPayload.eventId="44003")'
                f')'
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
                
                # Skip if no useful data extracted (e.g. not a job completion log)
                if not parsed_data.get('jobId') or parsed_data.get('jobId') == 'unknown':
                    # Fallback for generic logs if we want to keep them?
                    # For now, let's only emit if we have some job context
                    # Or check valid fields
                    pass

                job_id = parsed_data.get('jobId', 'unknown')
                status = parsed_data.get('jobStatus', 'unknown')
                
                # If we still have unknown status but it's a severity error, mark it
                if status == 'unknown' and entry.severity == 'ERROR':
                    status = 'FAILED'
                
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
                        "total_resource_size_bytes": int(parsed_data.get('total_resource_size_bytes', 0)),
                        "message": str(payload)
                    },
                    timestamp=ts
                ))
                
        except Exception as e:
            self.logger.error(f"Error collecting logs: {e}")

        return metrics
