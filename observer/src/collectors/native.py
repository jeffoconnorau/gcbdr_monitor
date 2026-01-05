from typing import List
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

    def collect(self) -> List[Metric]:
        if not self.client:
            return []

        metrics = []
        try:
            # Calculate time window (last POLL_INTERVAL_SECONDS + buffer)
            # using UTC to match Cloud Logging
            now = datetime.now(timezone.utc)
            seconds = Config.POLL_INTERVAL_SECONDS + 10
            time_filter = now - timedelta(seconds=seconds)
            timestamp_str = time_filter.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            
            # Construct Query
            # We look for successful/failed backup jobs. 
            # Adjust filter based on actual log structure of GCBDR.
            # This is a broad filter to catch backupdr related logs.
            filter_str = (
                f'timestamp >= "{timestamp_str}" AND '
                f'(resource.type="backupdr.googleapis.com/BackupVault" OR '
                f' resource.type="backupdr.googleapis.com/ManagementServer" OR '
                f' logName:"gcb_backup_recovery_jobs" OR '
                f' protoPayload.serviceName="backupdr.googleapis.com")'
            )

            # Efficiently iterate over pages
            entries = self.client.list_entries(filter_=filter_str, page_size=100)
            
            for entry in entries:
                # Extract useful info. This heavily depends on the actual log schema.
                # We'll try to extract operation name and status.
                
                payload = entry.payload if isinstance(entry.payload, dict) else {}
                proto_payload = entry.payload if hasattr(entry, 'payload') and isinstance(entry.payload, dict) else {}
                # If it's an audit log, payload is in proto_payload usually? 
                # Actually list_entries returns StructEntry or ProtoEntry.
                # For simplicity, we check common fields.
                
                job_id = "unknown"
                status = "unknown"
                job_type = "unknown"
                
                # Try to parse Audit Logs
                if hasattr(entry, 'payload_pb') or (isinstance(entry.payload, dict) and '@type' in entry.payload):
                    # It's difficult to parse proto directly without correct classes, 
                    # but python client presumes dict for JSON payloads.
                    pass

                # Basic heuristic extraction
                if 'methodName' in payload:
                    job_type = payload['methodName']
                elif hasattr(entry, 'msg'): # TextEntry
                     job_type = "log_entry"
                
                # Attempt to find status
                if entry.severity:
                    status = entry.severity
                
                # Create a metric event
                # We use the log timestamp
                ts = entry.timestamp.timestamp() if entry.timestamp else now.timestamp()
                
                metrics.append(Metric(
                    name="gcbdr_log_event",
                    tags={
                        "project": self.project_id,
                        "severity": str(status),
                        "type": str(job_type)
                    },
                    fields={
                        "message": str(payload) or str(entry.payload)
                    },
                    timestamp=ts
                ))
                
        except Exception as e:
            self.logger.error(f"Error collecting logs: {e}")

        return metrics
