from typing import List
import requests
import time
import urllib3
import google.auth
from google.auth.transport.requests import Request
from .base import BaseCollector, Metric
from config import Config

# Suppress InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class MgmtConsoleCollector(BaseCollector):
    def __init__(self):
        super().__init__("mgmt_console_collector")
        self.endpoint = Config.MGMT_CONSOLE_ENDPOINT
        self.creds = None
        
        if not self.endpoint:
            self.logger.warning("Management Console Endpoint not configured. Collector will be idle.")
        else:
            try:
                # Use Application Default Credentials
                # Scopes: We might need specific scopes, but default often works for internal APIs
                # or 'https://www.googleapis.com/auth/cloud-platform'
                credentials, project_id = google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
                self.creds = credentials
                self.logger.info(f"Loaded IAM credentials for project: {project_id}")
            except Exception as e:
                self.logger.error(f"Failed to load IAM credentials: {e}")

    def _get_token(self):
        if not self.creds:
            return None
        
        try:
            # Refresh if expired
            if not self.creds.valid:
                self.creds.refresh(Request())
            return self.creds.token
        except Exception as e:
            self.logger.error(f"Failed to refresh token: {e}")
            return None

    def _parse_job_time(self, job: dict) -> float:
        """Parse job completion time from various possible fields."""
        try:
            # Fields in order of preference
            time_fields = ['ended', 'completed', 'updated', 'created']
            for field in time_fields:
                val = job.get(field)
                if val:
                    # Actifio API typically returns milliseconds or ISO string
                    # If int/float, assume milliseconds (common in Java APIs) or seconds
                    if isinstance(val, (int, float)):
                         # Heuristic: if > 3e9 (year 2065), it's probably millis; else seconds
                         # Current time in millis is ~1.7e12
                        return float(val) / 1000.0 if val > 1_000_000_000_000 else float(val)
                    # TODO: Add string parsing if needed (e.g. ISO 8601)
            return None
        except Exception:
            return None

    def collect(self) -> List[Metric]:
        if not self.endpoint:
            return []

        token = self._get_token()
        if not token:
            self.logger.warning("No valid IAM token available. Skipping collection.")
            return []

        metrics = []
        try:
            # API Base is typically /actifio/api
            url = f"{self.endpoint}/actifio/api/jobstatus"
            headers = {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json"
            }
            
            resp = requests.get(url, headers=headers, verify=False, timeout=30)
            
            if resp.status_code == 200:
                jobs = resp.json()
                if isinstance(jobs, list):
                    for job in jobs:
                        job_id = job.get('id', 'unknown')
                        status = job.get('status', 'unknown')
                        job_type = job.get('jobtype', 'unknown')
                        
                        metrics.append(Metric(
                            name="mgmt_console_job",
                            tags={
                                "job_id": str(job_id),
                                "status": status,
                                "type": job_type,
                                "resource_type": "mgmt_console_resource", # Fallback for compatibility
                                "source_resource": job.get('source', job.get('hostname', 'mgmt_console_unknown')),
                                "source": "mgmt_console"
                            },
                            fields={
                                "duration": int(job.get('duration', 0)),
                                "size_bytes": int(job.get('bytes', 0))
                            },
                            # Use ended time for timestamp if available, otherwise current time
                            timestamp=self._parse_job_time(job) or time.time()
                        ))
            else:
                self.logger.error(f"Failed to fetch jobs from {url}: {resp.status_code} - {resp.text}")
                    
        except Exception as e:
            self.logger.error(f"Error collecting from Management Console: {e}")

        return metrics
