from typing import List
import requests
import time
import google.auth
from google.auth.transport.requests import Request
from .base import BaseCollector, Metric
from config import Config

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

    def collect(self) -> List[Metric]:
        if not self.endpoint:
            return []

        token = self._get_token()
        if not token:
            self.logger.warning("No valid IAM token available. Skipping collection.")
            return []

        metrics = []
        try:
            url = f"{self.endpoint}/jobstatus"
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
                                "source": "mgmt_console"
                            },
                            fields={
                                "duration": int(job.get('duration', 0)),
                                "size_bytes": int(job.get('bytes', 0))
                            },
                            timestamp=time.time() 
                        ))
            else:
                self.logger.error(f"Failed to fetch jobs: {resp.status_code} - {resp.text}")
                    
        except Exception as e:
            self.logger.error(f"Error collecting from Management Console: {e}")

        return metrics
