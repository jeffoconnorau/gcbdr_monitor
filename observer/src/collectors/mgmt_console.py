from typing import List, Dict, Any
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
        self.endpoint = Config.MGMT_CONSOLE_ENDPOINT.rstrip('/') if Config.MGMT_CONSOLE_ENDPOINT else None
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

    def _parse_job_time(self, job: Dict[str, Any]) -> float:
        """Parse job timestamp from various potential fields, handling microseconds."""
        # Priority: enddate -> startdate -> queuedate
        candidates = [
            ('enddate', job.get('enddate')),
            ('startdate', job.get('startdate')),
            ('queuedate', job.get('queuedate')),
            ('ended', job.get('ended')),
            ('completed', job.get('completed'))
        ]
        
        for name, ts in candidates:
            if ts:
                try:
                    ts_float = float(ts)
                    # Heuristic: If > 1e11 (roughly year 2286 in seconds), it's likely microseconds
                    # Current time in seconds is ~1.7e9, in milliseconds ~1.7e12, in microseconds ~1.7e15
                    if ts_float > 1e11 and ts_float > time.time() * 100000: # Ensure it's not just a large millisecond value
                        self.logger.debug(f"Parsed job time '{name}' as microseconds: {ts_float}. Converted to seconds: {ts_float / 1_000_000.0}")
                        return ts_float / 1_000_000.0
                    else:
                        self.logger.debug(f"Parsed job time '{name}' as seconds/milliseconds: {ts_float}. Using as is.")
                        # If it's milliseconds, it will be handled by the timestamp=self._parse_job_time(job) or time.time()
                        # which expects seconds. The original code had a heuristic for milliseconds.
                        # Let's re-incorporate that for the final return if it's not microseconds.
                        if ts_float > 1_000_000_000_000: # If > 1 trillion, likely milliseconds
                            self.logger.debug(f"Detected milliseconds for '{name}': {ts_float}. Converted to seconds: {ts_float / 1000.0}")
                            return ts_float / 1000.0
                        return ts_float
                except (ValueError, TypeError):
                    self.logger.debug(f"Could not convert candidate '{name}' with value '{ts}' to float for job {job.get('id')}.")
                    continue
        
        self.logger.warning(f"Could not parse timestamp for job {job.get('id')}. Candidates checked: {[(name, val) for name, val in candidates if val is not None]}")
        return None

    def _get_session_id(self, token):
        """Exchange IAM token for Actifio Session ID."""
        try:
            url = f"{self.endpoint}/actifio/session"
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Length": "0"
            }
            resp = requests.post(url, headers=headers, verify=False, timeout=10)
            if resp.status_code == 200:
                return resp.json().get('session_id')
            else:
                self.logger.error(f"Failed to create session: {resp.status_code} - {resp.text}")
                return None
        except Exception as e:
            self.logger.error(f"Session creation error: {e}")
            return None

    def collect(self) -> List[Metric]:
        if not self.endpoint:
            return []

        token = self._get_token()
        if not token:
            self.logger.warning("No valid IAM token available. Skipping collection.")
            return []

        # 1. Get Session ID
        session_id = self._get_session_id(token)
        if not session_id:
            return []

        metrics = []
        try:
            # 2. Fetch Jobs using Session ID and correct path (no /api/)
            url = f"{self.endpoint}/actifio/jobstatus"
            headers = {
                "Authorization": f"Bearer {token}",
                "backupdr-management-session": f"Actifio {session_id}",
                "Accept": "application/json"
            }
            
            resp = requests.get(url, headers=headers, verify=False, timeout=30)
            
            if resp.status_code == 200:
                data = resp.json()
                # Actifio API often returns { "items": [...] } wrapper
                jobs = data.get('items', []) if isinstance(data, dict) else data
                
                if isinstance(jobs, list):
                    for job in jobs:
                        job_id = job.get('id', 'unknown')
                        status = str(job.get('status', 'unknown')).lower()
                        job_type = str(job.get('jobtype', 'unknown')).lower()
                        job_name = job.get('jobname', 'unknown')
                        
                        metrics.append(Metric(
                            name="mgmt_console_job",
                            tags={
                                "job_id": str(job_id),
                                "status": status,
                                "type": job_type,
                                "resource_type": job.get('apptype', 'mgmt_console_resource'),
                                "source_resource": job.get('appname', 'mgmt_console_unknown'),
                                "source": "mgmt_console",
                                "project_id": "mgmt-console"
                            },
                            fields={
                                "duration": int(job.get('duration', 0) or 0),
                                "size_bytes": int(job.get('bytes', 0) or 0),
                                "job_name": job_name
                            },
                            # Use ended time for timestamp if available, otherwise current time
                            timestamp=self._parse_job_time(job) or time.time()
                        ))
            else:
                self.logger.error(f"Failed to fetch jobs from {url}: {resp.status_code} - {resp.text}")
                    
        except Exception as e:
            self.logger.error(f"Error collecting from Management Console: {e}")

        return metrics
