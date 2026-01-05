import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # General
    POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))
    MOCK_MODE = os.getenv("GCBDR_MOCK_MODE", "false").lower() == "true"
    SINGLE_RUN = os.getenv("SINGLE_RUN", "false").lower() == "true"

    # InfluxDB
    INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://localhost:8086")
    INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "my-super-secret-auth-token")
    INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "my-org")
    INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "gcbdr")

    # Management Console
    MGMT_CONSOLE_ENDPOINT = os.getenv("MGMT_CONSOLE_ENDPOINT", "")
    # Auth is now handled via IAM (Application Default Credentials)

    # Native GCBDR (Google Cloud)
    GOOGLE_CLOUD_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT", "")
    GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")

    @classmethod
    def validate(cls):
        # Add validation logic if needed
        pass
