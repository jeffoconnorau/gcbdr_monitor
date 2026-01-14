# GCBDR Monitor

GCBDR Monitor calculates change rates and detects anomalies in Google Cloud Backup and DR (GCBDR) jobs. It helps ensure **data integrity**, monitor **performance issues**, and control **costs** by flagging unexpected behavior in your backup workloads.

## Features

- **Unified Monitoring**: Tracks both Vault and Appliance workloads.
- **Smart Enrichment**: Correlates appliance logs with job data to fill missing size metrics.
- **Advanced Anomaly Detection**:
  - **Data Integrity**: Detects suspiciously small backups (e.g., empty source, configuration failure).
  - **Performance**: Flags stalled or unusually slow jobs (Duration Spikes).
  - **Cost Control**: Alerts on unexpected data growth (Size Spikes).
- **Detailed Metrics**: Reports Daily Change Rate (GB & %), Total Resource Size (GiB), and Job Counts.
- **Alerting**: Notifications via Google Chat, Email, and Pub/Sub.
- **Cloud Ready**: Deployable as a Cloud Run service with a simple HTTP API.

## Getting Started

### Prerequisites

- **Python 3.9+**
- **Google Cloud Project** with:
  - `roles/logging.viewer` (Read logs)
  - `roles/compute.viewer` (Fetch disk sizes)
  - **Cloud SQL Admin API** enabled (Fetch SQL instance sizes)

### Installation

1.  **Authenticate Locally** (if not using Cloud Shell or Service Account):
    ```bash
    gcloud auth application-default login
    ```

2.  **Clone and Install**:
    ```bash
    git clone <repository-url>
    cd gcbdr_monitor
    python3 -m venv venv && source venv/bin/activate
    pip install -r requirements.txt
    ```

3.  **Run Locally**:
    ```bash
    export GOOGLE_CLOUD_PROJECT="your-project-id"
    python main.py
    ```
    Server starts at `http://0.0.0.0:8080`.

4.  **Trigger Analysis**:
    ```bash
    # Analyze last 7 days for SQL resources, output as HTML
    curl "http://localhost:8080/?days=7&filter_name=*sql*&format=html"
    ```
    **Parameters**:
    - `days`: History depth (default: 7).
    - `filter_name`: Resource name filter (wildcards supported).
    - `source_type`: `all` (default), `vault`, `appliance`.
    - `format`: `json` (default), `csv`, `html`.

## Metrics Explained

- **Total Resource Size (GiB)**: Full size of the protected asset (from logs or API).
- **Daily Change Rate (GB)**: Average daily incremental change.
- **Change Rate (%)**: Relative change against total size.
- **Anomalies**: Automatically flagged events (e.g., "Size Spike (Z=4.2)").

## Advanced Usage

### Notifications
Configure via environment variables:
- **Google Chat**: Set `GOOGLE_CHAT_WEBHOOK` (Space ID or URL).
  > **Note**: For local testing with a user account, use the full **Webhook URL** (Option 2). The "Space ID" method requires service account permissions (`chat.bot` scope) often unavailable to user accounts.
- **Email**: Set `SMTP_HOST`, `SMTP_USER`, `SMTP_PASSWORD`, `EMAIL_SENDER`, `EMAIL_RECIPIENTS`.
- **Pub/Sub**: Set `PUBSUB_TOPIC`.

### Cloud Run Deployment
```bash
gcloud builds submit --tag gcr.io/your-project/gcbdr-monitor
gcloud run deploy gcbdr-monitor --image gcr.io/your-project/gcbdr-monitor --platform managed --allow-unauthenticated
```

### Log Inspection Tool
Use `scripts/inspect_logs.py` to debug raw data:
```bash
python scripts/inspect_logs.py --type vault      # Check Vault logs
python scripts/inspect_logs.py --type appliance  # Check Appliance logs
```

## Observer Module
For long-term trends and visual dashboards, use the **Observer Module**.
See [observer/README.md](observer/README.md) for setup instructions.

## Troubleshooting

### Common Issues
- **"No data found"**:
  - Verify `GOOGLE_CLOUD_PROJECT` is set correctly.
  - Ensure backups exist within the requested `days` range.
- **Permission Errors**:
  - Service Account needs `roles/logging.viewer` and `roles/compute.viewer`.
  - **Local Development**: Run `gcloud auth application-default login` to authenticate with your user credentials. Ensure your user has the necessary IAM roles on the project.

### Debugging
If report data seems incorrect, use the log inspector to verify raw availability:
```bash
# Check if any Vault logs exist
python scripts/inspect_logs.py --type vault
```

## License
Apache License 2.0 - see [LICENSE](LICENSE).
