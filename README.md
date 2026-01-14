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

### Authentication

The application uses **Application Default Credentials (ADC)**. Choose one of the following methods:

#### Option 1: Local Development (User Account)
```bash
# Login with your Google account
gcloud auth application-default login

# Set your project
export GOOGLE_CLOUD_PROJECT="your-project-id"
```

#### Option 2: Service Account (Production / Cloud Run)
```bash
# Create a service account
gcloud iam service-accounts create gcbdr-monitor \
    --display-name="GCBDR Monitor Service Account"

# Grant required roles
gcloud projects add-iam-policy-binding your-project-id \
    --member="serviceAccount:gcbdr-monitor@your-project-id.iam.gserviceaccount.com" \
    --role="roles/logging.viewer"

gcloud projects add-iam-policy-binding your-project-id \
    --member="serviceAccount:gcbdr-monitor@your-project-id.iam.gserviceaccount.com" \
    --role="roles/compute.viewer"

# For local testing with a service account key (optional)
gcloud iam service-accounts keys create key.json \
    --iam-account=gcbdr-monitor@your-project-id.iam.gserviceaccount.com
export GOOGLE_APPLICATION_CREDENTIALS="key.json"
```

> **Note**: On Cloud Run, the service automatically uses the attached service account—no key file needed.

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

### Authentication Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `google.auth.exceptions.DefaultCredentialsError` | No credentials configured | Run `gcloud auth application-default login` or set `GOOGLE_APPLICATION_CREDENTIALS` |
| `403 Forbidden` / `Permission denied` | Missing IAM roles | Grant `roles/logging.viewer` and `roles/compute.viewer` to user/service account |
| `Could not automatically determine credentials` | ADC not set up | Run `gcloud auth application-default login` |
| `Request had insufficient authentication scopes` | Token missing required scopes | Re-run `gcloud auth application-default login` or use a service account |

### Common Issues
- **"No data found"**:
  - Verify `GOOGLE_CLOUD_PROJECT` is set correctly.
  - Ensure backups exist within the requested `days` range.
  - Check that your account has access to the project's logs.
- **Permission Errors**:
  - Service Account needs `roles/logging.viewer` and `roles/compute.viewer`.
  - **Local Development**: Run `gcloud auth application-default login` to authenticate with your user credentials. Ensure your user has the necessary IAM roles on the project.
  - For cross-project monitoring, grant roles in each source project.

### Debugging
If report data seems incorrect, use the log inspector to verify raw availability:
```bash
# Check if any Vault logs exist
python scripts/inspect_logs.py --type vault
```

## License
Apache License 2.0 - see [LICENSE](LICENSE).
