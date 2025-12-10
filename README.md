# GCBDR Monitor

GCBDR Monitor is a Python-based service designed to monitor Google Cloud Backup and DR (GCBDR) jobs. It analyzes backup logs to identify anomalies in change rates, helping to detect potential issues or unexpected data growth.

## Features

- **Multi-Workload Support**: Monitors both **Backup Vault** workloads and **Management Console** (Appliance) workloads.
- **Log Enrichment**: Automatically enriches Appliance logs with missing size and transfer data by correlating with GCB Jobs logs.
- **Split Reporting**: Provides distinct analysis for Vault and Appliance workloads while maintaining an aggregate summary.
- **Enhanced Reporting**: Provides detailed statistics for each protected resource, including:
    - Current Daily Change Rate (GB and %)
    - Total Resource Size (GiB)
    - Backup Job Count
- **Anomaly Detection**: Compares current job statistics against historical averages to identify outliers.
- **Notifications**: Alerts via Google Chat, Email, and Pub/Sub.
- **Cloud Run Ready**: Designed to be deployed as a Cloud Run service.
- **API Endpoint**: Exposes a simple HTTP endpoint to trigger analysis.

## Prerequisites

- Python 3.9+
- Google Cloud Project with:
    - Cloud Logging enabled
    - Permissions to read logs (`roles/logging.viewer` or similar)
    - Permissions to view Compute Engine resources (`roles/compute.viewer`) for fetching disk sizes if missing in logs.
    - **Cloud SQL Admin API** enabled in this project (`sqladmin.googleapis.com`) to look up Cloud SQL instance sizes.

## Metrics Explained

- **Total Resource Size (GiB)**: The total size of the protected resource. If not found in backup logs, it is fetched from GCB Jobs logs (for appliances) or directly from the Compute Engine API (for GCE instances).
- **Current Daily Change Rate (GB)**: The average daily change rate calculated over the requested reporting period (default 7 days).
- **Current Daily Change Rate (%)**: (Current Daily Change GB / Total Resource Size GB) * 100.
- **Backup Job Count**: The total number of successful backup jobs for the resource in the analyzed period.

## Installation

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd gcbdr_monitor
    ```

2.  **Install dependencies:**
    It is recommended to use a virtual environment.
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```

## Usage

### Running Locally

1.  **Set Environment Variables:**
    ```bash
    export GOOGLE_CLOUD_PROJECT="your-project-id"
    ```

2.  **Start the Application:**
    ```bash
    python main.py
    ```
    The server will start on `http://0.0.0.0:8080` (or the port specified by `PORT` env var).

3.  **Trigger Analysis:**
    You can trigger the analysis by visiting the root endpoint:
    ```bash
    curl "http://localhost:8080/?days=7&filter_name=*sql*&source_type=appliance&format=html"
    ```
    - `days`: (Optional) Number of days of history to analyze (default: 7).
    - `filter_name`: (Optional) Filter resources by name. Supports wildcards (e.g., `*sql*`, `vm-?`) or case-insensitive substring search.
    - `source_type`: (Optional) Filter by backup source. Options: `all` (default), `vault`, `appliance`.
    - `format`: (Optional) Output format. Options: `json` (default), `csv`, `html`.

### Inspecting Logs

Use the `inspect_logs.py` utility to view raw log entries and verify data:

```bash
# Inspect Backup Vault logs (default)
python scripts/inspect_logs.py --type vault

# Inspect Appliance logs
python scripts/inspect_logs.py --type appliance

# Inspect GCB Jobs logs (used for enrichment)
python scripts/inspect_logs.py --type gcb_jobs
```

### Anomaly Detection

The tool automatically detects anomalies in backup jobs using advanced statistical analysis:

1.  **Size Spikes (Z-Score)**: Flags jobs where the data transferred is significantly higher than the historical average (> 3 standard deviations).
2.  **Size Drop-offs**: Flags jobs where the data transferred is suspiciously small (< 10% of the average), which might indicate an empty source or configuration issue.
3.  **Duration Spikes**: Flags jobs that take significantly longer than usual (> 3 standard deviations).

Anomalies are reported in the JSON, CSV, and HTML outputs with a `reasons` field explaining the cause (e.g., "Size Spike (Z=4.2)", "Size Drop-off").

Example Anomaly Output (JSON):
```json
{
  "job_id": "job-123",
  "resource": "web-server-1",
  "date": "2023-10-27",
  "time": "10:00:00 UTC",
  "gib_transferred": 50.5,
  "avg_gib": 10.2,
  "duration_seconds": 3600,
  "avg_duration_seconds": 600,
  "reasons": "Size Spike (Z=4.0), Duration Spike (Z=5.0)"
}
```

### Notifications

The tool can alert you via Google Chat or Email when anomalies are detected.

#### Configuration
Set the following environment variables to enable notifications:

**Google Chat:**
- `GOOGLE_CHAT_WEBHOOK`: The Webhook URL for your Google Chat space.
- `GCBDR_MONITOR_SKIP_SSL_VERIFY`: (Optional) Set to `true` to disable SSL certificate verification (useful for internal proxies with custom CAs).

**Email:**
- `SMTP_HOST`: Hostname of the SMTP server (e.g., `smtp.gmail.com`).
- `SMTP_PORT`: Port (default: 587).
- `SMTP_USER`: SMTP Username/Email.
- `SMTP_PASSWORD`: SMTP Password (or App Password).
- `EMAIL_SENDER`: Email address to send from.
- `EMAIL_RECIPIENTS`: Comma-separated list of recipient emails.

**Pub/Sub:**
- `PUBSUB_TOPIC`: The full topic name (e.g., `projects/your-project/topics/your-topic`).
  - The Cloud Run service account must have `roles/pubsub.publisher` on this topic.

You can also suppress notifications for a specific run by adding `&notify=false` to the URL.

#### Troubleshooting Notifications

**Email (SMTP) Issues:**
- **Authentication Failed (535):**
    - If using Gmail or Outlook with 2FA enabled, you **MUST** use an **App Password**, not your regular login password.
    - **Special Characters:** If your password contains special characters, ensure they are properly escaped when setting the environment variable in your shell.
        - *Bad:* `export SMTP_PASSWORD=foo!bar` (bash might interpret `!`)
        - *Good:* `export SMTP_PASSWORD='foo!bar'` (use single quotes)
- **Connection Timeout:** Check if your firewall allows outbound traffic on port 587 (or 465/25).

**Google Chat Issues:**
- **SSL Certificate Verify Failed:**
    - This often happens behind corporate proxies or firewalls that intercept SSL traffic.
    - **Fix:** Set `GCBDR_MONITOR_SKIP_SSL_VERIFY=true` to bypass verification (use with caution).

#### Troubleshooting Deployment

**Permission Errors (CLI/Cloud Build):**
- **Symptom:** `Error 403: ... permission denied` during build or push.
- **Fix:** Grant the following roles to your **Compute Engine Default Service Account** (which Cloud Build often uses by default) or your specific Cloud Build service account.
  
  Replace `[PROJECT_NUMBER]` with your Google Cloud Project Number.
  ```bash
  # 1. Grant Log Writer (allows writing build logs)
  gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT \
      --member=serviceAccount:[PROJECT_NUMBER]-compute@developer.gserviceaccount.com \
      --role=roles/logging.logWriter

  # 2. Grant Artifact Registry Admin (allows creating/pushing attributes)
  gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT \
      --member=serviceAccount:[PROJECT_NUMBER]-compute@developer.gserviceaccount.com \
      --role=roles/artifactregistry.repoAdmin
  
  # 3. Grant Create-on-Push Writer (required for first-time repo creation)
  gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT \
      --member=serviceAccount:[PROJECT_NUMBER]-compute@developer.gserviceaccount.com \
      --role=roles/artifactregistry.createOnPushWriter
  ```

**Runtime Errors (Cloud Run):**
- **Symptom:** `Internal Server Error: 403 Permission denied for all log views`
- **Fix:** The Cloud Run service account needs permission to *read* logs.
  ```bash
  gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT \
      --member=serviceAccount:[PROJECT_NUMBER]-compute@developer.gserviceaccount.com \
      --role=roles/logging.viewer
  ```

- **Symptom:** `Total Resource Size` is 0GB in Cloud Run (but works locally).
- **Fix:** The service account needs permission to query Compute Engine and Cloud SQL APIs to look up resource sizes.
  ```bash
  # Grant Compute Viewer (for VMs and Disks)
  gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT \
      --member=serviceAccount:[PROJECT_NUMBER]-compute@developer.gserviceaccount.com \
      --role=roles/compute.viewer

  # Grant Cloud SQL Viewer (for SQL Instances)
  gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT \
      --member=serviceAccount:[PROJECT_NUMBER]-compute@developer.gserviceaccount.com \
      --role=roles/cloudsql.viewer
  ```

**Cloud Run Deployment Errors:**
- **Symptom:** `Deployment failed ... Retry` or image not found errors.
- **Cause:** Often due to using a placeholder ID in the image URL (e.g., `gcr.io/your-project-id/...`).
- **Fix:** Ensure you replace `your-project-id` with your *actual* Project ID.
  ```bash
  gcloud run deploy gcbdr-monitor \
      --image gcr.io/$GOOGLE_CLOUD_PROJECT/gcbdr-monitor \
      --platform managed \
      --region asia-southeast1 \
      --allow-unauthenticated \
      --set-env-vars GOOGLE_CLOUD_PROJECT=$GOOGLE_CLOUD_PROJECT
  ```

- **Symptom:** `GOOGLE_CLOUD_PROJECT environment variable not set`
- **Fix:** You must explicitly set this variable (and others) during deployment.
```
  gcloud run services update gcbdr-monitor \
      --set-env-vars GOOGLE_CLOUD_PROJECT=$GOOGLE_CLOUD_PROJECT,GOOGLE_CHAT_WEBHOOK='https://chat.googleapis.com/v1/spaces/ABCDE_a1FG/messages?key=examplekey-extended&token=exampletoken-extended' \
      --region asia-southeast1
  ```
  *(Add other variables like `SMTP_HOST`, `SMTP_PASSWORD` etc. to this command using comma separation).*

### Alerting (Cloud Monitoring)

The application automatically logs structured JSON events when anomalies are detected. You can set up a **Log-based Alert Policy** in Google Cloud Monitoring to get notified via Email, SMS, Slack, PagerDuty, etc.

**Log Filter:**
```
jsonPayload.event="GCBDR_ANOMALY_DETECTED"
severity>=WARNING
```

**To create an alert policy via gcloud:**
1.  Verify the `alert_policy.json` file in the repository.
2.  Run the following command (replace `YOUR_CHANNEL_ID` with your actual Notification Channel ID, e.g., `projects/YOUR_PROJECT/notificationChannels/12345`).
    *   *Tip: List channels with `gcloud beta monitoring channels list`*

```bash
gcloud alpha monitoring policies create \
  --policy-from-file=alert_policy.json \
  --notification-channels="YOUR_CHANNEL_ID"
```

### Output Structure

The analysis returns a JSON object with the following structure:

```json
{
  "summary": {
    "anomalies_count": 6,
    "current_daily_change_gb": 179.5601,
    "current_daily_change_pct": 7.17,
    "failed_jobs": 3,
    "successful_jobs": 491,
    "total_jobs": 494,
    "total_resource_size_gb": 2505.24
  },
  "vault_workloads": {
    "total_jobs": 50,
    "successful_jobs": 49,
    "failed_jobs": 1,
    "resource_stats": [ ... ]
  },
  "appliance_workloads": {
    "total_jobs": 50,
    "successful_jobs": 49,
    "failed_jobs": 1,
    "resource_stats": [ ... ]
  },
  "anomalies": [
    {
      "avg_bytes": 590558002.75,
      "avg_duration_seconds": 0.0,
      "avg_gib": 0.55,
      "bytes": 42949672,
      "date": "2025-12-08",
      "duration_seconds": 0,
      "gib_transferred": 0.04,
      "job_id": "Job_19742479",
      "reasons": "Size Drop-off (7.3% of avg)",
      "resource": "FERHATDB",
      "resource_type": "Oracle",
      "time": "06:03:24 UTC",
      "total_resource_size_gb": 164.71
    },
    {
      "avg_bytes": 1836098519.0,
      "avg_duration_seconds": 0.0,
      "avg_gib": 1.71,
      "bytes": 7097433456,
      "date": "2025-12-08",
      "duration_seconds": 0,
      "gib_transferred": 6.61,
      "job_id": "Job_19729093",
      "reasons": "Size Spike (Factor=3.9x)",
      "resource": "winsql22-01",
      "resource_type": "VMBackup",
      "time": "06:39:24 UTC",
      "total_resource_size_gb": 62.12
    }
  ]
}
```

## Deploying to Cloud Run

1.  **Setup Artifact Registry & Region:**
    - We will use `asia-southeast1` (Singapore) for this deployment.
    - Ensure you have an Artifact Registry repository or use the default GCR if enabled.
    - *Tip:* You can configure your default region globally:
      ```bash
      gcloud config set run/region asia-southeast1
      ```

2.  **Build the Container:**
    ```bash
    gcloud builds submit --tag gcr.io/your-project-id/gcbdr-monitor
    ```

3.  **Deploy:**
    ```bash
    gcloud run deploy gcbdr-monitor \
      --image gcr.io/your-project-id/gcbdr-monitor \
      --platform managed \
      --region asia-southeast1 \
      --allow-unauthenticated
    ```

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
