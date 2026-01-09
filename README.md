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

### Code Structure

- **main.py**: The main Flask application file. It handles routing and request handling.
- **analyzer.py**: The core logic of the application. It fetches, parses, and analyzes the backup logs.
- **formatters.py**: Contains functions to format the analysis results into CSV.
- **notifier.py**: Manages sending notifications via Google Chat, Email, and Pub/Sub.
- **templates/report.html**: Jinja2 template for the HTML report.

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

You can also suppress notifications for a specific run by adding `&notify=false` to the URL.

### Deploying to Cloud Run

1.  **Build the Container:**
    ```bash
    gcloud builds submit --tag gcr.io/your-project-id/gcbdr-monitor
    ```

2.  **Deploy:**
    ```bash
    gcloud run deploy gcbdr-monitor \
      --image gcr.io/your-project-id/gcbdr-monitor \
      --platform managed \
      --region your-region \
      --allow-unauthenticated
    ```

### Observer Module (Visual Dashboard)

The **Observer Module** provides a comprehensive visual dashboard using Grafana to track long-term trends and historical data.
For detailed setup instructions, see [observer/README.md](observer/README.md).

**Dashboard File**: `observer/dashboards/grafana_gcbdr_dashboard.json`

**Key Panels:**
- **Job Statistics**: High-level counters for Native vs. Management Console jobs (Backup/Restore counts, Daily Job Average).
- **Data Volume**: Time-series view of data volume (GB) processed by backup jobs.
- **Top 10 Anomalies**: A list of the most significant outliers in terms of duration or size, sorted by deviation.
- **Detailed Job Information**: Segmented view of jobs by Source (Native/Mgmt) and Status (Success/Fail/Warning).
- **Restore Analysis**: Stacked bar charts showing restore activity over the last 7 days.

### Notifications

The tool can alert you via Google Chat or Email when anomalies are detected.

#### Configuration
Set the following environment variables to enable notifications:

**Google Chat:**
- `GOOGLE_CHAT_WEBHOOK`: 
    - **Option 1 (Space ID)**: Just the Space ID (e.g., `AAAA...`) or Resource Name (`spaces/AAAA...`). Requires the Service Account to have permissions in the Google Chat Space.
    - **Option 2 (Webhook)**: The full Webhook URL (e.g., `https://chat.googleapis.com/v1/spaces/...`).

**Email:**
- `SMTP_HOST`: Hostname of the SMTP server (e.g., `smtp.gmail.com`).
- `SMTP_PORT`: Port (default: 587).
- `SMTP_USER`: SMTP Username/Email.
- `SMTP_PASSWORD`: SMTP Password (or App Password).
- `EMAIL_SENDER`: Email address to send from.
- `EMAIL_RECIPIENTS`: Comma-separated list of recipient emails.

**Pub/Sub:**
- `PUBSUB_TOPIC`: The full topic name (e.g., `projects/your-project/topics/your-topic`).





## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
