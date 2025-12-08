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
- **Cloud Run Ready**: Designed to be deployed as a Cloud Run service.
- **API Endpoint**: Exposes a simple HTTP endpoint to trigger analysis.

## Prerequisites

- Python 3.9+
- Google Cloud Project with:
    - Cloud Logging enabled
    - Permissions to read logs (`roles/logging.viewer` or similar)
    - Permissions to view Compute Engine resources (`roles/compute.viewer`) for fetching disk sizes if missing in logs.

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
python inspect_logs.py --type vault

# Inspect Appliance logs
python inspect_logs.py --type appliance

# Inspect GCB Jobs logs (used for enrichment)
python inspect_logs.py --type gcb_jobs
```

### Output Structure

The analysis returns a JSON object with the following structure:

```json
{
  "summary": {
    "total_jobs": 100,
    "successful_jobs": 98,
    "failed_jobs": 2,
    "anomalies_count": 0,
    "total_resource_size_gb": 5000.00,
    "current_daily_change_gb": 50.0000,
    "current_daily_change_pct": 1.00
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
      "avg_bytes": 1836098519.0,
      "avg_gib": 1.71,
      "bytes": 7097433456,
      "date": "2025-12-08",
      "factor": 3.87,
      "gib_transferred": 6.61,
      "job_id": "Job_19729093",
      "resource": "winsql22-01",
      "resource_type": "VMBackup",
      "time": "06:39:24 UTC",
      "total_resource_size_gb": 62.12
    },
    {
      "avg_bytes": 3579139.3333333335,
      "avg_gib": 0.0033,
      "bytes": 21474836,
      "date": "2025-12-08",
      "factor": 6.0,
      "gib_transferred": 0.02,
      "job_id": "Job_19704041",
      "resource": "WINSQL22-02",
      "resource_type": "SqlInstance",
      "time": "02:53:15 UTC",
      "total_resource_size_gb": 200.0
    }
  ]
}
```

## Deploying to Cloud Run

1.  **Build the Container:**
    ```bash
    gcloud builds submit --tag gcr.io/your-project-id/gcbdr-monitor
    ```

2.  **Deploy:**
    ```bash
    gcloud run deploy gcbdr-monitor \
      --image gcr.io/your-project-id/gcbdr-monitor \
      --platform managed \
      --region us-central1 \
      --allow-unauthenticated
    ```

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
