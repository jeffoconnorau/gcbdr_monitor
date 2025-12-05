# GCBDR Monitor

GCBDR Monitor is a Python-based service designed to monitor Google Cloud Backup and DR (GCBDR) jobs. It analyzes backup logs to identify anomalies in change rates, helping to detect potential issues or unexpected data growth.

## Features

- **Log Analysis**: Queries Cloud Logging for GCBDR backup job logs using structured filters.
- **Enhanced Reporting**: Provides detailed statistics for each protected resource, including:
    - Average Daily Change Rate (GB and %)
    - Current Daily Change Rate (GB and %)
    - Growth Rate % (Current vs Historical)
    - Total Resource Size (GiB)
- **Anomaly Detection**: Compares current job statistics against historical averages to identify outliers.
- **Cloud Run Ready**: Designed to be deployed as a Cloud Run service.
- **API Endpoint**: Exposes a simple HTTP endpoint to trigger analysis.

## Prerequisites

- Python 3.9+
- Google Cloud Project with:
    - Cloud Logging enabled
    - Permissions to read logs (`roles/logging.viewer` or similar)

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
    curl "http://localhost:8080/?days=7"
    ```
    - `days`: (Optional) Number of days of history to analyze (default: 7).

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
      --region us-central1 \
      --allow-unauthenticated
    ```

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
