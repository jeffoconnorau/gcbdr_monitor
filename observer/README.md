# GCBDR Observer

The GCBDR Observer is a dedicated sidecar module that provides long-term visual monitoring and historical analysis using **InfluxDB** and **Grafana**. Unlike the stateless API, the Observer maintains a persistent history (default 90 days) to track trends and anomalies over time.

## Features
- **Unified Dashboard**: View Native GCBDR and Management Console jobs in a single pane.
- **Historical Trends**: Track job duration, data size, and failure rates over 90 days.
- **Anomaly Detection**: Visual panels highlighting jobs behaving strangely (Duration > 2 stddev, etc.).
- **Top Talkers**: Identify which resources are churning the most data.
- **Job Restoration Tracking**: Dedicated tracking for `RESTORE` and `RECOVERY` activities.

## Quick Start (Docker)

The Observer is designed to run locally or on a VM using Docker Compose.

1.  **Navigate to the observer directory:**
    ```bash
    cd observer
    ```

2.  **Configure Credentials:**
    - Ensure you have a Google Cloud Service Account Key (JSON) with `roles/logging.viewer`.
    - Save it as `gcp_creds.json` in the `observer/` directory.

3.  **Start the Stack:**
    ```bash
    docker compose up --build -d
    ```
    This starts the Python Collector, InfluxDB, and Grafana.

4.  **Access the Dashboard:**
    - **Grafana**: [http://localhost:3000](http://localhost:3000)
    - **Credentials**: `admin` / `admin` (default)
    - **Data Source**: Pre-configured to talk to the local InfluxDB.

## Dashboard Architecture
- **Collector (`src/`)**: A Python service that polls:
    - **Native GCBDR**: Via Google Cloud Logging API (`bdr_backup_recovery_jobs`).
    - **Management Console**: Via Actifio API (`/api/v1/jobs`).
- **Grafana**: Visualizes the data with pre-built dashboards (JSON provisioned).

## Deploying to Cloud Run (Collector Only)

You can deploy the **Collector** component to Google Cloud Run, but it requires an external InfluxDB to write to (e.g., InfluxDB Cloud or a VM-hosted instance).

1.  **Build the Container:**
    ```bash
    gcloud builds submit --tag gcr.io/your-project-id/gcbdr-observer
    ```

2.  **Deploy to Cloud Run:**
    ```bash
    gcloud run deploy gcbdr-observer \
      --image gcr.io/your-project-id/gcbdr-observer \
      --platform managed \
      --region your-region \
      --set-env-vars="INFLUXDB_URL=https://us-east-1-1.aws.cloud2.influxdata.com,INFLUXDB_TOKEN=your-token,INFLUXDB_ORG=your-org,INFLUXDB_BUCKET=gcbdr,GOOGLE_CLOUD_PROJECT=your-project-id"
    ```

    **Note**: You must set the `INFLUXDB_*` environment variables to point to your persistent InfluxDB instance. The local `docker-compose` setup runs InfluxDB as a sidecar, which is not supported in standard Cloud Run services without externalizing the state.
