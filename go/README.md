# GCBDR Monitor - Go Implementation

This is a Go port of the Python-based GCBDR Monitor. It provides the same core functionality for analyzing Google Cloud Backup and DR jobs and detecting anomalies.

## Features

- **Log Fetching**: Queries Cloud Logging for Vault and Appliance backup jobs
- **Anomaly Detection**: Z-score based detection for size spikes, drop-offs, and duration anomalies
- **Notifications**: Google Chat (webhook), Email (SMTP), Pub/Sub
- **Output Formats**: JSON, CSV, HTML

## Getting Started

### Prerequisites

- Go 1.21+
- Google Cloud Project with logging access

### Build

```bash
cd go
go mod tidy
go build -o gcbdr-monitor ./cmd/gcbdr-monitor/
```

### Run

```bash
export GOOGLE_CLOUD_PROJECT="your-project-id"
./gcbdr-monitor
```

Server starts at `http://localhost:8080`.

### API

```bash
# Analyze last 7 days, JSON output
curl "http://localhost:8080/?days=7"

# Filter by name, HTML output
curl "http://localhost:8080/?days=7&filter_name=*sql*&format=html"

# Disable notifications
curl "http://localhost:8080/?days=7&notify=false"
```

**Parameters**:
- `days`: History depth (default: 7)
- `filter_name`: Resource name filter (wildcards supported)
- `source_type`: `all` (default), `vault`, `appliance`
- `format`: `json` (default), `csv`, `html`
- `notify`: `true` (default), `false`

### Notifications

Configure via environment variables:

| Variable | Description |
|----------|-------------|
| `GOOGLE_CHAT_WEBHOOK` | Google Chat webhook URL |
| `SMTP_HOST` | SMTP server hostname |
| `SMTP_PORT` | SMTP port (default: 587) |
| `SMTP_USER` | SMTP username |
| `SMTP_PASSWORD` | SMTP password |
| `EMAIL_SENDER` | Sender email address |
| `EMAIL_RECIPIENTS` | Comma-separated recipient emails |
| `PUBSUB_TOPIC` | Pub/Sub topic name |

## Project Structure

```
go/
├── cmd/gcbdr-monitor/main.go    # HTTP server
├── internal/
│   ├── analyzer/analyzer.go     # Log fetching, parsing, detection
│   ├── notifier/notifier.go     # Chat, Email, Pub/Sub
│   └── formatter/formatter.go   # JSON, CSV, HTML output
├── go.mod
└── README.md
```
