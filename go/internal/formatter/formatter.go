// Package formatter provides output formatting for analysis results.
package formatter

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"html/template"
	"strings"

	"github.com/jeffoconnorau/gcbdr_monitor/go/internal/analyzer"
)

// FormatJSON formats the result as JSON.
func FormatJSON(result *analyzer.AnalysisResult) ([]byte, error) {
	return json.MarshalIndent(result, "", "  ")
}

// FormatCSV formats the result as CSV.
func FormatCSV(result *analyzer.AnalysisResult) ([]byte, error) {
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)

	// Header
	w.Write([]string{"Resource Name", "Type", "Source", "Total Size (GiB)", "Daily Change (GB)", "Daily Change (%)", "Job Count"})

	// Combine stats
	allStats := append(result.VaultWorkloads.ResourceStats, result.ApplianceWorkloads.ResourceStats...)

	for _, r := range allStats {
		w.Write([]string{
			r.ResourceName,
			r.ResourceType,
			r.JobSource,
			fmt.Sprintf("%.2f", r.TotalResourceSizeGB),
			fmt.Sprintf("%.2f", r.CurrentDailyChangeGB),
			fmt.Sprintf("%.2f", r.CurrentDailyChangePct),
			fmt.Sprintf("%d", r.BackupJobCount),
		})
	}

	// Anomalies section
	if len(result.Anomalies) > 0 {
		w.Write([]string{})
		w.Write([]string{"ANOMALIES DETECTED"})
		w.Write([]string{"Job ID", "Resource", "Date", "Time", "Change (GB)", "Avg (GB)", "Duration (s)", "Avg Duration (s)", "Reasons"})

		for _, a := range result.Anomalies {
			w.Write([]string{
				a.JobID,
				a.Resource,
				a.Date,
				a.Time,
				fmt.Sprintf("%.2f", a.GiBTransferred),
				fmt.Sprintf("%.2f", a.AvgGiB),
				fmt.Sprintf("%.0f", a.DurationSeconds),
				fmt.Sprintf("%.1f", a.AvgDurationSeconds),
				strings.Join(a.Reasons, ", "),
			})
		}
	}

	// Daily Baselines Section
	if len(result.DailyBaselines) > 0 {
		w.Write([]string{})
		w.Write([]string{"DAILY BASELINE METRICS"})
		w.Write([]string{"Date", "Modified (GB)", "New (GB)", "Deleted (GB)", "Suspicious (GB)", "Total Protected (GB)", "Resources"})

		for _, b := range result.DailyBaselines {
			w.Write([]string{
				b.Date,
				fmt.Sprintf("%.2f", b.ModifiedDataGB),
				fmt.Sprintf("%.2f", b.NewDataGB),
				fmt.Sprintf("%.2f", b.DeletedDataGB),
				fmt.Sprintf("%.2f", b.SuspiciousDataGB),
				fmt.Sprintf("%.2f", b.TotalProtectedGB),
				fmt.Sprintf("%d", b.ResourceCount),
			})
		}
	}

	w.Flush()
	return buf.Bytes(), w.Error()
}

const htmlTemplate = `<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>GCBDR Monitor Report</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 20px; background: #1a1a2e; color: #eee; }
        h1, h2 { color: #00d9ff; }
        table { border-collapse: collapse; width: 100%; margin: 20px 0; }
        th, td { padding: 12px; text-align: left; border: 1px solid #333; }
        th { background: #16213e; color: #00d9ff; }
        tr:nth-child(even) { background: #0f0f23; }
        tr:hover { background: #1f4068; }
        .anomaly { background: #4a1a1a !important; }
        .summary { display: flex; flex-wrap: wrap; gap: 20px; margin-bottom: 20px; }
        .stat-card { background: #16213e; padding: 20px; border-radius: 8px; min-width: 150px; }
        .stat-card h3 { margin: 0; color: #00d9ff; font-size: 2em; }
        .stat-card p { margin: 5px 0 0; color: #888; }
        .warning { background: #5a4a1a; color: #ffd700; padding: 15px; border: 1px solid #b8860b; border-radius: 5px; margin-bottom: 20px; }
    </style>
</head>
<body>
    <h1>GCBDR Monitor Report</h1>
    
    {{if gt .Summary.ZeroSizeVaultCount 0}}
    <div class="warning">
        <strong>⚠️ Potential Permission Issue Detected</strong><br>
        {{.Summary.ZeroSizeVaultCount}} out of {{.Summary.TotalVaultResourceCount}} Vault resources are showing 0GB size.<br>
        Please check that the service account has <code>Compute Viewer</code> or <code>Cloud SQL Viewer</code> permissions.
    </div>
    {{end}}

    <div class="summary">
        <div class="stat-card">
            <h3>{{.Summary.TotalJobs}}</h3>
            <p>Total Jobs</p>
        </div>
        <div class="stat-card">
            <h3 style="color: #4CAF50;">{{.Summary.SuccessfulJobs}}</h3>
            <p>Successful</p>
        </div>
        <div class="stat-card">
            <h3 style="color: #f44336;">{{.Summary.FailedJobs}}</h3>
            <p>Failed</p>
        </div>
        <div class="stat-card">
            <h3 style="color: #ff9800;">{{.Summary.AnomalyCount}}</h3>
            <p>Anomalies</p>
        </div>
        <div class="stat-card">
            <h3>{{printf "%.2f" .Summary.TotalResourceSizeGB}}</h3>
            <p>Total Size (GiB)</p>
        </div>
        <div class="stat-card">
            <h3>{{printf "%.2f" .Summary.CurrentDailyChangeGB}}</h3>
            <p>Daily Change (GB) ({{printf "%.2f" .Summary.CurrentDailyChangePct}}%)</p>
        </div>
    </div>

    {{if .Anomalies}}
    <h2>⚠️ Anomalies Detected</h2>
    <table>
        <tr><th>Job ID</th><th>Resource</th><th>Date/Time</th><th>Change (GB)</th><th>Avg (GB)</th><th>Duration (s)</th><th>Reasons</th></tr>
        {{range .Anomalies}}
        <tr class="anomaly">
            <td>{{.JobID}}</td>
            <td>{{.Resource}}</td>
            <td>{{.Date}} {{.Time}}</td>
            <td>{{printf "%.2f" .GiBTransferred}}</td>
            <td>{{printf "%.2f" .AvgGiB}}</td>
            <td>{{printf "%.0f" .DurationSeconds}}</td>
            <td>{{range .Reasons}}{{.}}<br>{{end}}</td>
        </tr>
        {{end}}
    </table>
    {{end}}

    <h2>Resource Statistics</h2>
    <table>
        <tr><th>Resource Name</th><th>Type</th><th>Source</th><th>Total Size (GiB)</th><th>Daily Change (GB)</th><th>Daily Change (%)</th><th>Job Count</th></tr>
        {{range .AllStats}}
        <tr>
            <td>{{.ResourceName}}</td>
            <td>{{.ResourceType}}</td>
            <td>{{.JobSource}}</td>
            <td>{{printf "%.2f" .TotalResourceSizeGB}}</td>
            <td>{{printf "%.2f" .CurrentDailyChangeGB}}</td>
            <td>{{printf "%.2f" .CurrentDailyChangePct}}</td>
            <td>{{.BackupJobCount}}</td>
        </tr>
        {{end}}
    </table>

    
    {{if .DailyBaselines}}
    <h2>Daily Baseline Metrics</h2>
    <table>
        <tr>
            <th>Date</th>
            <th>Modified (GB)</th>
            <th>New (GB)</th>
            <th>Deleted (GB)</th>
            <th>Suspicious (GB)</th>
            <th>Total Protected (GB)</th>
            <th>Resources</th>
        </tr>
        {{range .DailyBaselines}}
        <tr>
            <td>{{.Date}}</td>
            <td>{{printf "%.2f" .ModifiedDataGB}}</td>
            <td style="color: {{if .NewDataGB}}#4CAF50{{else}}inherit{{end}};">{{printf "%.2f" .NewDataGB}}</td>
            <td style="color: {{if .DeletedDataGB}}#f44336{{else}}inherit{{end}};">{{printf "%.2f" .DeletedDataGB}}</td>
            <td style="color: {{if .SuspiciousDataGB}}#ff9800{{else}}inherit{{end}};">{{printf "%.2f" .SuspiciousDataGB}}</td>
            <td>{{printf "%.2f" .TotalProtectedGB}}</td>
            <td>
                {{.ResourceCount}}
                {{if .NewResourceCount}}<span style="color:#4CAF50;">(+{{.NewResourceCount}})</span>{{end}}
                {{if .DeletedResourceCount}}<span style="color:#f44336;">(-{{.DeletedResourceCount}})</span>{{end}}
            </td>
        </tr>
        {{end}}
    </table>
    {{end}}
</body>
</html>`

// HTMLData is the data structure for HTML template.
type HTMLData struct {
	Summary        analyzer.Summary
	Anomalies      []analyzer.Anomaly
	DailyBaselines []analyzer.DailyBaseline
	AllStats       []analyzer.ResourceStats
}

// FormatHTML formats the result as HTML.
func FormatHTML(result *analyzer.AnalysisResult) ([]byte, error) {
	tmpl, err := template.New("report").Parse(htmlTemplate)
	if err != nil {
		return nil, fmt.Errorf("template parse error: %w", err)
	}

	allStats := append(result.VaultWorkloads.ResourceStats, result.ApplianceWorkloads.ResourceStats...)

	data := HTMLData{
		Summary:        result.Summary,
		Anomalies:      result.Anomalies,
		DailyBaselines: result.DailyBaselines,
		AllStats:       allStats,
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return nil, fmt.Errorf("template execute error: %w", err)
	}

	return buf.Bytes(), nil
}
