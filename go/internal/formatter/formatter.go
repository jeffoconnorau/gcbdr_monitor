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
        .summary { display: flex; gap: 20px; margin-bottom: 20px; }
        .stat-card { background: #16213e; padding: 20px; border-radius: 8px; min-width: 150px; }
        .stat-card h3 { margin: 0; color: #00d9ff; font-size: 2em; }
        .stat-card p { margin: 5px 0 0; color: #888; }
    </style>
</head>
<body>
    <h1>GCBDR Monitor Report</h1>
    
    <div class="summary">
        <div class="stat-card">
            <h3>{{.Summary.TotalVaultJobs}}</h3>
            <p>Vault Jobs</p>
        </div>
        <div class="stat-card">
            <h3>{{.Summary.TotalApplianceJobs}}</h3>
            <p>Appliance Jobs</p>
        </div>
        <div class="stat-card">
            <h3>{{.Summary.AnomalyCount}}</h3>
            <p>Anomalies</p>
        </div>
    </div>

    {{if .Anomalies}}
    <h2>⚠️ Anomalies</h2>
    <table>
        <tr><th>Resource</th><th>Job ID</th><th>Date/Time</th><th>Transferred</th><th>Average</th><th>Reasons</th></tr>
        {{range .Anomalies}}
        <tr class="anomaly">
            <td>{{.Resource}}</td>
            <td>{{.JobID}}</td>
            <td>{{.Date}} {{.Time}}</td>
            <td>{{printf "%.2f" .GiBTransferred}} GiB</td>
            <td>{{printf "%.2f" .AvgGiB}} GiB</td>
            <td>{{range .Reasons}}{{.}}<br>{{end}}</td>
        </tr>
        {{end}}
    </table>
    {{end}}

    <h2>Resource Statistics</h2>
    <table>
        <tr><th>Resource</th><th>Type</th><th>Source</th><th>Daily Change (GB)</th><th>Job Count</th></tr>
        {{range .AllStats}}
        <tr>
            <td>{{.ResourceName}}</td>
            <td>{{.ResourceType}}</td>
            <td>{{.JobSource}}</td>
            <td>{{printf "%.2f" .CurrentDailyChangeGB}}</td>
            <td>{{.BackupJobCount}}</td>
        </tr>
        {{end}}
    </table>
</body>
</html>`

// HTMLData is the data structure for HTML template.
type HTMLData struct {
	Summary  analyzer.Summary
	Anomalies []analyzer.Anomaly
	AllStats []analyzer.ResourceStats
}

// FormatHTML formats the result as HTML.
func FormatHTML(result *analyzer.AnalysisResult) ([]byte, error) {
	tmpl, err := template.New("report").Parse(htmlTemplate)
	if err != nil {
		return nil, fmt.Errorf("template parse error: %w", err)
	}

	allStats := append(result.VaultWorkloads.ResourceStats, result.ApplianceWorkloads.ResourceStats...)

	data := HTMLData{
		Summary:   result.Summary,
		Anomalies: result.Anomalies,
		AllStats:  allStats,
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return nil, fmt.Errorf("template execute error: %w", err)
	}

	return buf.Bytes(), nil
}
