// GCBDR Monitor - Go Implementation
// HTTP server for backup job analysis and anomaly detection.
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/jeffoconnorau/gcbdr_monitor/go/internal/analyzer"
	"github.com/jeffoconnorau/gcbdr_monitor/go/internal/formatter"
	"github.com/jeffoconnorau/gcbdr_monitor/go/internal/notifier"
)

const Version = "2.0.0"

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	http.HandleFunc("/", handleAnalysis)
	http.HandleFunc("/health", handleHealth)

	log.Printf("GCBDR Monitor v%s starting on port %s", Version, port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "OK")
}

func handleAnalysis(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()

	// Parse parameters
	days := 7
	if d := r.URL.Query().Get("days"); d != "" {
		if parsed, err := strconv.Atoi(d); err == nil {
			days = parsed
		}
	}

	filterName := r.URL.Query().Get("filter_name")
	sourceType := r.URL.Query().Get("source_type")
	if sourceType == "" {
		sourceType = "all"
	}

	outputFormat := strings.ToLower(r.URL.Query().Get("format"))
	if outputFormat == "" {
		outputFormat = "json"
	}

	shouldNotify := r.URL.Query().Get("notify") != "false"

	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if projectID == "" {
		http.Error(w, "GOOGLE_CLOUD_PROJECT environment variable not set", http.StatusInternalServerError)
		return
	}

	log.Printf("Starting GCBDR analysis v%s for project %s with %d days history", Version, projectID, days)

	// Create analyzer
	a, err := analyzer.New(projectID, days)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to create analyzer: %v", err), http.StatusInternalServerError)
		return
	}
	defer a.Close()

	// Run analysis
	result, err := a.Analyze(ctx, filterName, sourceType)
	if err != nil {
		http.Error(w, fmt.Sprintf("Analysis error: %v", err), http.StatusInternalServerError)
		return
	}

	// Send notifications if anomalies found
	if len(result.Anomalies) > 0 && shouldNotify {
		log.Printf("Sending notifications for %d anomalies...", len(result.Anomalies))
		nm := notifier.NewManager(projectID)
		nm.SendNotifications(result.Anomalies)
	}

	// Format output
	var output []byte
	var contentType string

	switch outputFormat {
	case "csv":
		output, err = formatter.FormatCSV(result)
		contentType = "text/csv"
	case "html":
		output, err = formatter.FormatHTML(result)
		contentType = "text/html; charset=utf-8"
	default:
		output, err = formatter.FormatJSON(result)
		contentType = "application/json"
	}

	if err != nil {
		http.Error(w, fmt.Sprintf("Format error: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", contentType)
	w.Write(output)
}
