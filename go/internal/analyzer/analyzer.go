// Package analyzer provides backup job analysis and anomaly detection.
package analyzer

import (
	"context"
	"fmt"
	"log"
	"math"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/logging"
	"cloud.google.com/go/logging/logadmin"
	"google.golang.org/api/iterator"
)

// JobData represents a parsed backup job.
type JobData struct {
	JobID           string
	ResourceName    string
	ResourceType    string
	Status          string
	StartTime       time.Time
	EndTime         time.Time
	GiBTransferred  float64
	DurationSeconds float64
	JobSource       string // "vault" or "appliance"
}

// ResourceStats holds aggregated statistics for a resource.
type ResourceStats struct {
	ResourceName          string  `json:"resource_name"`
	ResourceType          string  `json:"resource_type"`
	JobSource             string  `json:"job_source"`
	TotalResourceSizeGB   float64 `json:"total_resource_size_gb"`
	CurrentDailyChangeGB  float64 `json:"current_daily_change_gb"`
	CurrentDailyChangePct float64 `json:"current_daily_change_pct"`
	BackupJobCount        int     `json:"backup_job_count"`
	AvgGiB                float64 `json:"avg_gib"`
	StdDevGiB             float64 `json:"stddev_gib"`
	AvgDurationSeconds    float64 `json:"avg_duration_seconds"`
	StdDevDuration        float64 `json:"stddev_duration"`
}

// Anomaly represents a detected anomaly.
type Anomaly struct {
	JobID              string   `json:"job_id"`
	Resource           string   `json:"resource"`
	Date               string   `json:"date"`
	Time               string   `json:"time"`
	GiBTransferred     float64  `json:"gib_transferred"`
	AvgGiB             float64  `json:"avg_gib"`
	DurationSeconds    float64  `json:"duration_seconds"`
	AvgDurationSeconds float64  `json:"avg_duration_seconds"`
	Reasons            []string `json:"reasons"`
}

// AnalysisResult is the output of the analysis.
type AnalysisResult struct {
	Summary            Summary         `json:"summary"`
	VaultWorkloads     WorkloadResult  `json:"vault_workloads"`
	ApplianceWorkloads WorkloadResult  `json:"appliance_workloads"`
	Anomalies          []Anomaly       `json:"anomalies"`
}

// Summary provides high-level stats.
type Summary struct {
	TotalVaultJobs     int `json:"total_vault_jobs"`
	TotalApplianceJobs int `json:"total_appliance_jobs"`
	AnomalyCount       int `json:"anomaly_count"`
}

// WorkloadResult holds stats for a workload type.
type WorkloadResult struct {
	ResourceStats []ResourceStats `json:"resource_stats"`
}

// Analyzer performs backup job analysis.
type Analyzer struct {
	ProjectID string
	Days      int
	client    *logadmin.Client
}

// New creates a new Analyzer.
func New(projectID string, days int) (*Analyzer, error) {
	ctx := context.Background()
	client, err := logadmin.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to create logging client: %w", err)
	}
	return &Analyzer{
		ProjectID: projectID,
		Days:      days,
		client:    client,
	}, nil
}

// Close releases resources.
func (a *Analyzer) Close() error {
	return a.client.Close()
}

// Analyze performs the full analysis.
func (a *Analyzer) Analyze(ctx context.Context, filterName, sourceType string) (*AnalysisResult, error) {
	result := &AnalysisResult{}

	// Fetch and process vault logs
	if sourceType == "all" || sourceType == "vault" {
		vaultJobs, err := a.fetchAndParseVaultLogs(ctx)
		if err != nil {
			log.Printf("Warning: failed to fetch vault logs: %v", err)
		} else {
			filtered := filterJobs(vaultJobs, filterName)
			stats := calculateStatistics(filtered, a.Days)
			result.VaultWorkloads.ResourceStats = stats
			result.Summary.TotalVaultJobs = len(filtered)
			
			// Detect anomalies
			anomalies := detectAnomalies(filtered, stats)
			result.Anomalies = append(result.Anomalies, anomalies...)
		}
	}

	// Fetch and process appliance logs
	if sourceType == "all" || sourceType == "appliance" {
		applianceJobs, err := a.fetchAndParseApplianceLogs(ctx)
		if err != nil {
			log.Printf("Warning: failed to fetch appliance logs: %v", err)
		} else {
			filtered := filterJobs(applianceJobs, filterName)
			stats := calculateStatistics(filtered, a.Days)
			result.ApplianceWorkloads.ResourceStats = stats
			result.Summary.TotalApplianceJobs = len(filtered)
			
			// Detect anomalies
			anomalies := detectAnomalies(filtered, stats)
			result.Anomalies = append(result.Anomalies, anomalies...)
		}
	}

	result.Summary.AnomalyCount = len(result.Anomalies)
	return result, nil
}

func (a *Analyzer) fetchAndParseVaultLogs(ctx context.Context) ([]JobData, error) {
	filter := fmt.Sprintf(
		`logName="projects/%s/logs/backupdr.googleapis.com%%2Fbdr_backup_restore_jobs" AND timestamp >= "%s"`,
		a.ProjectID,
		time.Now().AddDate(0, 0, -a.Days).Format(time.RFC3339),
	)
	return a.fetchLogs(ctx, filter, "vault")
}

func (a *Analyzer) fetchAndParseApplianceLogs(ctx context.Context) ([]JobData, error) {
	filter := fmt.Sprintf(
		`logName="projects/%s/logs/backupdr.googleapis.com%%2Fbackup_recovery_appliance_events" AND jsonPayload.eventId=44003 AND timestamp >= "%s"`,
		a.ProjectID,
		time.Now().AddDate(0, 0, -a.Days).Format(time.RFC3339),
	)
	return a.fetchLogs(ctx, filter, "appliance")
}

func (a *Analyzer) fetchLogs(ctx context.Context, filter, source string) ([]JobData, error) {
	var jobs []JobData
	log.Printf("DEBUG: Querying logs with filter: %s", filter)
	it := a.client.Entries(ctx, logadmin.Filter(filter))
	
	for {
		entry, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to iterate logs: %w", err)
		}
		
		job := parseLogEntry(entry, source)
		if job != nil {
			jobs = append(jobs, *job)
		}
	}
	
	log.Printf("Fetched %d %s jobs", len(jobs), source)
	return jobs, nil
}

func parseLogEntry(entry *logging.Entry, source string) *JobData {
	payload, ok := entry.Payload.(map[string]interface{})
	if !ok {
		return nil
	}
	
	job := &JobData{
		JobSource: source,
		StartTime: entry.Timestamp,
	}
	
	if id, ok := payload["jobId"].(string); ok {
		job.JobID = id
	}
	if name, ok := payload["resourceName"].(string); ok {
		job.ResourceName = name
	}
	if rtype, ok := payload["resourceType"].(string); ok {
		job.ResourceType = rtype
	}
	if status, ok := payload["status"].(string); ok {
		job.Status = status
	}
	if gib, ok := payload["dataCopiedGiB"].(float64); ok {
		job.GiBTransferred = gib
	}
	if duration, ok := payload["durationSeconds"].(float64); ok {
		job.DurationSeconds = duration
	}
	
	return job
}

func filterJobs(jobs []JobData, pattern string) []JobData {
	if pattern == "" {
		return jobs
	}
	
	var filtered []JobData
	pattern = strings.ToLower(pattern)
	
	// Check if pattern contains wildcards
	hasWildcard := strings.ContainsAny(pattern, "*?")
	
	for _, job := range jobs {
		name := strings.ToLower(job.ResourceName)
		if hasWildcard {
			matched, _ := matchWildcard(pattern, name)
			if matched {
				filtered = append(filtered, job)
			}
		} else if strings.Contains(name, pattern) {
			filtered = append(filtered, job)
		}
	}
	return filtered
}

func matchWildcard(pattern, s string) (bool, error) {
	// Convert wildcard pattern to regex
	regexPattern := "^" + regexp.QuoteMeta(pattern) + "$"
	regexPattern = strings.ReplaceAll(regexPattern, `\*`, ".*")
	regexPattern = strings.ReplaceAll(regexPattern, `\?`, ".")
	return regexp.MatchString(regexPattern, s)
}

func calculateStatistics(jobs []JobData, days int) []ResourceStats {
	// Group by resource
	byResource := make(map[string][]JobData)
	for _, job := range jobs {
		byResource[job.ResourceName] = append(byResource[job.ResourceName], job)
	}
	
	var stats []ResourceStats
	for name, rjobs := range byResource {
		if len(rjobs) == 0 {
			continue
		}
		
		// Calculate averages
		var totalGiB, totalDuration float64
		for _, j := range rjobs {
			totalGiB += j.GiBTransferred
			totalDuration += j.DurationSeconds
		}
		avgGiB := totalGiB / float64(len(rjobs))
		avgDuration := totalDuration / float64(len(rjobs))
		
		// Calculate standard deviations
		var sumSqGiB, sumSqDuration float64
		for _, j := range rjobs {
			sumSqGiB += math.Pow(j.GiBTransferred-avgGiB, 2)
			sumSqDuration += math.Pow(j.DurationSeconds-avgDuration, 2)
		}
		stdDevGiB := math.Sqrt(sumSqGiB / float64(len(rjobs)))
		stdDevDuration := math.Sqrt(sumSqDuration / float64(len(rjobs)))
		
		// Daily change rate
		dailyChangeGB := (totalGiB * 1.073741824) / float64(days) // GiB to GB
		
		stats = append(stats, ResourceStats{
			ResourceName:          name,
			ResourceType:          rjobs[0].ResourceType,
			JobSource:             rjobs[0].JobSource,
			CurrentDailyChangeGB:  dailyChangeGB,
			BackupJobCount:        len(rjobs),
			AvgGiB:                avgGiB,
			StdDevGiB:             stdDevGiB,
			AvgDurationSeconds:    avgDuration,
			StdDevDuration:        stdDevDuration,
		})
	}
	
	// Sort by resource name
	sort.Slice(stats, func(i, j int) bool {
		return stats[i].ResourceName < stats[j].ResourceName
	})
	
	return stats
}

func detectAnomalies(jobs []JobData, stats []ResourceStats) []Anomaly {
	const zScoreThreshold = 3.0
	const dropOffThreshold = 0.1
	
	// Create stats lookup
	statsMap := make(map[string]ResourceStats)
	for _, s := range stats {
		statsMap[s.ResourceName] = s
	}
	
	var anomalies []Anomaly
	for _, job := range jobs {
		s, ok := statsMap[job.ResourceName]
		if !ok {
			continue
		}
		
		var reasons []string
		
		// Size spike (Z-score)
		if s.StdDevGiB > 0 {
			zScore := (job.GiBTransferred - s.AvgGiB) / s.StdDevGiB
			if zScore > zScoreThreshold {
				reasons = append(reasons, fmt.Sprintf("Size Spike (Z=%.1f)", zScore))
			}
		}
		
		// Size drop-off
		if s.AvgGiB > 1.0 && job.GiBTransferred < s.AvgGiB*dropOffThreshold {
			reasons = append(reasons, "Size Drop-off")
		}
		
		// Duration spike
		if s.StdDevDuration > 0 {
			durationZ := (job.DurationSeconds - s.AvgDurationSeconds) / s.StdDevDuration
			if durationZ > zScoreThreshold {
				reasons = append(reasons, fmt.Sprintf("Duration Spike (Z=%.1f)", durationZ))
			}
		}
		
		if len(reasons) > 0 {
			anomalies = append(anomalies, Anomaly{
				JobID:              job.JobID,
				Resource:           job.ResourceName,
				Date:               job.StartTime.Format("2006-01-02"),
				Time:               job.StartTime.Format("15:04:05"),
				GiBTransferred:     job.GiBTransferred,
				AvgGiB:             s.AvgGiB,
				DurationSeconds:    job.DurationSeconds,
				AvgDurationSeconds: s.AvgDurationSeconds,
				Reasons:            reasons,
			})
		}
	}
	
	return anomalies
}

// GetProjectID returns the project ID from environment.
func GetProjectID() string {
	return os.Getenv("GOOGLE_CLOUD_PROJECT")
}
