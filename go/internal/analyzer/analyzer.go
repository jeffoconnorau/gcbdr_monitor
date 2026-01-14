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
	"google.golang.org/protobuf/types/known/structpb"
)

// JobData represents a parsed backup job.
type JobData struct {
	JobID                  string
	ResourceName           string
	ResourceType           string
	Status                 string
	StartTime              time.Time
	EndTime                time.Time
	GiBTransferred         float64
	DurationSeconds        float64
	TotalResourceSizeBytes int64
	JobSource              string // "vault" or "appliance"
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

// DailyBaseline represents aggregated daily metrics.
type DailyBaseline struct {
	Date                 string  `json:"date"`
	ModifiedDataGB       float64 `json:"modified_data_gb"`
	NewDataGB            float64 `json:"new_data_gb"`
	DeletedDataGB        float64 `json:"deleted_data_gb"`
	SuspiciousDataGB     float64 `json:"suspicious_data_gb"`
	TotalProtectedGB     float64 `json:"total_protected_gb"`
	ResourceCount        int     `json:"resource_count"`
	NewResourceCount     int     `json:"new_resource_count"`
	DeletedResourceCount int     `json:"deleted_resource_count"`
}

// AnalysisResult is the output of the analysis.
type AnalysisResult struct {
	Summary            Summary         `json:"summary"`
	VaultWorkloads     WorkloadResult  `json:"vault_workloads"`
	ApplianceWorkloads WorkloadResult  `json:"appliance_workloads"`
	Anomalies          []Anomaly       `json:"anomalies"`
	DailyBaselines     []DailyBaseline `json:"daily_baselines"`
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

	// Collect all jobs
	var allVaultJobs, allApplianceJobs []JobData
	if sourceType == "all" || sourceType == "vault" {
		if jobs, err := a.fetchAndParseVaultLogs(ctx); err == nil {
			allVaultJobs = filterJobs(jobs, filterName)
			stats := calculateStatistics(allVaultJobs, a.Days)
			result.VaultWorkloads.ResourceStats = stats
			result.Summary.TotalVaultJobs = len(allVaultJobs)
			anomalies := detectAnomalies(allVaultJobs, stats)
			result.Anomalies = append(result.Anomalies, anomalies...)
		} else {
			log.Printf("Warning: failed to fetch vault logs: %v", err)
		}
	}

	if sourceType == "all" || sourceType == "appliance" {
		if jobs, err := a.fetchAndParseApplianceLogs(ctx); err == nil {
			allApplianceJobs = filterJobs(jobs, filterName)
			stats := calculateStatistics(allApplianceJobs, a.Days)
			result.ApplianceWorkloads.ResourceStats = stats
			result.Summary.TotalApplianceJobs = len(allApplianceJobs)
			anomalies := detectAnomalies(allApplianceJobs, stats)
			result.Anomalies = append(result.Anomalies, anomalies...)
		} else {
			log.Printf("Warning: failed to fetch appliance logs: %v", err)
		}
	}

	allJobs := append(allVaultJobs, allApplianceJobs...)
	result.DailyBaselines = calculateDailyBaselines(allJobs, result.Anomalies, a.Days)

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
		`logName="projects/%s/logs/backupdr.googleapis.com%%2Fbackup_recovery_appliance_events" AND jsonPayload.eventId=(44003) AND timestamp >= "%s"`,
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
	if entry.Payload == nil {
		log.Printf("Debug: Entry payload is nil for %s", source)
		return nil
	}

	var payload map[string]interface{}

	switch p := entry.Payload.(type) {
	case map[string]interface{}:
		payload = p
	case *structpb.Struct:
		payload = p.AsMap()
	default:
		log.Printf("Debug: Payload is not map[string]interface{} or structpb, it is %T for %s", entry.Payload, source)
		return nil
	}

	// Debug log for the first few entries to verify structure
	// This is noisy but helpful for debugging the user's issue
	// log.Printf("Debug: Processing payload keys: %v", getKeys(payload))

	job := &JobData{
		JobSource: source,
		StartTime: entry.Timestamp,
	}

	// Generic/Vault fields
	if id, ok := payload["jobId"].(string); ok {
		job.JobID = id
	}
	if name, ok := payload["sourceResourceName"].(string); ok {
		job.ResourceName = name
	}
	if rtype, ok := payload["resourceType"].(string); ok {
		job.ResourceType = rtype
	}
	if status, ok := payload["jobStatus"].(string); ok {
		job.Status = status
	}
	if gib, ok := payload["incrementalBackupSizeGib"].(float64); ok {
		job.GiBTransferred = gib
	}

	// Appliance specific overrides
	if source == "appliance" {
		// Job ID
		if name, ok := payload["jobName"].(string); ok {
			job.JobID = name
		} else if srcid, ok := payload["srcid"].(string); ok {
			job.JobID = srcid
		}

		// Resource Name
		if appName, ok := payload["appName"].(string); ok {
			job.ResourceName = appName
		}

		// Resource Type
		if appType, ok := payload["appType"].(string); ok {
			job.ResourceType = appType
		}

		// Status (44003 is success)
		job.Status = "SUCCESSFUL"

		// Bytes Transferred (convert to GiB)
		var bytes float64
		foundBytes := false

		// Helper to get float from map
		getFloat := func(key string) (float64, bool) {
			if v, ok := payload[key].(float64); ok {
				return v, true
			}
			if v, ok := payload[key].(string); ok {
				// Try parsing string as float
				var f float64
				if _, err := fmt.Sscanf(v, "%f", &f); err == nil {
					return f, true
				}
			}
			return 0, false
		}

		if v, ok := getFloat("dataCopiedInBytes"); ok {
			bytes = v
			foundBytes = true
		} else if v, ok := getFloat("bytesWritten"); ok {
			bytes = v
			foundBytes = true
		} else if v, ok := getFloat("transferSize"); ok {
			bytes = v
			foundBytes = true
		}

		if foundBytes {
			job.GiBTransferred = bytes / (1024 * 1024 * 1024)
		}
	}

	// Calculate duration from start/end times if available
	var startTime, endTime time.Time
	if st, ok := payload["startTime"].(string); ok {
		startTime, _ = time.Parse(time.RFC3339, st)
	} else if et, ok := payload["eventTime"].(string); ok {
		// Appliance logs use eventTime
		startTime, _ = time.Parse(time.RFC3339, et)
	}

	if et, ok := payload["endTime"].(string); ok {
		endTime, _ = time.Parse(time.RFC3339, et)
	} else if et, ok := payload["eventTime"].(string); ok {
		endTime, _ = time.Parse(time.RFC3339, et)
	}

	if !startTime.IsZero() && !endTime.IsZero() {
		job.DurationSeconds = endTime.Sub(startTime).Seconds()
	} else if duration, ok := payload["durationSeconds"].(float64); ok {
		job.DurationSeconds = duration
	}

	// Extract total resource size (logic matches Python analyzer.py)
	var totalBytes int64

	// Helper to get int64 or valid float64 as int64
	getAsInt64 := func(v interface{}) int64 {
		switch i := v.(type) {
		case float64:
			return int64(i)
		case string:
			// parse string if needed, but logs usually have numbers
			return 0
		default:
			return 0
		}
	}

	// 1. Check top-level fields
	if v, ok := payload["sourceResourceSizeBytes"]; ok {
		totalBytes = getAsInt64(v)
	} else if v, ok := payload["usedStorageGib"]; ok {
		totalBytes = int64(getAsInt64(v) * 1024 * 1024 * 1024)
	} else if v, ok := payload["sourceResourceDataSizeGib"]; ok {
		totalBytes = int64(getAsInt64(v) * 1024 * 1024 * 1024)
	}

	// 2. Check nested protectedResourceDetails if not found
	if totalBytes == 0 {
		if details, ok := payload["protectedResourceDetails"].(map[string]interface{}); ok {
			if v, ok := details["sourceResourceSizeBytes"]; ok {
				totalBytes = getAsInt64(v)
			} else if v, ok := details["usedStorageGib"]; ok {
				totalBytes = int64(getAsInt64(v) * 1024 * 1024 * 1024)
			} else if v, ok := details["sourceResourceDataSizeGib"]; ok {
				totalBytes = int64(getAsInt64(v) * 1024 * 1024 * 1024)
			}
		}
	}

	job.TotalResourceSizeBytes = totalBytes
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
		var maxTotalBytes int64

		for _, j := range rjobs {
			totalGiB += j.GiBTransferred
			totalDuration += j.DurationSeconds
			if j.TotalResourceSizeBytes > maxTotalBytes {
				maxTotalBytes = j.TotalResourceSizeBytes
			}
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

		// Total Resource Size in GiB
		totalResourceSizeGB := float64(maxTotalBytes) / (1024 * 1024 * 1024)

		// Percent change
		var dailyChangePct float64
		if totalResourceSizeGB > 0 {
			dailyChangePct = (dailyChangeGB / totalResourceSizeGB) * 100
		}

		stats = append(stats, ResourceStats{
			ResourceName:          name,
			ResourceType:          rjobs[0].ResourceType,
			JobSource:             rjobs[0].JobSource,
			TotalResourceSizeGB:   totalResourceSizeGB,
			CurrentDailyChangeGB:  dailyChangeGB,
			CurrentDailyChangePct: dailyChangePct,
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

func calculateDailyBaselines(jobs []JobData, anomalies []Anomaly, days int) []DailyBaseline {
	// 1. Group jobs by date
	jobsByDate := make(map[string][]JobData)
	for _, job := range jobs {
		date := job.StartTime.Format("2006-01-02")
		jobsByDate[date] = append(jobsByDate[date], job)
	}

	// 2. Map anomalies by date+resource for quick lookup
	anomalyMap := make(map[string]float64) // date:resource -> gib
	for _, a := range anomalies {
		key := fmt.Sprintf("%s:%s", a.Date, a.Resource)
		anomalyMap[key] += a.GiBTransferred
	}

	// 3. Process each day to calculate metrics
	var baselines []DailyBaseline

	// Track resources seen globally to detect "New" resources
	// Note: In a stateless run, "New" is just "First seen in this period".
	// To be more accurate, we'd need historical state, but this matches the Python logic's "seen_resources"
	globalSeenResources := make(map[string]bool)

	// Iterate through dates in chronological order
	// We go back 'days' from now
	start := time.Now().AddDate(0, 0, -days)
	for i := 0; i <= days; i++ {
		date := start.AddDate(0, 0, i).Format("2006-01-02")
		daysJobs := jobsByDate[date]

		if len(daysJobs) == 0 {
			// Even if no jobs, we might want an entry? Python skips if no data properly or fills 0.
			// Let's create an entry with 0s if it's within our range, or just skip if map empty?
			// Python loops sorted(unique_dates). Let's do that.
			continue
		}
	}

	// Actually, let's just iterate over the dates present in the data, sorted.
	var sortedDates []string
	for d := range jobsByDate {
		sortedDates = append(sortedDates, d)
	}
	sort.Strings(sortedDates)

	for _, date := range sortedDates {
		daysJobs := jobsByDate[date]

		var b DailyBaseline
		b.Date = date
		b.ResourceCount = len(daysJobs)

		// Resources active today
		todayResources := make(map[string]bool)

		for _, job := range daysJobs {
			// Modified Data
			b.ModifiedDataGB += job.GiBTransferred

			// Total Protected (Max of the day for that resource)
			// Actually we sum the max size of each resource for that day

			// Suspicious Data
			key := fmt.Sprintf("%s:%s", date, job.ResourceName)
			if _, isAnomaly := anomalyMap[key]; isAnomaly {
				b.SuspiciousDataGB += job.GiBTransferred
			}

			todayResources[job.ResourceName] = true

			// New Data/Resource
			if !globalSeenResources[job.ResourceName] {
				b.NewDataGB += job.GiBTransferred
				b.NewResourceCount++
			}

			// Mark as seen globally
			globalSeenResources[job.ResourceName] = true
		}

		// To Calculate Total Protected GB: Sum of (Max TotalResourceSizeBytes per resource today)
		// To Calculate Deleted: Resources seen globally BEFORE today, but NOT today.
		// (This logic is tricky in a single pass of sorted dates.
		//  Deleted usually means "Was active yesterday, not active today".
		//  For this stateless version, "Deleted" might be hard to verify without looking ahead or knowing the full universe.
		//  Let's roughly match Python: "deleted_data_gb" is often simplistic or requires comparing set(yesterday) - set(today).

		// Let's implement a "Total Protected" calculation
		// Group by resource for this day
		resourceMaxSizes := make(map[string]int64)
		for _, job := range daysJobs {
			if job.TotalResourceSizeBytes > resourceMaxSizes[job.ResourceName] {
				resourceMaxSizes[job.ResourceName] = job.TotalResourceSizeBytes
			}
		}
		for _, size := range resourceMaxSizes {
			b.TotalProtectedGB += float64(size) / (1024 * 1024 * 1024)
		}

		// "Deleted" Logic (Simple version):
		// If we had a list of "all active resources" from previous days, we check if they are missing today.
		// But in a stateless list of jobs, "Deleted" is hard to distinguish from "Backup didn't run yet".
		// We'll leave Deleted as 0 for now unless we implement complex day-over-day diffing.

		baselines = append(baselines, b)
	}

	return baselines
}

// GetProjectID returns the project ID from environment.
func GetProjectID() string {
	return os.Getenv("GOOGLE_CLOUD_PROJECT")
}
