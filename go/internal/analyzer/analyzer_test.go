package analyzer

import (
	"testing"
	"time"
)

func TestCalculateDailyBaselines(t *testing.T) {
	// Setup dates
	now := time.Now()
	day1 := now.AddDate(0, 0, -2)
	day2 := now.AddDate(0, 0, -1)
	day3 := now

	// Mock Jobs
	jobs := []JobData{
		// Day 1: A and B
		{JobID: "1", ResourceName: "ResA", StartTime: day1, GiBTransferred: 10, TotalResourceSizeBytes: 100 * 1024 * 1024 * 1024},
		{JobID: "2", ResourceName: "ResB", StartTime: day1, GiBTransferred: 20, TotalResourceSizeBytes: 200 * 1024 * 1024 * 1024},

		// Day 2: A (Modified), C (New)
		{JobID: "3", ResourceName: "ResA", StartTime: day2, GiBTransferred: 12, TotalResourceSizeBytes: 100 * 1024 * 1024 * 1024},
		{JobID: "4", ResourceName: "ResC", StartTime: day2, GiBTransferred: 5, TotalResourceSizeBytes: 50 * 1024 * 1024 * 1024},

		// Day 3: C (Modified), A (Suspicious anomaly)
		{JobID: "5", ResourceName: "ResC", StartTime: day3, GiBTransferred: 6, TotalResourceSizeBytes: 50 * 1024 * 1024 * 1024},
		{JobID: "6", ResourceName: "ResA", StartTime: day3, GiBTransferred: 50, TotalResourceSizeBytes: 100 * 1024 * 1024 * 1024},
	}

	// Mock Anomalies (Job 6 is anomalous)
	anomalies := []Anomaly{
		{
			JobID:          "6",
			Resource:       "ResA",
			Date:           day3.Format("2006-01-02"),
			GiBTransferred: 50,
		},
	}

	// Run calculation
	baselines := calculateDailyBaselines(jobs, anomalies, 3)

	// Verify results
	// Expect 3 entries (Day 1, Day 2, Day 3)
	// Note: The function iterates 0..days backwards, so it might include today.
    // The loop in analyzer.go is: start := time.Now().AddDate(0, 0, -days); for i := 0; i <= days; i++
    // So it covers 'days' + 1 days (from T-days to T). 
    
    // Check Day 3 (Latest)
	var d3Stats DailyBaseline
    found := false
	for _, b := range baselines {
        if b.Date == day3.Format("2006-01-02") {
            d3Stats = b
            found = true
            break
        }
	}
    
    if !found {
        t.Fatalf("Day 3 stats not found")
    }

	// Verify Day 3 Stats
	if d3Stats.ModifiedDataGB != 56 { // 50 + 6
		t.Errorf("Day 3 ModifiedDataGB = %f, want 56", d3Stats.ModifiedDataGB)
	}
	if d3Stats.SuspiciousDataGB != 50 { // Job 6
		t.Errorf("Day 3 SuspiciousDataGB = %f, want 50", d3Stats.SuspiciousDataGB)
	}
    
    // Check Day 2 (Middle) - Should show ResC as New
	var d2Stats DailyBaseline
	for _, b := range baselines {
        if b.Date == day2.Format("2006-01-02") {
            d2Stats = b
            break
        }
	}
    
    if d2Stats.NewDataGB != 5 { // ResC is new (5GB)
        t.Errorf("Day 2 NewDataGB = %f, want 5 (ResC)", d2Stats.NewDataGB)
    }
    if d2Stats.NewResourceCount != 1 {
        t.Errorf("Day 2 NewResourceCount = %d, want 1", d2Stats.NewResourceCount)
    }
}
