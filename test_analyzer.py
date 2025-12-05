import sys
from unittest.mock import Mock

# Mock google.cloud.logging before importing analyzer
mock_google = Mock()
mock_logging = Mock()
mock_google.cloud.logging = mock_logging
sys.modules['google'] = mock_google
sys.modules['google.cloud'] = mock_google.cloud
sys.modules['google.cloud.logging'] = mock_logging

import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

# Now import analyzer
from analyzer import calculate_statistics, detect_anomalies, parse_job_data, analyze_backup_jobs, process_jobs

class TestAnalyzer(unittest.TestCase):
    def test_calculate_statistics(self):
        # Mock job history
        history = [
            {'resource_name': 'vm-1', 'bytes_transferred': 100, 'timestamp': datetime.now(timezone.utc)},
            {'resource_name': 'vm-1', 'bytes_transferred': 200, 'timestamp': datetime.now(timezone.utc)},
            {'resource_name': 'vm-2', 'bytes_transferred': 500, 'timestamp': datetime.now(timezone.utc)},
        ]
        
        stats = calculate_statistics(history)
        
        self.assertEqual(stats['vm-1']['avg_bytes'], 150.0)
        self.assertEqual(stats['vm-1']['data_points'], 2)
        self.assertEqual(stats['vm-2']['avg_bytes'], 500.0)

    def test_detect_anomalies(self):
        stats = {
            'vm-1': {'avg_bytes': 100.0, 'data_points': 5}
        }
        
        current_jobs = [
            {'jobId': 'job-1', 'resource_name': 'vm-1', 'bytes_transferred': 110, 'timestamp': datetime.now(timezone.utc)}, # Normal
            {'jobId': 'job-2', 'resource_name': 'vm-1', 'bytes_transferred': 200, 'timestamp': datetime.now(timezone.utc)}, # Anomaly (2x > 1.5x)
            {'jobId': 'job-3', 'resource_name': 'vm-2', 'bytes_transferred': 1000, 'timestamp': datetime.now(timezone.utc)}, # New resource (no stats)
        ]
        
        anomalies = detect_anomalies(current_jobs, stats, threshold_factor=1.5)
        
        self.assertEqual(len(anomalies), 1)
        self.assertEqual(anomalies[0]['job_id'], 'job-2')
        self.assertEqual(anomalies[0]['factor'], 2.0)

    def test_parse_job_data(self):
        # Mock entry
        entry = Mock()
        entry.timestamp = datetime.now(timezone.utc)
        
        # Case 1: Standard entry
        entry.payload = {
            'jobId': 'job-1',
            'jobStatus': 'SUCCESSFUL',
            'incrementalBackupSizeGib': 1,
            'sourceResourceName': 'vm-1'
        }
        data = parse_job_data(entry)
        self.assertEqual(data['jobStatus'], 'SUCCESSFUL')
        self.assertEqual(data['bytes_transferred'], 1073741824) # 1 GiB
        # resource_name is derived in process_jobs, not parse_job_data
        self.assertEqual(data['sourceResourceName'], 'vm-1')

    def test_process_jobs_deduplication(self):
        # Create multiple logs for same job
        logs = [
            {'jobId': 'job-1', 'jobStatus': 'RUNNING', 'timestamp': 1},
            {'jobId': 'job-1', 'jobStatus': 'SUCCESSFUL', 'timestamp': 2},
            {'jobId': 'job-2', 'jobStatus': 'RUNNING', 'timestamp': 1},
            {'jobId': 'job-2', 'jobStatus': 'FAILED', 'timestamp': 2},
        ]
        
        unique_jobs = process_jobs(logs)
        
        self.assertEqual(len(unique_jobs), 2)
        
        job1 = next(j for j in unique_jobs if j['jobId'] == 'job-1')
        self.assertEqual(job1['status'], 'SUCCESSFUL')
        
        job2 = next(j for j in unique_jobs if j['jobId'] == 'job-2')
        self.assertEqual(job2['status'], 'FAILED')

    @patch('analyzer.fetch_backup_logs')
    def test_analyze_backup_jobs_counts(self, mock_fetch):
        # Create mock entries
        entry1 = Mock()
        entry1.payload = {'jobId': 'j1', 'jobStatus': 'SUCCESSFUL', 'incrementalBackupSizeGib': 0.1, 'sourceResourceName': 'vm1'}
        entry1.timestamp = datetime.now(timezone.utc)
        
        entry2 = Mock()
        entry2.payload = {'jobId': 'j2', 'jobStatus': 'FAILED', 'sourceResourceName': 'vm1'}
        entry2.timestamp = datetime.now(timezone.utc)
        
        mock_fetch.return_value = [entry1, entry2]
        
        result = analyze_backup_jobs('project-id')
        
        self.assertEqual(result['total_jobs_found'], 2)
        self.assertEqual(result['successful_count'], 1)
        self.assertEqual(result['failed_count'], 1)

if __name__ == '__main__':
    unittest.main()
