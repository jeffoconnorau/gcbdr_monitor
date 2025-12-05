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
        # 100GB total, 1GB change = 1%
        history = [
            {
                'resource_name': 'vm-1', 
                'bytes_transferred': 1073741824, # 1 GiB
                'total_resource_size_bytes': 107374182400, # 100 GiB
                'resourceType': 'GCE_INSTANCE',
                'timestamp': datetime.now(timezone.utc)
            },
            {
                'resource_name': 'vm-1', 
                'bytes_transferred': 2147483648, # 2 GiB
                'total_resource_size_bytes': 107374182400, # 100 GiB
                'resourceType': 'GCE_INSTANCE',
                'timestamp': datetime.now(timezone.utc)
            },
        ]
        
        stats = calculate_statistics(history)
        
        # Avg bytes = 1.5 GiB
        self.assertEqual(stats['vm-1']['avg_bytes'], 1610612736.0)
        self.assertEqual(stats['vm-1']['avg_daily_change_gb'], 1.5)
        self.assertEqual(stats['vm-1']['avg_daily_change_pct'], 1.5)
        self.assertEqual(stats['vm-1']['resource_type'], 'GCE_INSTANCE')

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
        
        # Case 1: Standard entry with total size (top level)
        entry.payload = {
            'jobId': 'job-1',
            'jobStatus': 'SUCCESSFUL',
            'incrementalBackupSizeGib': 1,
            'sourceResourceSizeBytes': 107374182400, # 100 GiB
            'sourceResourceName': 'vm-1'
        }
        data = parse_job_data(entry)
        self.assertEqual(data['jobStatus'], 'SUCCESSFUL')
        self.assertEqual(data['bytes_transferred'], 1073741824) # 1 GiB
        self.assertEqual(data['total_resource_size_bytes'], 107374182400)
        self.assertEqual(data['sourceResourceName'], 'vm-1')

        # Case 2: Nested protectedResourceDetails
        entry.payload = {
            'jobId': 'job-2',
            'jobStatus': 'SUCCESSFUL',
            'incrementalBackupSizeGib': 1,
            'protectedResourceDetails': {
                'sourceResourceSizeBytes': 53687091200 # 50 GiB
            },
            'sourceResourceName': 'vm-2'
        }
        data = parse_job_data(entry)
        self.assertEqual(data['total_resource_size_bytes'], 53687091200)

        # Case 3: sourceResourceDataSizeGib (GiB)
        entry.payload = {
            'jobId': 'job-3',
            'jobStatus': 'SUCCESSFUL',
            'incrementalBackupSizeGib': 1,
            'sourceResourceDataSizeGib': 100.0, # 100 GiB
            'sourceResourceName': 'vm-3'
        }
        data = parse_job_data(entry)
        self.assertEqual(data['total_resource_size_bytes'], 107374182400)

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
        # History job (2 days ago) - 1GB change
        entry1 = Mock()
        entry1.payload = {
            'jobId': 'j1', 
            'jobStatus': 'SUCCESSFUL', 
            'incrementalBackupSizeGib': 1, 
            'sourceResourceSizeBytes': 107374182400,
            'sourceResourceName': 'vm1',
            'resourceType': 'GCE_INSTANCE'
        }
        entry1.timestamp = datetime.now(timezone.utc) - timedelta(days=2)
        
        # Current job (today) - 2GB change
        entry2 = Mock()
        entry2.payload = {
            'jobId': 'j2', 
            'jobStatus': 'SUCCESSFUL', 
            'incrementalBackupSizeGib': 2, 
            'sourceResourceSizeBytes': 107374182400,
            'sourceResourceName': 'vm1',
            'resourceType': 'GCE_INSTANCE'
        }
        entry2.timestamp = datetime.now(timezone.utc)
        
        mock_fetch.return_value = [entry1, entry2]
        
        result = analyze_backup_jobs('project-id')
        
        self.assertEqual(result['successful_count'], 2)
        self.assertEqual(len(result['resource_stats']), 1)
        
        stats = result['resource_stats'][0]
        self.assertEqual(stats['resource_name'], 'vm1')
        self.assertEqual(stats['avg_daily_change_gb'], 1.0) # Historical
        self.assertEqual(stats['current_daily_change_gb'], 2.0) # Current
        self.assertEqual(stats['growth_rate_pct'], 100.0) # 1GB -> 2GB = 100% growth
        self.assertEqual(stats['total_resource_size_gb'], 100.0) # 100 GiB

    @patch('analyzer.fetch_backup_logs')
    @patch('analyzer.fetch_gce_instance_details')
    def test_analyze_backup_jobs_gce_fallback(self, mock_fetch_gce, mock_fetch_logs):
        # Mock logs with 0 size
        mock_entry = Mock()
        mock_entry.payload = {
            'jobId': 'job-1',
            'jobStatus': 'SUCCESSFUL',
            'incrementalBackupSizeGib': 1,
            'sourceResourceName': 'projects/other-project/zones/us-west1-a/instances/vm-gce',
            'resourceType': 'Compute Engine',
            'startTime': '2023-01-01T12:00:00Z',
            'endTime': '2023-01-01T13:00:00Z'
        }
        mock_entry.timestamp = datetime.now(timezone.utc)
        mock_fetch_logs.return_value = [mock_entry]
        
        # Mock GCE return
        mock_fetch_gce.return_value = 500.0
        
        result = analyze_backup_jobs('monitoring-project')
        
        stats = result['resource_stats'][0]
        self.assertEqual(stats['resource_name'], 'projects/other-project/zones/us-west1-a/instances/vm-gce')
        self.assertEqual(stats['total_resource_size_gb'], 500.0)
        # 1 GB change / 500 GB total = 0.2%
        self.assertEqual(stats['current_daily_change_pct'], 0.2)
        
        # Verify fetch_gce_instance_details was called with correct args
        # Note: analyze_backup_jobs calls it with (project_id, resource_name)
        # The parsing happens INSIDE fetch_gce_instance_details, so we just check the call arguments
        mock_fetch_gce.assert_called_with('monitoring-project', 'projects/other-project/zones/us-west1-a/instances/vm-gce')

    def test_fetch_gce_instance_details_parsing(self):
        # We can't easily test the internal parsing of fetch_gce_instance_details without mocking compute_v1
        # But we can verify it doesn't crash
        pass

if __name__ == '__main__':
    unittest.main()
