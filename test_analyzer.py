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
from analyzer import calculate_statistics, detect_anomalies, parse_job_data, analyze_backup_jobs

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
            {'job_id': 'job-1', 'resource_name': 'vm-1', 'bytes_transferred': 110, 'timestamp': datetime.now(timezone.utc)}, # Normal
            {'job_id': 'job-2', 'resource_name': 'vm-1', 'bytes_transferred': 200, 'timestamp': datetime.now(timezone.utc)}, # Anomaly (2x > 1.5x)
            {'job_id': 'job-3', 'resource_name': 'vm-2', 'bytes_transferred': 1000, 'timestamp': datetime.now(timezone.utc)}, # New resource (no stats)
        ]
        
        anomalies = detect_anomalies(current_jobs, stats, threshold_factor=1.5)
        
        self.assertEqual(len(anomalies), 1)
        self.assertEqual(anomalies[0]['job_id'], 'job-2')
        self.assertEqual(anomalies[0]['factor'], 2.0)

    def test_parse_job_data_inference(self):
        # Mock entry
        entry = Mock()
        entry.timestamp = datetime.now(timezone.utc)
        
        # Case 1: Explicit status
        entry.payload = {'status': 'SUCCESS', 'job_id': '1'}
        data = parse_job_data(entry)
        self.assertEqual(data['status'], 'SUCCESS')
        
        # Case 2: Infer success
        entry.payload = {'message': 'Backup job succeeded', 'job_id': '2'}
        data = parse_job_data(entry)
        self.assertEqual(data['status'], 'SUCCESS')
        
        # Case 3: Infer failure
        entry.payload = {'message': 'Backup job failed', 'job_id': '3'}
        data = parse_job_data(entry)
        self.assertEqual(data['status'], 'FAILURE')
        
        # Case 4: Unknown
        entry.payload = {'message': 'Something else', 'job_id': '4'}
        data = parse_job_data(entry)
        self.assertEqual(data['status'], 'UNKNOWN')

    @patch('analyzer.fetch_backup_logs')
    def test_analyze_backup_jobs_counts(self, mock_fetch):
        # Create mock entries
        entry1 = Mock()
        entry1.payload = {'status': 'SUCCESS', 'bytes_transferred': 100, 'resource_name': 'vm1'}
        entry1.timestamp = datetime.now(timezone.utc)
        
        entry2 = Mock()
        entry2.payload = {'status': 'FAILURE', 'resource_name': 'vm1'}
        entry2.timestamp = datetime.now(timezone.utc)
        
        mock_fetch.return_value = [entry1, entry2]
        
        result = analyze_backup_jobs('project-id')
        
        self.assertEqual(result['total_jobs_found'], 2)
        self.assertEqual(result['successful_count'], 1)
        self.assertEqual(result['failed_count'], 1)

if __name__ == '__main__':
    unittest.main()
