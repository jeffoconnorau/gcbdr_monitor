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
from analyzer import calculate_statistics, detect_anomalies, parse_job_data, analyze_backup_jobs, process_jobs, parse_appliance_job_data

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
            'vm-1': {'avg_bytes': 1073741824}, # 1 GiB average
            'vm-2': {'avg_bytes': 0}
        }
        
        current_jobs = [
            {'jobId': 'job-1', 'resource_name': 'vm-1', 'bytes_transferred': 1073741824, 'timestamp': datetime.now(timezone.utc), 'resourceType': 'GCE_INSTANCE', 'total_resource_size_bytes': 107374182400}, # Normal (1 GiB)
            {'jobId': 'job-2', 'resource_name': 'vm-1', 'bytes_transferred': 10737418240, 'timestamp': datetime.now(timezone.utc), 'resourceType': 'GCE_INSTANCE', 'total_resource_size_bytes': 107374182400}, # Anomaly (10 GiB)
            {'jobId': 'job-3', 'resource_name': 'vm-2', 'bytes_transferred': 1000, 'timestamp': datetime.now(timezone.utc), 'resourceType': 'GCE_INSTANCE', 'total_resource_size_bytes': 107374182400}, # New resource (no stats)
        ]
        
        anomalies = detect_anomalies(current_jobs, stats)
        
        self.assertEqual(len(anomalies), 1)
        self.assertEqual(anomalies[0]['job_id'], 'job-2')
        self.assertEqual(anomalies[0]['gib_transferred'], 10.0)
        self.assertEqual(anomalies[0]['avg_gib'], 1.0)
        self.assertEqual(anomalies[0]['factor'], 10.0)
        # Check metadata
        self.assertIn('date', anomalies[0])
        self.assertIn('time', anomalies[0])
        self.assertEqual(anomalies[0]['resource_type'], 'GCE_INSTANCE')
        self.assertEqual(anomalies[0]['total_resource_size_gb'], 100.0)

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

    @patch('analyzer.fetch_gcb_jobs_logs')
    @patch('analyzer.fetch_appliance_logs')
    @patch('analyzer.fetch_backup_logs')
    def test_analyze_backup_jobs_counts(self, mock_fetch, mock_fetch_appliance, mock_fetch_gcb):
        mock_fetch_appliance.return_value = []
        mock_fetch_gcb.return_value = []
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
        
        self.assertEqual(result['summary']['successful_jobs'], 2)
        self.assertEqual(len(result['vault_workloads']['resource_stats']), 1)
        
        stats = result['vault_workloads']['resource_stats'][0]
        self.assertEqual(stats['resource_name'], 'vm1')
        # 1GB (history) + 2GB (current) = 3GB total / 2 jobs = 1.5 GB avg
        self.assertEqual(stats['current_daily_change_gb'], 1.5) 
        self.assertEqual(stats['total_resource_size_gb'], 100.0) # 100 GiB
        # 1 current + 1 historical = 2 total
        self.assertEqual(stats['backup_job_count'], 2)

    @patch('analyzer.fetch_gcb_jobs_logs')
    @patch('analyzer.fetch_appliance_logs')
    @patch('analyzer.fetch_backup_logs')
    @patch('analyzer.fetch_gce_instance_details')
    @patch('analyzer.fetch_gce_disk_details')
    @patch('analyzer.fetch_cloudsql_details')
    def test_analyze_backup_jobs_fallback(self, mock_fetch_sql, mock_fetch_disk, mock_fetch_gce, mock_fetch_logs, mock_fetch_appliance, mock_fetch_gcb):
        mock_fetch_appliance.return_value = []
        mock_fetch_gcb.return_value = []
        
        # Mock logs with 0 size for different resource types
        entry_gce = Mock()
        entry_gce.payload = {
            'jobId': 'job-1', 'jobStatus': 'SUCCESSFUL', 'incrementalBackupSizeGib': 1,
            'sourceResourceName': 'projects/p/zones/z/instances/vm', 'resourceType': 'Compute Engine',
            'startTime': '2023-01-01T12:00:00Z', 'endTime': '2023-01-01T13:00:00Z'
        }
        entry_gce.timestamp = datetime.now(timezone.utc)
        
        entry_disk = Mock()
        entry_disk.payload = {
            'jobId': 'job-2', 'jobStatus': 'SUCCESSFUL', 'incrementalBackupSizeGib': 1,
            'sourceResourceName': 'projects/p/zones/z/disks/disk1', 'resourceType': 'Persistent Disk',
            'startTime': '2023-01-01T12:00:00Z', 'endTime': '2023-01-01T13:00:00Z'
        }
        entry_disk.timestamp = datetime.now(timezone.utc)
        
        entry_sql = Mock()
        entry_sql.payload = {
            'jobId': 'job-3', 'jobStatus': 'SUCCESSFUL', 'incrementalBackupSizeGib': 1,
            'sourceResourceName': 'projects/p/instances/sql1', 'resourceType': 'Cloud SQL',
            'startTime': '2023-01-01T12:00:00Z', 'endTime': '2023-01-01T13:00:00Z'
        }
        entry_sql.timestamp = datetime.now(timezone.utc)

        mock_fetch_logs.return_value = [entry_gce, entry_disk, entry_sql]
        
        # Mock returns
        mock_fetch_gce.return_value = 500.0
        mock_fetch_disk.return_value = 100.0
        mock_fetch_sql.return_value = 200.0
        
        result = analyze_backup_jobs('monitoring-project')
        
        # Verify GCE
        stats_gce = next(r for r in result['vault_workloads']['resource_stats'] if r['resource_name'] == 'projects/p/zones/z/instances/vm')
        self.assertEqual(stats_gce['total_resource_size_gb'], 500.0)
        
        # Verify Disk
        stats_disk = next(r for r in result['vault_workloads']['resource_stats'] if r['resource_name'] == 'projects/p/zones/z/disks/disk1')
        self.assertEqual(stats_disk['total_resource_size_gb'], 100.0)
        
        # Verify SQL
        stats_sql = next(r for r in result['vault_workloads']['resource_stats'] if r['resource_name'] == 'projects/p/instances/sql1')
        self.assertEqual(stats_sql['total_resource_size_gb'], 200.0)
        
        # Verify calls
        mock_fetch_gce.assert_called()
        mock_fetch_disk.assert_called_with('monitoring-project', 'projects/p/zones/z/disks/disk1')
        mock_fetch_sql.assert_called_with('monitoring-project', 'projects/p/instances/sql1')

    def test_internal_functions_exist(self):
        from analyzer import _calculate_disk_size
        self.assertTrue(callable(_calculate_disk_size))

if __name__ == '__main__':
    unittest.main()

    def test_parse_appliance_job_data(self):
        # Mock entry
        entry = Mock()
        entry.timestamp = datetime.now(timezone.utc)
        
        # Case 1: Standard appliance entry
        entry.payload = {
            'jobName': 'job-appliance-1',
            'eventId': 44003,
            'dataCopiedInBytes': 1073741824, # 1 GiB
            'sourceSize': 107374182400, # 100 GiB
            'appName': 'app-1',
            'appType': 'SQLServer'
        }
        data = parse_appliance_job_data(entry)
        self.assertEqual(data['jobId'], 'job-appliance-1')
        self.assertEqual(data['jobStatus'], 'SUCCESSFUL')
        self.assertEqual(data['bytes_transferred'], 1073741824)
        self.assertEqual(data['total_resource_size_bytes'], 107374182400)
        self.assertEqual(data['sourceResourceName'], 'app-1')
        self.assertEqual(data['resourceType'], 'SQLServer')
        self.assertEqual(data['job_source'], 'appliance')

    @patch('analyzer.fetch_appliance_logs')
    @patch('analyzer.fetch_backup_logs')
    def test_analyze_backup_jobs_with_appliance(self, mock_fetch_vault, mock_fetch_appliance):
        # Vault job
        entry1 = Mock()
        entry1.payload = {
            'jobId': 'j1', 
            'jobStatus': 'SUCCESSFUL', 
            'incrementalBackupSizeGib': 1, 
            'sourceResourceSizeBytes': 107374182400,
            'sourceResourceName': 'vm1',
            'resourceType': 'GCE_INSTANCE'
        }
        entry1.timestamp = datetime.now(timezone.utc)
        mock_fetch_vault.return_value = [entry1]
        
        # Appliance job
        entry2 = Mock()
        entry2.payload = {
            'jobName': 'j2',
            'eventId': 44003,
            'dataCopiedInBytes': 2147483648, # 2 GiB
            'sourceSize': 107374182400,
            'appName': 'app1',
            'appType': 'SQLServer'
        }
        entry2.timestamp = datetime.now(timezone.utc)
        mock_fetch_appliance.return_value = [entry2]
        
        result = analyze_backup_jobs('project-id')
        
        self.assertEqual(result['summary']['successful_jobs'], 2)
        # 100 + 100 = 200 total size
        self.assertEqual(result['summary']['total_resource_size_gb'], 200.0)
        # 1 + 2 = 3 total change
        self.assertEqual(result['summary']['current_daily_change_gb'], 3.0)
        # 3 / 200 * 100 = 1.5%
        self.assertEqual(result['summary']['current_daily_change_pct'], 1.5)
        
        # Check appliance stats
        self.assertEqual(result['appliance_workloads']['successful_jobs'], 1)
        app_stats = result['appliance_workloads']['resource_stats'][0]
        self.assertEqual(app_stats['resource_name'], 'app1')
        self.assertEqual(app_stats['current_daily_change_gb'], 2.0)
        self.assertEqual(app_stats['job_source'], 'appliance')
        
        # Check vault stats
        self.assertEqual(result['vault_workloads']['successful_jobs'], 1)
        vault_stats = result['vault_workloads']['resource_stats'][0]
        self.assertEqual(vault_stats['resource_name'], 'vm1')
        self.assertEqual(vault_stats['current_daily_change_gb'], 1.0)
        self.assertEqual(vault_stats['job_source'], 'vault')

    @patch('analyzer.fetch_gcb_jobs_logs')
    @patch('analyzer.fetch_appliance_logs')
    @patch('analyzer.fetch_backup_logs')
    def test_analyze_backup_jobs_enrichment(self, mock_fetch_vault, mock_fetch_appliance, mock_fetch_gcb):
        # Vault job (empty)
        mock_fetch_vault.return_value = []
        
        # Appliance job with missing size (using sample data structure)
        entry_app = Mock()
        entry_app.payload = {
            'jobName': 'Job_19729093',
            'eventId': 44003,
            'dataCopiedInBytes': 1073741824, # 1 GiB
            'sourceSize': 0, # Missing size
            'appName': 'winsql22-01',
            'appType': 'VMBackup'
        }
        entry_app.timestamp = datetime.now(timezone.utc)
        mock_fetch_appliance.return_value = [entry_app]
        
        # GCB job with size (using sample data structure)
        entry_gcb = Mock()
        entry_gcb.insert_id = '19750232_142253982799'
        entry_gcb.payload = {
            'job_name': 'Job_19729093',
            'resource_data_size_in_gib': 62.12,
            'data_copied_in_gib': 6.61
        }
        mock_fetch_gcb.return_value = [entry_gcb]
        
        result = analyze_backup_jobs('project-id')
        
        # Check if size was enriched
        app_stats = result['appliance_workloads']['resource_stats'][0]
        self.assertEqual(app_stats['resource_name'], 'winsql22-01')
        self.assertEqual(app_stats['total_resource_size_gb'], 62.12)
        
        # 6.61 GiB transferred
        self.assertEqual(app_stats['current_daily_change_gb'], 6.61)
        
        # 6.61 / 62.12 * 100 = 10.6407...
        self.assertAlmostEqual(app_stats['current_daily_change_pct'], 10.64, places=2)

    def test_matches_filter(self):
        from analyzer import matches_filter
        
        # Exact match
        self.assertTrue(matches_filter("test-vm", "test-vm"))
        self.assertTrue(matches_filter("TEST-VM", "test-vm")) # Case insensitive
        
        # Substring (implicit wildcard)
        self.assertTrue(matches_filter("my-test-vm", "test"))
        self.assertFalse(matches_filter("my-vm", "test"))
        
        # Wildcard
        self.assertTrue(matches_filter("test-vm-1", "test*"))
        self.assertTrue(matches_filter("test-vm-1", "*vm*"))
        self.assertTrue(matches_filter("test-vm-1", "test-vm-?"))
        self.assertFalse(matches_filter("prod-vm-1", "test*"))
        
        # None/Empty
        self.assertTrue(matches_filter("anything", None))
        self.assertTrue(matches_filter("anything", ""))
        self.assertFalse(matches_filter(None, "pattern"))

    @patch('analyzer.fetch_gcb_jobs_logs')
    @patch('analyzer.fetch_appliance_logs')
    @patch('analyzer.fetch_backup_logs')
    def test_analyze_backup_jobs_filtering(self, mock_fetch_vault, mock_fetch_appliance, mock_fetch_gcb):
        mock_fetch_appliance.return_value = []
        mock_fetch_gcb.return_value = []
        
        # Create jobs for different resources
        entry1 = Mock()
        entry1.payload = {
            'jobId': 'j1', 'jobStatus': 'SUCCESSFUL', 'incrementalBackupSizeGib': 1, 
            'sourceResourceSizeBytes': 100, 'sourceResourceName': 'sql-db-1', 'resourceType': 'Cloud SQL'
        }
        entry1.timestamp = datetime.now(timezone.utc)
        
        entry2 = Mock()
        entry2.payload = {
            'jobId': 'j2', 'jobStatus': 'SUCCESSFUL', 'incrementalBackupSizeGib': 1, 
            'sourceResourceSizeBytes': 100, 'sourceResourceName': 'web-server-1', 'resourceType': 'GCE_INSTANCE'
        }
        entry2.timestamp = datetime.now(timezone.utc)
        
        mock_fetch_vault.return_value = [entry1, entry2]
        
        # Test with filter "sql"
        result = analyze_backup_jobs('project-id', filter_name='sql')
        
        self.assertEqual(result['summary']['successful_jobs'], 1)
        self.assertEqual(len(result['vault_workloads']['resource_stats']), 1)
        self.assertEqual(result['vault_workloads']['resource_stats'][0]['resource_name'], 'sql-db-1')
        
        # Test with wildcard "*server*"
        result = analyze_backup_jobs('project-id', filter_name='*server*')
        
        self.assertEqual(result['summary']['successful_jobs'], 1)
        self.assertEqual(len(result['vault_workloads']['resource_stats']), 1)
        self.assertEqual(result['vault_workloads']['resource_stats'][0]['resource_name'], 'web-server-1')
