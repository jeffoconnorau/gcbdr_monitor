import pytest
from datetime import datetime, timedelta, timezone
from analyzer import calculate_statistics, detect_anomalies

def test_calculate_statistics():
    # Mock job history
    history = [
        {'resource_name': 'vm-1', 'bytes_transferred': 100, 'timestamp': datetime.now(timezone.utc)},
        {'resource_name': 'vm-1', 'bytes_transferred': 200, 'timestamp': datetime.now(timezone.utc)},
        {'resource_name': 'vm-2', 'bytes_transferred': 500, 'timestamp': datetime.now(timezone.utc)},
    ]
    
    stats = calculate_statistics(history)
    
    assert stats['vm-1']['avg_bytes'] == 150.0
    assert stats['vm-1']['data_points'] == 2
    assert stats['vm-2']['avg_bytes'] == 500.0

def test_detect_anomalies():
    stats = {
        'vm-1': {'avg_bytes': 100.0, 'data_points': 5}
    }
    
    current_jobs = [
        {'job_id': 'job-1', 'resource_name': 'vm-1', 'bytes_transferred': 110, 'timestamp': datetime.now(timezone.utc)}, # Normal
        {'job_id': 'job-2', 'resource_name': 'vm-1', 'bytes_transferred': 200, 'timestamp': datetime.now(timezone.utc)}, # Anomaly (2x > 1.5x)
        {'job_id': 'job-3', 'resource_name': 'vm-2', 'bytes_transferred': 1000, 'timestamp': datetime.now(timezone.utc)}, # New resource (no stats)
    ]
    
    anomalies = detect_anomalies(current_jobs, stats, threshold_factor=1.5)
    
    assert len(anomalies) == 1
    assert anomalies[0]['job_id'] == 'job-2'
    assert anomalies[0]['factor'] == 2.0
