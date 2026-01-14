"""
Collector for Daily Baseline Metrics.
Collects aggregated daily data for charting in Grafana.
"""
from typing import List
import logging
from datetime import datetime, timezone

from collectors.base import BaseCollector, Metric


class BaselineCollector(BaseCollector):
    """
    Collects daily baseline metrics from the analyzer output.
    
    Metrics exported:
    - modified_data_gb: Total bytes transferred for the day
    - new_data_gb: Size of newly seen resources
    - deleted_data_gb: Size of resources no longer seen
    - suspicious_data_gb: Sum of anomaly bytes transferred
    - total_protected_gb: Total protected capacity
    """
    
    def __init__(self):
        super().__init__("baseline")
        
    def collect_from_baselines(self, daily_baselines: List[dict]) -> List[Metric]:
        """
        Converts daily baseline data to InfluxDB metrics.
        
        Args:
            daily_baselines: List of baseline dicts from analyzer
            
        Returns:
            List of Metric objects for InfluxDB export
        """
        metrics = []
        
        for baseline in daily_baselines:
            date_str = baseline.get('date')
            if not date_str:
                continue
                
            # Parse date to timestamp
            try:
                dt = datetime.strptime(date_str, '%Y-%m-%d').replace(
                    hour=12, minute=0, second=0, tzinfo=timezone.utc
                )
                timestamp = dt.timestamp()
            except ValueError:
                self.logger.warning(f"Invalid date format: {date_str}")
                continue
            
            # Create metric for this day
            metric = Metric(
                name="daily_baseline",
                tags={
                    "source": "gcbdr_monitor"
                },
                fields={
                    "modified_data_gb": float(baseline.get('modified_data_gb', 0)),
                    "new_data_gb": float(baseline.get('new_data_gb', 0)),
                    "deleted_data_gb": float(baseline.get('deleted_data_gb', 0)),
                    "suspicious_data_gb": float(baseline.get('suspicious_data_gb', 0)),
                    "total_protected_gb": float(baseline.get('total_protected_gb', 0)),
                    "resource_count": int(baseline.get('resource_count', 0)),
                    "new_resource_count": int(baseline.get('new_resource_count', 0)),
                    "deleted_resource_count": int(baseline.get('deleted_resource_count', 0))
                },
                timestamp=timestamp
            )
            metrics.append(metric)
            
        self.logger.info(f"Collected {len(metrics)} daily baseline metrics")
        return metrics
    
    def collect(self) -> List[Metric]:
        """
        Standard collect method - returns empty list.
        Use collect_from_baselines() with data from analyzer instead.
        """
        # This collector requires external data, so collect() returns empty.
        # Call collect_from_baselines() with data after running analyzer.
        return []
