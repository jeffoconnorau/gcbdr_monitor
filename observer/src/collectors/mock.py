import random
import time
from typing import List
from .base import BaseCollector, Metric

class MockCollector(BaseCollector):
    def __init__(self):
        super().__init__("mock_collector")

    def collect(self) -> List[Metric]:
        metrics = []
        job_types = ["backup", "restore", "snapshot"]
        statuses = ["success", "fail", "running"]
        policies = ["Gold", "Silver", "Bronze"]
        
        # Simulate some jobs
        for _ in range(random.randint(5, 15)):
            job_type = random.choice(job_types)
            status = random.choice(statuses)
            
            # Prefer success
            if random.random() > 0.8:
                status = "fail"
            elif random.random() > 0.9:
                status = "running"
            else:
                status = "success"

            tags = {
                "source": "mock_mgmt_console" if random.random() > 0.5 else "mock_native",
                "job_type": job_type,
                "status": status,
                "policy_name": random.choice(policies),
                "resource_name": f"vm-{random.randint(1000, 9999)}"
            }
            
            fields = {
                "duration_seconds": random.uniform(10.0, 300.0),
                "data_size_bytes": random.randint(1024*1024, 1024*1024*1024)
            }
            
            metrics.append(Metric(
                name="gcbdr_job_status",
                tags=tags,
                fields=fields,
                timestamp=time.time()
            ))
            
        self.logger.info(f"Generated {len(metrics)} mock metrics")
        return metrics
