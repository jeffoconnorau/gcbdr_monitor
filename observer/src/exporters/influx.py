from typing import List
import logging
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

from config import Config
from collectors.base import Metric

class InfluxExporter:
    def __init__(self):
        self.logger = logging.getLogger("gcbdr.exporter.influx")
        try:
            self.client = InfluxDBClient(
                url=Config.INFLUXDB_URL,
                token=Config.INFLUXDB_TOKEN,
                org=Config.INFLUXDB_ORG
            )
            self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
            self.bucket = Config.INFLUXDB_BUCKET
            self.logger.info(f"Initialized InfluxDB Client for {Config.INFLUXDB_URL}")
        except Exception as e:
            self.logger.error(f"Failed to initialize InfluxDB client: {e}")
            raise e

    def export(self, metrics: List[Metric]):
        if not metrics:
            return

        points = []
        for m in metrics:
            p = Point(m.name)
            
            for k, v in m.tags.items():
                p.tag(k, v)
            
            for k, v in m.fields.items():
                p.field(k, v)
                
            if m.timestamp:
                # Assuming timestamp is in seconds, convert to ns if needed or specify precision
                # InfluxDB client defaults to ns if write_precision not set?
                # Let's trust the default for now or treat timestamp as datetime
                pass
            
            points.append(p)

        try:
            self.write_api.write(bucket=self.bucket, org=Config.INFLUXDB_ORG, record=points)
        except Exception as e:
            self.logger.error(f"Failed to write to InfluxDB: {e}")
            raise e
