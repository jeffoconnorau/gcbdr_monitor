import time
import logging
import signal
import sys
from concurrent.futures import ThreadPoolExecutor

from config import Config
from collectors.base import BaseCollector
# Will import specific collectors here
from collectors.mock import MockCollector
from collectors.mgmt_console import MgmtConsoleCollector
from collectors.native import NativeGCBDRCollector
from exporters.influx import InfluxExporter

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("gcbdr.main")

class GCBDRMonitor:
    def __init__(self):
        self.running = True
        self.collectors = []
        try:
            self.exporter = InfluxExporter()
        except Exception:
            logger.warning("InfluxDB not available. Metrics will strictly be logged only (if implemented in exporters).")
            # In a real scenario we might want to crash or have a DummyExporter
            # For now let's let it crash if export is called, or handle it
            pass
        
        # Initialize Collectors
        if Config.MOCK_MODE:
            logger.info("Mock Mode enabled. Registering MockCollector.")
            self.collectors.append(MockCollector())
        
        if Config.MGMT_CONSOLE_ENDPOINT:
            self.collectors.append(MgmtConsoleCollector())
        
        if Config.GOOGLE_CLOUD_PROJECT:
            self.collectors.append(NativeGCBDRCollector())

    def register_signal_handlers(self):
        signal.signal(signal.SIGINT, self.handle_exit)
        signal.signal(signal.SIGTERM, self.handle_exit)

    def handle_exit(self, signum, frame):
        logger.info("Received exit signal. Shutting down...")
        self.running = False

    def run(self):
        self.register_signal_handlers()
        logger.info(f"Starting GCBDR Monitor. Poll Interval: {Config.POLL_INTERVAL_SECONDS}s")
        
        while self.running:
            start_time = time.time()
            
            all_metrics = []
            
            # Collect in parallel if we have many collectors, but sequential is fine for now
            for collector in self.collectors:
                try:
                    logger.info(f"Collecting from {collector.name}...")
                    metrics = collector.collect()
                    all_metrics.extend(metrics)
                    logger.info(f"Collected {len(metrics)} metrics from {collector.name}")
                except Exception as e:
                    logger.error(f"Error collecting from {collector.name}: {e}", exc_info=True)
            
            # Export
            if all_metrics:
                try:
                    self.exporter.export(all_metrics)
                    logger.info(f"Exported {len(all_metrics)} metrics")
                except Exception as e:
                    logger.error(f"Error exporting metrics: {e}", exc_info=True)
            
            # Sleep
            if Config.SINGLE_RUN:
                logger.info("Single run mode enabled. Exiting.")
                self.running = False
                break

            elapsed = time.time() - start_time
            sleep_time = max(0, Config.POLL_INTERVAL_SECONDS - elapsed)
            time.sleep(sleep_time)

if __name__ == "__main__":
    monitor = GCBDRMonitor()
    monitor.run()
