from abc import ABC, abstractmethod
from typing import List, Dict, Any
import logging

class Metric(ABC):
    def __init__(self, name: str, tags: Dict[str, str], fields: Dict[str, Any], timestamp: float = None):
        self.name = name
        self.tags = tags
        self.fields = fields
        self.timestamp = timestamp

class BaseCollector(ABC):
    def __init__(self, name: str):
        self.name = name
        self.logger = logging.getLogger(f"gcbdr.collector.{name}")

    @abstractmethod
    def collect(self) -> List[Metric]:
        """
        Collect metrics and return a list of Metric objects.
        """
        pass
