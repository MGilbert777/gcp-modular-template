from abc import ABC, abstractmethod
from typing import Any
from .interfaces import LoggerProtocol
from .logger_factory import get_logger

class GCPBasePipeline(ABC):
    """
    Abstract base class for modular GCP pipelines.
    
    Args:
        logger: Any object implementing LoggerProtocol.
    """
    def __init__(self, service_name: str, logger: LoggerProtocol = None):
        self.logger = logger or get_logger(service_name)
        self.service_name = service_name

    @abstractmethod
    def extract(self) -> Any: ...

    @abstractmethod
    def transform(self, data: Any) -> Any: ...

    @abstractmethod
    def load(self, data: Any) -> None: ...
