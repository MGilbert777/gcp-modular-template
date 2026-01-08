from abc import ABC, abstractmethod
from typing import Any
from .interfaces import LoggerProtocol
from .logger_factory import get_logger

class GCPBasePipeline(ABC):
    """
    Abstract base class for modular GCP pipelines.
    
    Args:
        service_name: Name of the service for logging context.
        logger: Any object implementing LoggerProtocol.
    """
    def __init__(self, service_name: str, logger: LoggerProtocol = None):
        self.logger = logger or get_logger(service_name)
        self.service_name = service_name

    @abstractmethod
    def extract(self) -> Any:
        """Extract data from source."""
        pass

    @abstractmethod
    def transform(self, data: Any) -> Any:
        """Apply business logic."""
        pass

    @abstractmethod
    def load(self, data: Any) -> None:
        """Load data into destination."""
        pass

    def run(self) -> None:
        """Orchestrates the pipeline execution flow."""
        self.logger.info(f"Starting pipeline: {self.service_name}")
        try:
            data = self.extract()
            transformed_data = self.transform(data)
            self.load(transformed_data)
            self.logger.info(f"Pipeline {self.service_name} completed successfully.")
        except Exception as e:
            self.logger.error(f"Pipeline {self.service_name} failed: {str(e)}")
            raise