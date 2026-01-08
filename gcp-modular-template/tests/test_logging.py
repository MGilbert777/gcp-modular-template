import os
from interfaces import LoggerProtocol
from logger_factory import get_logger
from base_pipeline import GCPBasePipeline

class TestPipeline(GCPBasePipeline):
    """A minimal implementation of the base pipeline for testing."""
    
    def extract(self):
        self.logger.info("Extracting test data...")
        return {"data": 123}

    def transform(self, data):
        self.logger.debug(f"Transforming data: {data}")
        return data

    def load(self, data):
        self.logger.info(f"Loading data to destination.")

def run_test():
    # 1. Test with Loguru (The default)
    print("--- Testing with Loguru ---")
    loguru_logger = get_logger(service_type="pipeline", library="loguru")
    pipeline_a = TestPipeline(service_name="loguru_service", logger=loguru_logger)
    pipeline_a.run()

    print("\n" + "="*30 + "\n")

    # 2. Test with Standard Python Logging
    # Note: Business logic (TestPipeline) does not change at all.
    print("--- Testing with Standard Logging ---")
    std_logger = get_logger(service_type="api", library="standard")
    pipeline_b = TestPipeline(service_name="std_service", logger=std_logger)
    pipeline_b.run()

if __name__ == "__main__":
    # Ensure a local .env exists or mock the env var
    os.environ.setdefault("LOG_LEVEL", "DEBUG")
    run_test()