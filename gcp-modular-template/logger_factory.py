import os
import sys
import logging
from loguru import logger
from dotenv import load_dotenv
from .interfaces import LoggerProtocol

def get_logger(service_type: str = "pipeline", library: str = "loguru") -> LoggerProtocol:
    """
    Factory to return a modular logger.
    """
    load_dotenv()
    
    if library == "loguru":
        logger.remove()
        log_format = "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan> - <level>{message}</level>"
        logger.add(sys.stdout, format=log_format, level=os.getenv("LOG_LEVEL", "INFO"))
        return logger # Loguru naturally satisfies the Protocol
    
    # Fallback to standard library logging
    std_logger = logging.getLogger(service_type)
    if not std_logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        std_logger.addHandler(handler)
        std_logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))
    return std_logger
