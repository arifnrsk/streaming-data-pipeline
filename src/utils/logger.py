"""Logging utility for the streaming data pipeline."""

import logging
import structlog
from typing import Any
from src.utils.config_loader import ConfigLoader


def setup_logging(config_loader: ConfigLoader = None) -> structlog.BoundLogger:
    """Setup structured logging configuration.
    
    Args:
        config_loader (ConfigLoader, optional): Configuration loader instance
        
    Returns:
        structlog.BoundLogger: Configured logger instance
    """
    if config_loader is None:
        config_loader = ConfigLoader()
        
    logging_config = config_loader.get_logging_config()
    log_level = logging_config.get('level', 'INFO')
    log_format = logging_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Configure standard logging
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format=log_format,
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Configure structlog
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    return structlog.get_logger()


def get_logger(name: str) -> structlog.BoundLogger:
    """Get a logger instance with the specified name.
    
    Args:
        name (str): Logger name
        
    Returns:
        structlog.BoundLogger: Logger instance
    """
    return structlog.get_logger(name) 