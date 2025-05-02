"""
Redis Project Logging Configuration

This module provides a centralized configuration for logging throughout the Redis project.
It sets up formatters, handlers, and configurable log levels to enable comprehensive
debugging while keeping logs organized.

Features:
- Console logging with color-coded levels
- File logging with rotation
- Configurable log levels per module
- Detailed formatting with timestamps and source information

Author: omkarkh1
"""

import logging
import logging.handlers
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Union

# Default log directory - create logs in the project directory
DEFAULT_LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')

# Ensure log directory exists
os.makedirs(DEFAULT_LOG_DIR, exist_ok=True)

# Default log levels
DEFAULT_CONSOLE_LEVEL = logging.INFO
DEFAULT_FILE_LEVEL = logging.DEBUG

# Log format constants
DETAILED_FORMAT = '%(asctime)s [%(levelname)8s] %(name)s:%(lineno)d - %(message)s'
SIMPLE_FORMAT = '%(asctime)s [%(levelname)8s] %(message)s'

# Time format for logs
TIME_FORMAT = '%Y-%m-%d %H:%M:%S'


def get_logger(
    name: str, 
    console_level: int = DEFAULT_CONSOLE_LEVEL,
    file_level: int = DEFAULT_FILE_LEVEL,
    log_file: Optional[str] = None,
    max_file_size: int = 10 * 1024 * 1024,  # 10 MB
    backup_count: int = 5
) -> logging.Logger:
    """
    Get a configured logger with the specified name and settings.
    
    Args:
        name: Name of the logger, typically __name__ of the calling module
        console_level: Logging level for console output
        file_level: Logging level for file output
        log_file: Custom log file path (default: based on logger name)
        max_file_size: Maximum size of each log file before rotation
        backup_count: Number of backup log files to keep
        
    Returns:
        A configured logger instance
    """
    # Create logger
    logger = logging.getLogger(name)
    
    # If logger is already configured, return it
    if logger.handlers:
        return logger
    
    # Set the logger's level to the most verbose of the handlers
    # to ensure all messages reach the handlers for filtering
    logger.setLevel(min(console_level, file_level))
    
    # Create formatters
    detailed_formatter = logging.Formatter(DETAILED_FORMAT, TIME_FORMAT)
    simple_formatter = logging.Formatter(SIMPLE_FORMAT, TIME_FORMAT)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(console_level)
    console_handler.setFormatter(simple_formatter)
    logger.addHandler(console_handler)
    
    # File handler (with rotation)
    if log_file is None:
        # Create a log file name based on the logger name
        module_name = name.split('.')[-1]
        log_file = os.path.join(DEFAULT_LOG_DIR, f"{module_name}.log")
    
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=max_file_size,
        backupCount=backup_count,
        encoding='utf-8'
    )
    file_handler.setLevel(file_level)
    file_handler.setFormatter(detailed_formatter)
    logger.addHandler(file_handler)
    
    return logger


def configure_root_logger(
    console_level: int = DEFAULT_CONSOLE_LEVEL,
    file_level: int = DEFAULT_FILE_LEVEL,
    log_file: str = os.path.join(DEFAULT_LOG_DIR, 'redis.log')
) -> None:
    """
    Configure the root logger for application-wide settings.
    
    Args:
        console_level: Logging level for console output
        file_level: Logging level for file output
        log_file: Path to the main log file
    """
    # Configure the root logger
    root_logger = get_logger(
        'redis', 
        console_level=console_level,
        file_level=file_level,
        log_file=log_file
    )
    
    # Set as the root logger
    logging.root = root_logger


class LoggerAdapter(logging.LoggerAdapter):
    """
    A logger adapter that adds context information to log messages.
    
    This adapter allows adding contextual information (like client info or request IDs)
    to log messages without changing the logging calls throughout the code.
    """
    
    def __init__(self, logger: logging.Logger, extra: Optional[Dict] = None):
        """
        Initialize the adapter with a logger and optional extra context.
        
        Args:
            logger: The underlying logger instance
            extra: Dictionary of extra contextual information
        """
        super().__init__(logger, extra or {})
    
    def process(self, msg, kwargs):
        """
        Process the log message by adding contextual information.
        
        Args:
            msg: The log message
            kwargs: Additional keyword arguments for the logging method
            
        Returns:
            Tuple of (modified_message, modified_kwargs)
        """
        # Format the message with the extra context information
        context_items = []
        for key, value in self.extra.items():
            if value is not None:  # Only include non-None values
                context_items.append(f"{key}={value}")
        
        # If we have context items, prepend them to the message
        if context_items:
            context_str = " ".join(context_items)
            msg = f"[{context_str}] {msg}"
        
        return msg, kwargs
