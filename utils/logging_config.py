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
- Multiple formatter styles for different use cases

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
# Basic formats (existing)
DETAILED_FORMAT = '%(asctime)s [%(levelname)8s] %(name)s:%(lineno)d - %(message)s'
SIMPLE_FORMAT = '%(asctime)s [%(levelname)8s] %(message)s'
CONTEXT_FORMAT = '%(asctime)s [%(levelname)8s] [%(context)s] %(name)s:%(lineno)d - %(message)s'

# New formatter styles for different use cases
# 1. Compact formatter - minimalist logs
COMPACT_FORMAT = '%(asctime)s.%(msecs)03d|%(levelname).1s|%(message)s'

# 2. Full debug formatter - with thread info for concurrent debugging
DEBUG_FORMAT = '%(asctime)s [%(levelname)8s] [%(threadName)s:%(thread)d] [%(processName)s:%(process)d] %(name)s:%(lineno)d - %(message)s'

# 3. JSON-like formatter - structured format easier to parse
JSON_FORMAT = '{"time": "%(asctime)s", "level": "%(levelname)s", "name": "%(name)s", "line": %(lineno)d, "message": "%(message)s"}'

# 4. Performance formatter - includes function name and execution time placeholder
PERFORMANCE_FORMAT = '%(asctime)s [%(levelname)8s] %(name)s.%(funcName)s:%(lineno)d - [exec_time=%(exec_time)s] %(message)s'

# 5. Network traffic formatter - for protocol/network debugging
NETWORK_FORMAT = '%(asctime)s [%(levelname)8s] [%(ip)s:%(port)s] %(name)s - %(message)s'

# 6. Command formatter - for Redis commands
COMMAND_FORMAT = '%(asctime)s [%(levelname)8s] [cmd=%(cmd)s] [client=%(client)s] %(message)s'

# 7. Error formatter - includes exception info
ERROR_FORMAT = '%(asctime)s [%(levelname)8s] %(name)s:%(lineno)d - %(message)s\nException: %(exc_info)s\nStack: %(stack_info)s'

# 8. Database operations formatter
DATABASE_FORMAT = '%(asctime)s [%(levelname)8s] [db=%(db_name)s] [op=%(operation)s] [key=%(key)s] - %(message)s'

# 9. Security audit formatter
SECURITY_FORMAT = '%(asctime)s [%(levelname)8s] [user=%(user)s] [ip=%(ip)s] [action=%(action)s] - %(message)s'

# 10. Server status formatter
SERVER_FORMAT = '%(asctime)s [%(levelname)8s] [uptime=%(uptime)s] [mem=%(memory_usage)s] [clients=%(client_count)s] - %(message)s'

# Time format for logs
TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

# ANSI color codes for colored output
COLORS = {
    'RESET': '\033[0m',
    'RED': '\033[31m',
    'GREEN': '\033[32m',
    'YELLOW': '\033[33m',
    'BLUE': '\033[34m',
    'MAGENTA': '\033[35m',
    'CYAN': '\033[36m',
    'WHITE': '\033[37m',
    'BOLD': '\033[1m'
}


class ColoredFormatter(logging.Formatter):
    """
    A custom formatter that adds colors to log level names in console output.
    """
    LEVEL_COLORS = {
        logging.DEBUG: COLORS['BLUE'],
        logging.INFO: COLORS['GREEN'],
        logging.WARNING: COLORS['YELLOW'],
        logging.ERROR: COLORS['RED'],
        logging.CRITICAL: COLORS['BOLD'] + COLORS['RED'],
    }
    
    def __init__(self, fmt=None, datefmt=None, style='%'):
        super().__init__(fmt, datefmt, style)
    
    def format(self, record):
        # Save the original levelname
        original_levelname = record.levelname
        
        # Add color to the levelname
        if record.levelno in self.LEVEL_COLORS:
            record.levelname = f"{self.LEVEL_COLORS[record.levelno]}{original_levelname}{COLORS['RESET']}"
        
        # Format the record
        result = super().format(record)
        
        # Restore the original levelname
        record.levelname = original_levelname
        
        return result


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
        # Create a context string from the extra information
        extra_copy = self.extra.copy()
        context_parts = []
        
        for key, value in extra_copy.items():
            if value is not None:  # Only include non-None values
                context_parts.append(f"{key}={value}")
        
        # Add the context to the message
        context_str = " ".join(context_parts)
        
        # Add the context to the kwargs as a special field for the formatter
        kwargs_copy = kwargs.copy() if kwargs else {}
        if 'extra' not in kwargs_copy:
            kwargs_copy['extra'] = {}
        
        # Add 'context' to the extra dict for the formatter to use
        kwargs_copy['extra']['context'] = context_str
        
        return msg, kwargs_copy


def get_logger(
    name: str, 
    console_level: int = DEFAULT_CONSOLE_LEVEL,
    file_level: int = DEFAULT_FILE_LEVEL,
    log_file: Optional[str] = None,
    max_file_size: int = 10 * 1024 * 1024,  # 10 MB
    backup_count: int = 5,
    use_colored_output: bool = True
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
        use_colored_output: Whether to use colors in console output
        
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
    context_formatter = logging.Formatter(CONTEXT_FORMAT, TIME_FORMAT)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(console_level)
    
    if use_colored_output:
        console_formatter = ColoredFormatter(SIMPLE_FORMAT, TIME_FORMAT)
    else:
        console_formatter = logging.Formatter(SIMPLE_FORMAT, TIME_FORMAT)
    
    console_handler.setFormatter(console_formatter)
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
    file_handler.setFormatter(context_formatter)
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
