"""
Redis Command Validator

This module provides validation for Redis commands to ensure they are correctly
formatted and have the appropriate arguments before execution.

The validator checks command syntax, argument counts, and data types to prevent
errors during command processing.

Author: omkarkh1
"""

import os
import sys
from typing import Any, Dict, List, Optional, Tuple, Union, Set

# Add parent directory to sys.path to resolve imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.logging_config import get_logger, LoggerAdapter

# Set up logger
logger = get_logger(__name__)

class ValidationError(Exception):
    """Exception raised for command validation errors"""
    pass

class RedisCommandValidator:
    """
    Validator for Redis commands
    
    This class validates Redis commands before they are executed,
    ensuring they have the correct format, number of arguments,
    and argument types.
    """
    
    def __init__(self):
        """Initialize the command validator"""
        self.logger = LoggerAdapter(logger, {"component": "Validator"})
        
        # Command specifications: command name -> (min_args, max_args, description)
        # max_args of -1 means unlimited arguments
        self.command_specs: Dict[str, Tuple[int, int, str]] = {
            "GET": (1, 1, "GET key"),
            "SET": (2, -1, "SET key value [EX seconds|PX milliseconds]"),
            "DEL": (1, -1, "DEL key [key ...]"),
            "EXISTS": (1, -1, "EXISTS key [key ...]"),
            "EXPIRE": (2, 2, "EXPIRE key seconds"),
            "TTL": (1, 1, "TTL key"),
            "PING": (0, 1, "PING [message]"),
            "FLUSHALL": (0, 0, "FLUSHALL"),
        }
        
        # Argument validators for specific commands
        self.validators = {
            "SET": self._validate_set_args,
            "EXPIRE": self._validate_expire_args,
        }
        
        self.logger.info(f"Command validator initialized with {len(self.command_specs)} command specifications")
    
    def validate_command(self, command: List[Any]) -> Tuple[bool, Optional[str]]:
        """
        Validate a Redis command
        
        Args:
            command: The command list to validate (command name and arguments)
            
        Returns:
            Tuple of (is_valid, error_message)
            - is_valid: True if command is valid, False otherwise
            - error_message: None if command is valid, error message otherwise
        """
        self.logger.debug(f"Validating command: {command}")
        
        if not command:
            self.logger.warning("Empty command received")
            return False, "Empty command"
        
        # Extract the command name
        cmd_bytes = command[0]
        
        # Convert command name to string if it's bytes
        if isinstance(cmd_bytes, bytes):
            cmd_name = cmd_bytes.decode('utf-8').upper()
        else:
            cmd_name = str(cmd_bytes).upper()
        
        # Extract arguments
        args = command[1:] if len(command) > 1 else []
        
        self.logger.debug(f"Command: {cmd_name}, Arguments: {args}")
        
        # Check if command exists
        if cmd_name not in self.command_specs:
            self.logger.warning(f"Unknown command: {cmd_name}")
            return False, f"Unknown command '{cmd_name}'"
        
        # Get command specification
        min_args, max_args, cmd_syntax = self.command_specs[cmd_name]
        
        # Check argument count
        if len(args) < min_args:
            self.logger.warning(f"Too few arguments for '{cmd_name}': {len(args)} < {min_args}")
            return False, f"Wrong number of arguments for '{cmd_name}' command"
        
        if max_args != -1 and len(args) > max_args:
            self.logger.warning(f"Too many arguments for '{cmd_name}': {len(args)} > {max_args}")
            return False, f"Wrong number of arguments for '{cmd_name}' command"
        
        # Run command-specific validator if available
        if cmd_name in self.validators:
            self.logger.debug(f"Running specific validator for {cmd_name}")
            is_valid, error_msg = self.validators[cmd_name](args)
            if not is_valid:
                return False, error_msg
        
        self.logger.info(f"Command '{cmd_name}' validated successfully")
        return True, None
    
    def _validate_set_args(self, args: List[Any]) -> Tuple[bool, Optional[str]]:
        """
        Validate arguments for the SET command
        
        SET key value [EX seconds|PX milliseconds]
        
        Args:
            args: Command arguments
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        self.logger.debug(f"Validating SET arguments: {args}")
        
        # Basic validation already done (min 2 args)
        if len(args) > 2:
            # Check optional arguments
            i = 2
            while i < len(args):
                option = args[i].decode('utf-8').upper() if isinstance(args[i], bytes) else str(args[i]).upper()
                
                if option in ["EX", "PX"]:
                    # Check if value is provided
                    if i + 1 >= len(args):
                        self.logger.warning(f"Missing value for {option} in SET command")
                        return False, f"Syntax error: missing value for {option}"
                    
                    # Check if value is an integer
                    try:
                        value = args[i+1]
                        if isinstance(value, bytes):
                            int(value.decode('utf-8'))
                        else:
                            int(value)
                    except ValueError:
                        self.logger.warning(f"Invalid {option} value, not an integer: {args[i+1]}")
                        return False, f"Value for {option} must be an integer"
                    
                    i += 2
                else:
                    self.logger.warning(f"Unknown SET option: {option}")
                    return False, f"Syntax error: unknown option {option}"
        
        self.logger.debug("SET arguments validated successfully")
        return True, None
    
    def _validate_expire_args(self, args: List[Any]) -> Tuple[bool, Optional[str]]:
        """
        Validate arguments for the EXPIRE command
        
        EXPIRE key seconds
        
        Args:
            args: Command arguments
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        self.logger.debug(f"Validating EXPIRE arguments: {args}")
        
        # Check if seconds is an integer
        try:
            seconds = args[1]
            if isinstance(seconds, bytes):
                int(seconds.decode('utf-8'))
            else:
                int(seconds)
        except ValueError:
            self.logger.warning(f"Invalid seconds value, not an integer: {args[1]}")
            return False, "Value for seconds must be an integer"
        
        self.logger.debug("EXPIRE arguments validated successfully")
        return True, None
