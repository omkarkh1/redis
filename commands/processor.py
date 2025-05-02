"""
Redis Command Processor

This module implements the command processing system for Redis commands.
It includes:
1. A data store for in-memory key-value storage
2. Command handlers for Redis commands
3. A dispatcher to route commands to appropriate handlers

The processor takes parsed RESP commands from clients, interprets them,
executes the appropriate operations on the data store, and returns
formatted responses.

Author: omkarkh1
"""

import time
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
import os
import sys
from threading import RLock

# Add parent directory to sys.path to resolve imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.logging_config import get_logger, LoggerAdapter
from commands.validator import RedisCommandValidator

# Set up logger
logger = get_logger(__name__)

# Type aliases for clarity
RedisValue = Union[str, bytes, int, float, None, List[Any]]
CommandHandler = Callable[['RedisDataStore', List[Any]], Any]


class RedisDataStore:
    """
    In-memory data store for Redis
    
    This class manages the key-value storage for Redis, providing methods
    for data manipulation and retrieval. It handles different data types
    and implements key expiration.
    """
    
    def __init__(self):
        """Initialize the Redis data store"""
        # Main data storage dictionary
        self.data: Dict[str, Any] = {}
        # Dictionary to track expiration times for keys
        self.expires: Dict[str, float] = {}
        # Lock for thread safety
        self.lock = RLock()
        
        self.logger = LoggerAdapter(logger, {"component": "DataStore"})
        self.logger.info("Redis data store initialized")
        print("Redis data store initialized and ready")
    
    def get(self, key: str) -> Optional[Any]:
        """
        Get the value associated with a key
        
        Args:
            key: The key to look up
            
        Returns:
            The value associated with the key, or None if not found
        """
        with self.lock:
            # Check if the key exists and is not expired
            self.logger.debug(f"Getting value for key: {key}")
            if self._check_expiry(key):
                value = self.data.get(key)
                value_type = type(value).__name__
                value_preview = str(value)[:50] + "..." if isinstance(value, (str, bytes)) and len(str(value)) > 50 else value
                self.logger.debug(f"Found key: {key}, type: {value_type}, value: {value_preview}")
                print(f"GET {key}: Found (type: {value_type})")
                return value
            
            self.logger.debug(f"Key not found: {key}")
            print(f"GET {key}: Not found")
            return None
    
    def set(self, key: str, value: Any, expiry: Optional[float] = None) -> bool:
        """
        Set a key-value pair, optionally with an expiration time
        
        Args:
            key: The key to set
            value: The value to associate with the key
            expiry: Optional expiration time in seconds from now
            
        Returns:
            True if successful
        """
        with self.lock:
            value_type = type(value).__name__
            value_preview = str(value)[:50] + "..." if isinstance(value, (str, bytes)) and len(str(value)) > 50 else value
            self.logger.debug(f"Setting key: {key}, type: {value_type}, value: {value_preview}")
            
            self.data[key] = value
            
            # Set expiration if provided
            if expiry is not None:
                expiry_time = time.time() + expiry
                self.expires[key] = expiry_time
                expiry_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(expiry_time))
                self.logger.debug(f"Set key: {key} with expiry in {expiry} seconds (until {expiry_str})")
                print(f"SET {key}: Success with expiry of {expiry} seconds")
            else:
                # Remove any existing expiration
                if key in self.expires:
                    del self.expires[key]
                    self.logger.debug(f"Removed existing expiry for key: {key}")
                self.logger.debug(f"Set key: {key} (no expiry)")
                print(f"SET {key}: Success (no expiry)")
                
            return True
    
    def delete(self, *keys: str) -> int:
        """
        Delete one or more keys
        
        Args:
            keys: Keys to delete
            
        Returns:
            Number of keys deleted
        """
        count = 0
        with self.lock:
            for key in keys:
                if key in self.data:
                    del self.data[key]
                    if key in self.expires:
                        del self.expires[key]
                    count += 1
                    logger.debug(f"Deleted key: {key}")
        
        return count
    
    def exists(self, *keys: str) -> int:
        """
        Check if one or more keys exist
        
        Args:
            keys: Keys to check
            
        Returns:
            Number of existing keys
        """
        count = 0
        with self.lock:
            for key in keys:
                if self._check_expiry(key):
                    count += 1
        
        logger.debug(f"Checked existence of {len(keys)} keys, found {count}")
        return count
    
    def expire(self, key: str, seconds: int) -> bool:
        """
        Set a key's time to live in seconds
        
        Args:
            key: The key to set expiration for
            seconds: Time to live in seconds
            
        Returns:
            True if the timeout was set, False if key doesn't exist
        """
        with self.lock:
            if key in self.data:
                self.expires[key] = time.time() + seconds
                logger.debug(f"Set expiry for key: {key} to {seconds} seconds")
                return True
            
            logger.debug(f"Failed to set expiry for non-existent key: {key}")
            return False
    
    def ttl(self, key: str) -> int:
        """
        Get the time to live for a key in seconds
        
        Args:
            key: The key to check
            
        Returns:
            TTL in seconds, or -1 if the key exists but has no TTL,
            or -2 if the key doesn't exist
        """
        with self.lock:
            if key not in self.data:
                return -2
            
            if key not in self.expires:
                return -1
            
            ttl = int(self.expires[key] - time.time())
            # If TTL has expired but not yet cleaned up
            if ttl <= 0:
                self.delete(key)
                return -2
            
            logger.debug(f"TTL for key {key}: {ttl} seconds")
            return ttl
    
    def _check_expiry(self, key: str) -> bool:
        """
        Check if a key exists and is not expired
        
        Args:
            key: The key to check
            
        Returns:
            True if the key exists and is not expired, False otherwise
        """
        # Key doesn't exist
        if key not in self.data:
            self.logger.debug(f"Key doesn't exist: {key}")
            return False
        
        # Key exists but has no expiration
        if key not in self.expires:
            self.logger.debug(f"Key exists with no expiry: {key}")
            return True
        
        # Check if key has expired
        current_time = time.time()
        expiry_time = self.expires[key]
        time_left = expiry_time - current_time
        
        if current_time > expiry_time:
            # Key has expired, delete it
            self.delete(key)
            self.logger.debug(f"Key expired and deleted: {key} (expired {abs(time_left):.2f} seconds ago)")
            return False
        
        # Key exists and has not expired
        self.logger.debug(f"Key exists and not expired: {key} (expires in {time_left:.2f} seconds)")
        return True
    
    def flush_all(self) -> bool:
        """
        Delete all keys in the current database
        
        Returns:
            True if successful
        """
        with self.lock:
            self.data.clear()
            self.expires.clear()
            logger.info("Database flushed")
            return True


class RedisCommandDispatcher:
    """
    Redis command dispatcher
    
    This class is responsible for:
    1. Routing commands to the appropriate handler functions
    2. Validating command arguments
    3. Formatting responses according to the Redis protocol
    """
    
    def __init__(self, data_store: RedisDataStore):
        """
        Initialize the command dispatcher
        
        Args:
            data_store: The Redis data store instance
        """
        self.data_store = data_store
        # Command handlers dictionary: maps command names to handler functions
        self.handlers: Dict[str, CommandHandler] = {}
        # Command validator
        self.validator = RedisCommandValidator()
        # Register all the command handlers
        self._register_handlers()
        
        self.logger = LoggerAdapter(logger, {"component": "Dispatcher"})
        self.logger.info("Command dispatcher initialized with handlers")
        print("Command dispatcher ready to process requests")
    
    def _register_handlers(self) -> None:
        """Register all command handlers"""
        # String commands
        self.handlers["GET"] = self._handle_get
        self.handlers["SET"] = self._handle_set
        self.handlers["DEL"] = self._handle_del
        self.handlers["EXISTS"] = self._handle_exists
        
        # Key expiry commands
        self.handlers["EXPIRE"] = self._handle_expire
        self.handlers["TTL"] = self._handle_ttl
        
        # Server commands
        self.handlers["PING"] = self._handle_ping
        self.handlers["FLUSHALL"] = self._handle_flushall
        
        # Add new command handlers here
        logger.debug(f"Registered {len(self.handlers)} command handlers")
    
    def dispatch(self, command: List[Any]) -> Any:
        """
        Dispatch a command to the appropriate handler
        
        Args:
            command: List containing the command name and arguments
            
        Returns:
            Result of the command execution
            
        Raises:
            ValueError: If the command is invalid or not supported
        """
        if not command:
            self.logger.warning("Empty command received")
            print("Error: Empty command received")
            return "-ERR empty command"
        
        # Extract the command name (first element) and convert to uppercase
        cmd_bytes = command[0]
        
        # Convert command name to string if it's bytes
        if isinstance(cmd_bytes, bytes):
            cmd_name = cmd_bytes.decode('utf-8').upper()
        else:
            cmd_name = str(cmd_bytes).upper()
        
        # Extract arguments (remaining elements)
        args = command[1:] if len(command) > 1 else []
        
        # Format args for logging (decode bytes if needed)
        log_args = []
        for arg in args:
            if isinstance(arg, bytes):
                try:
                    log_args.append(arg.decode('utf-8'))
                except UnicodeDecodeError:
                    log_args.append(f"<binary data of length {len(arg)}>")
            else:
                log_args.append(str(arg))
        
        self.logger.info(f"Received command: {cmd_name} with arguments: {log_args}")
        print(f"Processing: {cmd_name} {' '.join(str(a) for a in log_args[:3])}{'...' if len(log_args) > 3 else ''}")
        
        # Validate the command
        is_valid, error_msg = self.validator.validate_command(command)
        if not is_valid:
            self.logger.warning(f"Command validation failed: {error_msg}")
            print(f"Validation error: {error_msg}")
            return f"-ERR {error_msg}"
        
        # Check if the command is supported
        if cmd_name not in self.handlers:
            self.logger.warning(f"Unknown command: {cmd_name}")
            print(f"Error: Unknown command '{cmd_name}'")
            return f"-ERR unknown command '{cmd_name}'"
        
        # Get the handler function
        handler = self.handlers[cmd_name]
        
        try:
            # Execute the handler with the data store and arguments
            self.logger.debug(f"Executing handler for {cmd_name}")
            start_time = time.time()
            result = handler(self.data_store, args)
            execution_time = (time.time() - start_time) * 1000  # Convert to milliseconds
            
            result_preview = str(result)[:50] + "..." if isinstance(result, (str, bytes)) and len(str(result)) > 50 else result
            self.logger.info(f"Command {cmd_name} completed in {execution_time:.2f}ms, result: {result_preview}")
            print(f"Command {cmd_name} executed in {execution_time:.2f}ms")
            
            return result
        except Exception as e:
            # Log the error and return an error message
            self.logger.error(f"Error executing command {cmd_name}: {e}", exc_info=True)
            print(f"Error executing {cmd_name}: {e}")
            return f"-ERR {str(e)}"
    
    # String command handlers
    
    def _handle_get(self, store: RedisDataStore, args: List[Any]) -> Any:
        """Handle GET command"""
        if len(args) != 1:
            return "-ERR wrong number of arguments for 'get' command"
        
        key = args[0].decode('utf-8') if isinstance(args[0], bytes) else str(args[0])
        value = store.get(key)
        
        # Return the value or nil if key doesn't exist
        return value
    
    def _handle_set(self, store: RedisDataStore, args: List[Any]) -> str:
        """Handle SET command"""
        if len(args) < 2:
            return "-ERR wrong number of arguments for 'set' command"
        
        key = args[0].decode('utf-8') if isinstance(args[0], bytes) else str(args[0])
        value = args[1]
        
        # Check for optional EX/PX arguments
        expiry = None
        
        if len(args) > 2:
            i = 2
            while i < len(args):
                option = args[i].decode('utf-8').upper() if isinstance(args[i], bytes) else str(args[i]).upper()
                
                if option == "EX" and i + 1 < len(args):
                    # EX seconds -- Set the specified expire time, in seconds
                    seconds = int(args[i+1].decode('utf-8')) if isinstance(args[i+1], bytes) else int(args[i+1])
                    expiry = float(seconds)
                    i += 2
                elif option == "PX" and i + 1 < len(args):
                    # PX milliseconds -- Set the specified expire time, in milliseconds
                    milliseconds = int(args[i+1].decode('utf-8')) if isinstance(args[i+1], bytes) else int(args[i+1])
                    expiry = float(milliseconds) / 1000.0
                    i += 2
                else:
                    return f"-ERR syntax error"
        
        store.set(key, value, expiry)
        return "OK"
    
    def _handle_del(self, store: RedisDataStore, args: List[Any]) -> int:
        """Handle DEL command"""
        if not args:
            return "-ERR wrong number of arguments for 'del' command"
        
        keys = [arg.decode('utf-8') if isinstance(arg, bytes) else str(arg) for arg in args]
        return store.delete(*keys)
    
    def _handle_exists(self, store: RedisDataStore, args: List[Any]) -> int:
        """Handle EXISTS command"""
        if not args:
            return "-ERR wrong number of arguments for 'exists' command"
        
        keys = [arg.decode('utf-8') if isinstance(arg, bytes) else str(arg) for arg in args]
        return store.exists(*keys)
    
    # Key expiry command handlers
    
    def _handle_expire(self, store: RedisDataStore, args: List[Any]) -> int:
        """Handle EXPIRE command"""
        if len(args) != 2:
            return "-ERR wrong number of arguments for 'expire' command"
        
        key = args[0].decode('utf-8') if isinstance(args[0], bytes) else str(args[0])
        seconds = int(args[1].decode('utf-8')) if isinstance(args[1], bytes) else int(args[1])
        
        return 1 if store.expire(key, seconds) else 0
    
    def _handle_ttl(self, store: RedisDataStore, args: List[Any]) -> int:
        """Handle TTL command"""
        if len(args) != 1:
            return "-ERR wrong number of arguments for 'ttl' command"
        
        key = args[0].decode('utf-8') if isinstance(args[0], bytes) else str(args[0])
        return store.ttl(key)
    
    # Server command handlers
    
    def _handle_ping(self, store: RedisDataStore, args: List[Any]) -> str:
        """Handle PING command"""
        if not args:
            return "PONG"
        
        if len(args) > 1:
            return "-ERR wrong number of arguments for 'ping' command"
        
        # Return the argument as the response
        message = args[0].decode('utf-8') if isinstance(args[0], bytes) else str(args[0])
        return message
    
    def _handle_flushall(self, store: RedisDataStore, args: List[Any]) -> str:
        """Handle FLUSHALL command"""
        store.flush_all()
        return "OK"
