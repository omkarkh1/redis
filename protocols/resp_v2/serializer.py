"""
Redis Serialization Protocol (RESP) Version 2 Serializer

This module implements the serialization part of RESP v2 protocol for Redis client-server communication.
It includes functionality for serializing Python objects into RESP byte streams.

RESP is a binary-safe protocol that uses type-prefixed data formats to represent different
data types efficiently. This serializer converts common Python data types into their
RESP wire format representations following these encoding rules:

1. Simple Strings: Not directly serialized, bulk strings are used instead
2. Errors: Exceptions are serialized with '-' prefix
3. Integers: Python integers are serialized with ':' prefix
4. Bulk Strings: Strings and bytes are serialized with '$' prefix and length
5. Arrays: Lists and tuples are serialized with '*' prefix and length

Author: omkarkh1
"""

# Import type annotations from typing module to enhance code readability and enable static type checking
# Any: Represents any type
# List: Represents a list of items of a specific type
# Optional: Represents a value that can be of a specific type or None
# Tuple: Represents a fixed-size collection of items potentially of different types
# Union: Represents a value that can be one of several types
from typing import Any, List, Optional, Tuple, Union
import os
import sys

# Import socket module for network communication using the standard BSD socket interface
# Used for synchronous socket operations in the send_resp function
import socket

# Import asyncio for asynchronous I/O operations
# Enables non-blocking network operations in the send_resp_async function
import asyncio

# Add parent directory to sys.path to resolve imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from utils.logging_config import get_logger

# Set up logger
logger = get_logger(__name__)

# RESP Protocol Constants - These define the byte markers used in the RESP wire format
# Each RESP data type is identified by a specific prefix byte
RESP_SIMPLE_STRING = b'+'  # Simple string marker - used for status replies (e.g., "OK")
RESP_ERROR = b'-'          # Error marker - used for error messages (e.g., "ERR unknown command")
RESP_INTEGER = b':'        # Integer marker - used for numeric values (e.g., increments, counts)
RESP_BULK_STRING = b'$'    # Bulk string marker - used for binary-safe strings with explicit length
RESP_ARRAY = b'*'          # Array marker - used for collections of other RESP types
RESP_CRLF = b'\r\n'        # Carriage return + line feed - terminates all RESP elements

def serialize_resp(data: Any) -> bytes:
    """
    Convert Python objects to RESP format
    
    This function implements the serialization logic for the Redis protocol, converting
    various Python data types to their RESP wire format. The function uses a recursive
    approach for complex types like lists and tuples (which become RESP Arrays).
    
    The serialization follows these type mapping rules:
    - None → Null bulk string ("$-1\r\n")
    - str → Bulk string ("$<length>\r\n<data>\r\n")
    - bytes → Bulk string ("$<length>\r\n<data>\r\n")
    - int → Integer (":<number>\r\n")
    - Exception → Error ("-<message>\r\n")
    - list/tuple → Array ("*<count>\r\n<elements...>")
    - Other types → Converted to string and serialized as bulk string
    
    Examples:
        serialize_resp("hello") → b"$5\r\nhello\r\n"
        serialize_resp(42) → b":42\r\n"
        serialize_resp(None) → b"$-1\r\n"
        serialize_resp([1, "two", None]) → b"*3\r\n:1\r\n$3\r\ntwo\r\n$-1\r\n"
        serialize_resp(Exception("error")) → b"-error\r\n"
    
    This function is particularly useful for:
    1. Building client requests to send to Redis
    2. Constructing custom server responses in a Redis-compatible application
    3. Testing Redis protocol implementations
    4. Implementing Redis protocol extensions
    
    Args:
        data: Python object to serialize
        
    Returns:
        Bytes in RESP format
        
    Raises:
        TypeError: If the data type is not supported
    """
    # Check if the data is None and return the RESP null bulk string representation
    # In RESP, null values are represented as bulk strings with length -1
    if data is None:
        # Null bulk string - combines the bulk string marker ($) with -1 and CRLF
        # This special format indicates absence of a value (null/nil)
        logger.debug("Serializing None to null bulk string")
        return RESP_BULK_STRING + b"-1" + RESP_CRLF
    
    # Check if the data is a string and convert it to a RESP bulk string
    elif isinstance(data, str):
        # First encode the string to UTF-8 bytes to ensure binary safety
        # Redis protocol requires data to be transmitted as bytes
        encoded = data.encode('utf-8')
        logger.debug(f"Serializing string of length {len(encoded)}")
        # Create the bulk string format: $<length>\r\n<data>\r\n
        # - Start with bulk string marker ($)
        # - Add the byte length of the encoded string (converted to ASCII bytes)
        # - Add CRLF delimiter
        # - Add the actual string bytes
        # - End with another CRLF delimiter
        return RESP_BULK_STRING + str(len(encoded)).encode('utf-8') + RESP_CRLF + encoded + RESP_CRLF
    
    elif isinstance(data, bytes):
        # Bulk string for bytes
        logger.debug(f"Serializing bytes of length {len(data)}")
        return RESP_BULK_STRING + str(len(data)).encode('utf-8') + RESP_CRLF + data + RESP_CRLF
    
    elif isinstance(data, int):
        # Integer
        logger.debug(f"Serializing integer: {data}")
        return RESP_INTEGER + str(data).encode('utf-8') + RESP_CRLF
    
    elif isinstance(data, Exception):
        # Error
        msg = str(data).encode('utf-8')
        logger.debug(f"Serializing error: {data}")
        return RESP_ERROR + msg + RESP_CRLF
    
    elif isinstance(data, (list, tuple)):
        # Array
        if not data:
            # Empty array
            logger.debug("Serializing empty array")
            return RESP_ARRAY + b"0" + RESP_CRLF
        
        logger.debug(f"Serializing array with {len(data)} elements")
        result = RESP_ARRAY + str(len(data)).encode('utf-8') + RESP_CRLF
        for i, item in enumerate(data):
            logger.debug(f"Serializing array element {i}")
            result += serialize_resp(item)
        return result
    
    else:
        # Default to string representation as bulk string
        logger.debug(f"Serializing unknown type {type(data)} as string")
        return serialize_resp(str(data))

async def send_resp_async(writer: asyncio.StreamWriter, data: Any) -> None:
    """
    Serialize and send data asynchronously using asyncio StreamWriter.
    
    Args:
        writer: The asyncio StreamWriter to write data to
        data: Python object to serialize and send
        
    Raises:
        ConnectionError: If there's an issue with the connection
    """
    try:
        logger.debug(f"Sending data asynchronously: {type(data)}")
        serialized = serialize_resp(data)
        logger.debug(f"Serialized data size: {len(serialized)} bytes")

        # Write the serialized data to the asyncio StreamWriter
        # This is a non-blocking operation that adds data to the writer's buffer
        writer.write(serialized)
        
        # Ensure that the data is actually sent by flushing the writer's buffer
        # drain() is a flow control method that:
        # 1. Flushes the buffer to the network
        # 2. Pauses execution if the buffer is full (prevents memory issues)
        # 3. Resumes when buffer space is available
        # 4. Returns when all data has been successfully written to the network buffer
        await writer.drain()
        logger.debug("Data successfully sent")
    
    # Catch connection-related exceptions during the async write operation
    # ConnectionError: General network connectivity issues
    # BrokenPipeError: The connection was unexpectedly closed by the peer
    except (ConnectionError, BrokenPipeError) as e:
        # Convert all connection issues to a standard ConnectionError with details
        # This simplifies error handling for the caller by providing a consistent exception type
        logger.error(f"Failed to send data: {e}", exc_info=True)
        raise ConnectionError(f"Failed to send data: {e}")

# Define a synchronous function for sending RESP data over a socket
# This provides an alternative to async I/O for callers using traditional socket code
def send_resp(sock: socket.socket, data: Any) -> None:
    """
    Serialize and send data synchronously using a socket.
    
    Args:
        sock: The socket to write data to
        data: Python object to serialize and send
        
    Raises:
        ConnectionError: If there's an issue with the connection
    """
    try:
        logger.debug(f"Sending data synchronously: {type(data)}")
        # Convert the Python object to RESP byte format using the serialize_resp function
        # This handles all the type conversion and protocol formatting
        serialized = serialize_resp(data)
        logger.debug(f"Serialized data size: {len(serialized)} bytes")
        
        # Send all the serialized bytes to the socket
        # sendall() is a blocking call that:
        # 1. Continues sending data until all bytes are sent
        # 2. Automatically handles partial sends by continuing where it left off
        # 3. Only returns when all data has been sent or an error occurs
        sock.sendall(serialized)
        logger.debug("Data successfully sent")
    
    # Catch socket-related exceptions during the synchronous send operation
    # socket.error: General socket-related errors (connection reset, timeout, etc.)
    # BrokenPipeError: The connection was unexpectedly closed by the peer
    except (socket.error, BrokenPipeError) as e:
        # Convert all socket issues to a standard ConnectionError with details
        # This provides a consistent error interface regardless of the underlying issue
        logger.error(f"Failed to send data: {e}", exc_info=True)
        raise ConnectionError(f"Failed to send data: {e}")
