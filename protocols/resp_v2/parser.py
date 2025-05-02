"""
Redis Serialization Protocol (RESP) Version 2 Parser

This module implements the parsing part of RESP v2 protocol for Redis client-server communication.
It includes functionality for parsing RESP byte streams into Python objects.

RESP is a binary-safe protocol designed to be simple to implement and fast to parse.
It uses prefixed length encoding to efficiently transfer data while maintaining
the ability to represent different data types.

The protocol defines 5 basic data types:
1. Simple Strings: Prefixed with '+', cannot contain CR or LF
2. Errors: Prefixed with '-', similar to Simple Strings but represent error conditions
3. Integers: Prefixed with ':', used for numeric data
4. Bulk Strings: Prefixed with '$', binary-safe strings with explicit length
5. Arrays: Prefixed with '*', collections of other RESP data types

Author: omkarkh1
"""

import io
import os
import sys
from typing import Any, List, Optional, Tuple, Union

# Add parent directory to sys.path to resolve imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from utils.logging_config import get_logger

# Set up logger
logger = get_logger(__name__)

# RESP Protocol Constants
RESP_SIMPLE_STRING = b'+'  # Simple string marker
RESP_ERROR = b'-'          # Error marker
RESP_INTEGER = b':'        # Integer marker
RESP_BULK_STRING = b'$'    # Bulk string marker
RESP_ARRAY = b'*'          # Array marker
RESP_CRLF = b'\r\n'        # Carriage return + line feed (delimiter)

class RESPError(Exception):
    """Base exception for RESP parsing errors"""
    pass

class RESPProtocolError(RESPError):
    """Exception raised for protocol-level errors"""
    pass

class RESPParserV2:
    """
    Parser for Redis Serialization Protocol (RESP) Version 2
    
    This class implements a parser that can deserialize a RESP-formatted byte stream
    into corresponding Python objects. It follows a recursive descent parsing approach
    where the main parse method dispatches to type-specific parsing methods based on
    the type marker byte.
    
    The parser is designed to handle incomplete data by raising appropriate exceptions
    that can be caught by the calling code to request more data from the network.
    
    Usage:
        parser = RESPParserV2()
        result, bytes_consumed = parser.parse(data_bytes)
    """
    
    def __init__(self):
        """Initialize the RESP parser"""
        self.logger = get_logger(f"{__name__}.RESPParserV2")
        self.logger.debug("Initializing RESP Parser V2")
    
    def parse(self, data: bytes) -> Tuple[Any, int]:
        """
        Parse RESP data bytes into Python objects
        
        This is the main parsing function that examines the first byte to determine
        the data type and dispatches to the appropriate handler. The function follows
        a recursive descent parsing strategy where complex types like arrays may
        recursively call this method to parse their elements.
        
        The parser returns both the parsed object and the number of bytes consumed,
        which is crucial for parsing multiple commands in a single data stream or
        when handling partial data in network buffers.
        
        Args:
            data: Bytes to parse according to RESP protocol
            
        Returns:
            A tuple containing:
            - The parsed Python object
            - The number of bytes consumed from the input
            
        Raises:
            RESPProtocolError: If the data format is invalid or unsupported
        """
        if not data:
            # Log error and raise exception for empty data
            self.logger.error("Received empty data for parsing")
            raise RESPProtocolError("Empty data")
        
        # Read the type marker (first byte)
        type_byte = data[0:1]
        self.logger.debug(f"Parsing data with type marker: {type_byte}")
        
        # Dispatch to the appropriate handler based on the type marker
        if type_byte == RESP_SIMPLE_STRING:
            return self._parse_simple_string(data)
        elif type_byte == RESP_ERROR:
            return self._parse_error(data)
        elif type_byte == RESP_INTEGER:
            return self._parse_integer(data)
        elif type_byte == RESP_BULK_STRING:
            return self._parse_bulk_string(data)
        elif type_byte == RESP_ARRAY:
            return self._parse_array(data)
        else:
            self.logger.error(f"Unknown type marker: {type_byte}")
            raise RESPProtocolError(f"Unknown type marker: {type_byte}")
    
    def _parse_simple_string(self, data: bytes) -> Tuple[str, int]:
        """
        Parse a RESP Simple String (+)
        
        Format: "+<string data>\r\n"
        Simple strings cannot contain CR or LF characters, making them suitable for
        small, non-binary text responses. They are more efficient than bulk strings
        for short text as they don't require an additional length field.
        
        The parser decodes the byte data to UTF-8 strings, assuming text content.
        
        Examples:
            "+OK\r\n" parses to "OK"
            "+Hello World\r\n" parses to "Hello World"
        
        Args:
            data: Bytes starting with '+' followed by string data and CRLF
            
        Returns:
            A tuple containing:
            - The parsed string
            - The number of bytes consumed
            
        Raises:
            RESPProtocolError: If the format is invalid
        """
        # Find the end of the string (CRLF)
        try:
            crlf_pos = data.index(RESP_CRLF)
        except ValueError:
            self.logger.error("Incomplete simple string, missing CRLF")
            raise RESPProtocolError("Incomplete simple string, missing CRLF")
        
        # Extract the string content (skip the initial '+')
        string_data = data[1:crlf_pos].decode('utf-8')
        self.logger.debug(f"Parsed simple string: {string_data}")
        
        # Return the string and the number of bytes consumed (including CRLF)
        return string_data, crlf_pos + 2
    
    def _parse_error(self, data: bytes) -> Tuple[Exception, int]:
        """
        Parse a RESP Error (-)
        
        Format: "-<error message>\r\n"
        Errors are similar to simple strings but represent error conditions.
        In Redis, these typically include error type and message, such as:
        "-ERR unknown command 'foobar'" or "-WRONGTYPE Operation against a key holding the wrong kind of value"
        
        The parser wraps the error message in a RESPError exception object,
        allowing the client to distinguish between protocol errors and Redis errors.
        
        Examples:
            "-ERR unknown command\r\n" parses to RESPError("ERR unknown command")
            "-WRONGTYPE Operation not permitted\r\n" parses to RESPError("WRONGTYPE Operation not permitted")
        
        Args:
            data: Bytes starting with '-' followed by error message and CRLF
            
        Returns:
            A tuple containing:
            - An exception object containing the error message
            - The number of bytes consumed
            
        Raises:
            RESPProtocolError: If the format is invalid
        """
        # Find the end of the error message (CRLF)
        try:
            crlf_pos = data.index(RESP_CRLF)
        except ValueError:
            self.logger.error("Incomplete error message, missing CRLF")
            raise RESPProtocolError("Incomplete error message, missing CRLF")
        
        # Extract the error message (skip the initial '-')
        error_msg = data[1:crlf_pos].decode('utf-8')
        self.logger.debug(f"Parsed error message: {error_msg}")
        
        # Return an exception with the error message and bytes consumed
        return RESPError(error_msg), crlf_pos + 2
    
    def _parse_integer(self, data: bytes) -> Tuple[int, int]:
        """
        Parse a RESP Integer (:)
        
        Format: ":<integer>\r\n"
        The integer is represented as a decimal number in ASCII format. This type is
        used for integer responses like INCR commands, or to represent the size of
        sets, lists, etc. The integer can be positive or negative.
        
        The parser converts the ASCII representation to a Python int.
        
        Examples:
            ":1000\r\n" parses to 1000
            ":-1\r\n" parses to -1
        
        Args:
            data: Bytes starting with ':' followed by an integer and CRLF
            
        Returns:
            A tuple containing:
            - The parsed integer
            - The number of bytes consumed
            
        Raises:
            RESPProtocolError: If the format is invalid or value is not an integer
        """
        # Find the end of the integer (CRLF)
        try:
            crlf_pos = data.index(RESP_CRLF)
        except ValueError:
            self.logger.error("Incomplete integer, missing CRLF")
            raise RESPProtocolError("Incomplete integer, missing CRLF")
        
        # Extract the integer string (skip the initial ':')
        int_str = data[1:crlf_pos].decode('utf-8')
        
        # Convert to integer
        try:
            value = int(int_str)
        except ValueError:
            self.logger.error(f"Invalid integer format: {int_str}")
            raise RESPProtocolError(f"Invalid integer format: {int_str}")
        
        self.logger.debug(f"Parsed integer: {value}")
        
        # Return the integer and the number of bytes consumed
        return value, crlf_pos + 2
    
    def _parse_bulk_string(self, data: bytes) -> Tuple[Optional[bytes], int]:
        """
        Parse a RESP Bulk String ($)
        
        Format: "$<length>\r\n<data>\r\n"
        Special case: "$-1\r\n" represents a null value
        
        Bulk strings are the most versatile RESP data type, used for binary-safe
        string data of any length. The length prefix allows efficient parsing without
        needing to scan for terminator bytes, and the format can represent both
        text and binary data including embedded CR/LF sequences.
        
        Unlike simple strings, bulk strings are returned as bytes objects to preserve
        binary data. The caller is responsible for decoding to text if needed.
        
        Examples:
            "$5\r\nhello\r\n" parses to b"hello"
            "$0\r\n\r\n" parses to b"" (empty string)
            "$-1\r\n" parses to None (null string)
        
        Args:
            data: Bytes starting with '$' followed by length, CRLF, data, and CRLF
            
        Returns:
            A tuple containing:
            - The parsed bytes (or None for null bulk string)
            - The number of bytes consumed
            
        Raises:
            RESPProtocolError: If the format is invalid
        """
        # Find the end of the length line (CRLF)
        try:
            first_crlf_pos = data.index(RESP_CRLF)
        except ValueError:
            self.logger.error("Incomplete bulk string, missing length CRLF")
            raise RESPProtocolError("Incomplete bulk string, missing length CRLF")
        
        # Extract the length (skip the initial '$')
        length_str = data[1:first_crlf_pos].decode('utf-8')
        
        try:
            length = int(length_str)
        except ValueError:
            self.logger.error(f"Invalid bulk string length: {length_str}")
            raise RESPProtocolError(f"Invalid bulk string length: {length_str}")
        
        # Handle null bulk string
        if length == -1:
            self.logger.debug("Parsed null bulk string")
            return None, first_crlf_pos + 2
        
        # Check if we have enough data
        if len(data) < first_crlf_pos + 2 + length + 2:
            self.logger.error("Incomplete bulk string data")
            raise RESPProtocolError("Incomplete bulk string data")
        
        # Extract the string data
        start_pos = first_crlf_pos + 2
        end_pos = start_pos + length
        
        string_data = data[start_pos:end_pos]
        
        # Verify the final CRLF
        if data[end_pos:end_pos+2] != RESP_CRLF:
            self.logger.error("Missing CRLF at end of bulk string")
            raise RESPProtocolError("Missing CRLF at end of bulk string")
        
        self.logger.debug(f"Parsed bulk string of length {length}")
        
        # Return the string data and the number of bytes consumed
        return string_data, end_pos + 2
    
    def _parse_array(self, data: bytes) -> Tuple[Optional[List[Any]], int]:
        """
        Parse a RESP Array (*)
        
        Format: "*<count>\r\n<element-1>...<element-n>"
        Special case: "*-1\r\n" represents a null array
        
        Arrays are the most complex RESP data type, used to represent collections
        of other RESP values. They enable nested data structures and are commonly
        used for both client commands and server responses. Each element can be any
        RESP data type, including nested arrays.
        
        The parser recursively processes each element, handling heterogeneous arrays
        with different data types for different elements. This flexibility is used by
        Redis for multi-key commands, transactional responses, and scan operations.
        
        Examples:
            "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n" parses to [b"hello", b"world"]
            "*3\r\n:1\r\n:2\r\n:3\r\n" parses to [1, 2, 3]
            "*0\r\n" parses to [] (empty array)
            "*-1\r\n" parses to None (null array)
            "*2\r\n*2\r\n:1\r\n:2\r\n*2\r\n:3\r\n:4\r\n" parses to [[1, 2], [3, 4]]
        
        Args:
            data: Bytes starting with '*' followed by count, CRLF, and elements
            
        Returns:
            A tuple containing:
            - The parsed array (or None for null array)
            - The number of bytes consumed
            
        Raises:
            RESPProtocolError: If the format is invalid
        """
        # Find the end of the count line (CRLF)
        try:
            first_crlf_pos = data.index(RESP_CRLF)
        except ValueError:
            self.logger.error("Incomplete array, missing count CRLF")
            raise RESPProtocolError("Incomplete array, missing count CRLF")
        
        # Extract the count (skip the initial '*')
        count_str = data[1:first_crlf_pos].decode('utf-8')
        
        try:
            count = int(count_str)
        except ValueError:
            self.logger.error(f"Invalid array count: {count_str}")
            raise RESPProtocolError(f"Invalid array count: {count_str}")
        
        # Handle null array
        if count == -1:
            self.logger.debug("Parsed null array")
            return None, first_crlf_pos + 2
        
        # Handle empty array
        if count == 0:
            self.logger.debug("Parsed empty array")
            return [], first_crlf_pos + 2
        
        self.logger.debug(f"Parsing array with {count} elements")
        
        # Parse each element in the array
        result = []
        bytes_consumed = first_crlf_pos + 2  # Initial count line + CRLF
        remaining_data = data[bytes_consumed:]
        
        for i in range(count):
            try:
                element, element_bytes = self.parse(remaining_data)
                result.append(element)
                bytes_consumed += element_bytes
                remaining_data = data[bytes_consumed:]
                self.logger.debug(f"Parsed array element {i}: {type(element)}")
            except Exception as e:
                self.logger.error(f"Error parsing array element {i}: {str(e)}", exc_info=True)
                raise RESPProtocolError(f"Error parsing array element {i}: {str(e)}")
        
        self.logger.debug(f"Successfully parsed array with {count} elements")
        
        # Return a tuple containing both the parsed array result and the total number of bytes consumed
        # This enables the caller to correctly process multiple commands or handle partial data streams
        # The 'result' variable contains the fully parsed array with all of its elements
        # The 'bytes_consumed' variable tracks the exact number of bytes read from the input stream
        return result, bytes_consumed

# Define a convenience function that simplifies the common use case of parsing a complete RESP message
# This wrapper function hides the complexity of creating a parser instance and handling the bytes_consumed value
# It's particularly useful when you just need the parsed object and don't care about byte counting
def parse_resp(data: bytes) -> Any:
    """
    Parse RESP data bytes into Python objects
    
    This is a convenience function that creates a parser and processes the data.
    It simplifies the common case where the caller only needs the parsed object
    and doesn't care about how many bytes were consumed (e.g., when processing
    a complete response).
    
    This function is useful for one-shot parsing operations where the entire RESP
    message is available at once. For incremental parsing (e.g., with network I/O),
    use the RESPParserV2 class directly.
    
    Args:
        data: Bytes to parse according to RESP protocol
        
    Returns:
        The parsed Python object
        
    Raises:
        RESPProtocolError: If the data format is invalid or unsupported
    """
    # Get a module-level logger for this function
    logger = get_logger(f"{__name__}.parse_resp")
    logger.debug(f"Parsing complete RESP message of {len(data)} bytes")
    
    # Create a new instance of the RESPParserV2 class to handle the parsing operations
    # Each call to parse_resp gets a fresh parser instance to avoid state contamination
    parser = RESPParserV2()
    
    # Call the parse method on the parser instance, passing in the byte data to be parsed
    # The parse method returns a tuple (result, bytes_consumed), but we only care about result
    # The underscore (_) is a Python convention for ignoring a returned value we don't need
    result, bytes_consumed = parser.parse(data)
    
    logger.debug(f"Successfully parsed RESP message, consumed {bytes_consumed} bytes")
    
    # Return just the parsed object to the caller, discarding the bytes_consumed information
    # This simplifies the API for callers who are processing complete, well-formed messages
    return result
