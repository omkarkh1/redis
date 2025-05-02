# Import the socket module for network communication
# Author: omkarkh1
import socket
import sys
import os
import time

# Add parent directory to sys.path to resolve imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.logging_config import get_logger, LoggerAdapter
from protocols.resp_v2.parser import parse_resp
from protocols.resp_v2.serializer import send_resp
from commands.processor import RedisDataStore, RedisCommandDispatcher

# Set up logger
logger = get_logger(__name__)

def print_banner():
    """Print a Redis server banner to the console"""
    banner = """
    ____           ___      
   / __ \\___ _____/ (_)_____
  / /_/ / _ `/ __/ / / __(_)
 / _, _/\\_,_/\\__/_/_/\\__(_) 
/_/ |_|     Sync Server     
    """
    print(banner)
    print("Redis-compatible server starting...\n")

# Create Redis data store and command dispatcher
print_banner()
logger.info("Initializing Redis server components")
data_store = RedisDataStore()
command_dispatcher = RedisCommandDispatcher(data_store)

# Create a new TCP/IP socket using IPv4
# Parameters:
#   socket.AF_INET: Address Family IPv4
#   socket.SOCK_STREAM: Socket Type TCP
logger.info("Creating server socket")
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# Allow the socket to reuse the address immediately after closing
# This prevents "Address already in use" errors during development
# Parameters:
#   socket.SOL_SOCKET: Socket level option
#   socket.SO_REUSEADDR: Option to reuse local addresses
#   1: Value to enable the option (True)
server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
# Bind the socket to the localhost address on port 6379
# Parameter:
#   ('localhost', 6379): A tuple containing the host address and port number
server_socket.bind(('localhost', 6379))
# Enable the server to accept connections, with a backlog queue of 5
# Parameter:
#   backlog=5: Maximum number of queued connections before refusing new ones
server_socket.listen(backlog=5)
# Log server start
logger.info("Redis server started on port 6379")
print("Server listening on port 6379")

# Stats tracking
connection_count = 0
command_count = 0
start_time = time.time()

# Use a try...finally block to ensure the server socket is always closed
try:
    # Start an infinite loop to continuously accept client connections
    while True:
        # Log waiting for connection
        logger.debug("Waiting for a connection...")
        # Accept an incoming connection
        # This blocks until a client connects
        # Returns:
        #   client_socket: A new socket object for communication with the client
        #   addr: A tuple containing the client's (IP address, port number)
        client_socket, addr = server_socket.accept()
        
        # Increment connection counter
        connection_count += 1
        
        # Create a connection-specific logger
        conn_logger = LoggerAdapter(logger, {"client": f"{addr[0]}:{addr[1]}", "conn_id": connection_count})
        
        # Log the connected client address
        conn_logger.info(f"New connection established (total: {connection_count})")
        print(f"Client connected from {addr[0]}:{addr[1]} (connection #{connection_count})")

        try:
            # Process client communication until connection closed
            while True:
                # Receive data from the client (up to 4096 bytes at a time)
                data = client_socket.recv(4096)
                
                # If client closed the connection, data will be empty
                if not data:
                    conn_logger.debug("Client closed connection")
                    print(f"Client {addr[0]}:{addr[1]} closed connection")
                    break
                
                conn_logger.debug(f"Received {len(data)} bytes")
                
                try:
                    # Parse the RESP data
                    command = parse_resp(data)
                    
                    # Command-specific logging handled in the dispatcher
                    command_count += 1
                    
                    # Get rough command string for display
                    cmd_str = command[0].decode('utf-8') if isinstance(command[0], bytes) else str(command[0])
                    conn_logger.debug(f"Processing command #{command_count}: {cmd_str}")
                    
                    # Dispatch the command to the appropriate handler
                    start_time_cmd = time.time()
                    result = command_dispatcher.dispatch(command)
                    exec_time_ms = (time.time() - start_time_cmd) * 1000
                    
                    # Send the result back to the client
                    send_resp(client_socket, result)
                    conn_logger.debug(f"Response sent ({exec_time_ms:.2f}ms)")
                    
                except Exception as e:
                    # Log the error
                    conn_logger.error(f"Error processing command: {e}", exc_info=True)
                    print(f"Error processing command from {addr[0]}:{addr[1]}: {e}")
                    # Send error response
                    send_resp(client_socket, f"Error: {str(e)}")
        
        except Exception as e:
            conn_logger.error(f"Client connection error: {e}", exc_info=True)
            print(f"Connection error with {addr[0]}:{addr[1]}: {e}")
        finally:
            # For now, just close the client socket immediately
            # This ends the connection with the current client
            client_socket.close()
            conn_logger.debug("Connection closed")
            print(f"Closed connection with {addr[0]}:{addr[1]}")
            
# Catch KeyboardInterrupt (Ctrl+C) to allow graceful shutdown
except KeyboardInterrupt:
    # Calculate uptime
    uptime = time.time() - start_time
    hours, remainder = divmod(uptime, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    # Log server shutdown with statistics
    logger.info(f"Server shutting down. Stats: {connection_count} connections, {command_count} commands, " 
                f"uptime: {int(hours)}h {int(minutes)}m {int(seconds)}s")
    print(f"\nServer shutting down - Statistics:")
    print(f"- Total connections: {connection_count}")
    print(f"- Total commands processed: {command_count}")
    print(f"- Server uptime: {int(hours)}h {int(minutes)}m {int(seconds)}s")
# The finally block ensures this code runs regardless of exceptions
finally:
    # Close the main server listening socket
    server_socket.close()
    # Log socket closure
    logger.info("Server socket closed.")
    print("Server socket closed. Goodbye!")
