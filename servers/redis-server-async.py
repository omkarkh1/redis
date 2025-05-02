# Import the asyncio library, which provides infrastructure for writing
# single-threaded concurrent code using coroutines, multiplexing I/O access
# over sockets and other resources.
# 
# Coroutines: Functions that can pause execution and yield control back to the 
# event loop while waiting for an operation to complete (like I/O). This allows
# other code to run during these waiting periods, creating concurrency without threads.
#
# Multiplexing I/O access: The ability to handle many I/O operations within a single
# thread by efficiently switching between tasks that are ready to run, rather than
# blocking on operations. This is implemented using system calls like select(), poll(), 
# or epoll() that monitor multiple file descriptors to see which ones are ready for I/O.
#
# This approach enables handling hundreds or thousands of connections simultaneously
# within a single thread, similar to how Redis achieves high performance.
# Author: omkarkh1
import asyncio
import sys
import os
import time

# Add parent directory to sys.path to resolve imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.logging_config import get_logger, LoggerAdapter
from protocols.resp_v2.parser import parse_resp
from protocols.resp_v2.serializer import send_resp_async
from commands.processor import RedisDataStore, RedisCommandDispatcher

# Set up logger
logger = get_logger(__name__)

# Stats tracking
connection_count = 0
command_count = 0
start_time = 0

def print_banner():
    """Print a Redis server banner to the console"""
    banner = """
    ____           ___      
   / __ \\___ _____/ (_)_____
  / /_/ / _ `/ __/ / / __(_)
 / _, _/\\_,_/\\__/_/_/\\__(_) 
/_/ |_|     Async Server    
    """
    print(banner)
    print("Redis-compatible async server starting...\n")

# Create Redis data store and command dispatcher (shared among all clients)
print_banner()
logger.info("Initializing Redis server components")
data_store = RedisDataStore()
command_dispatcher = RedisCommandDispatcher(data_store)

async def handle_client(reader, writer):
    """
    Handle a client connection asynchronously.
    
    Parameters:
        reader (asyncio.StreamReader): An object that provides APIs to read data from the stream.
                                      It implements flow control algorithms to protect from
                                      memory exhaustion.
        writer (asyncio.StreamWriter): An object that provides APIs to write data to the stream.
                                      It implements flow control algorithms to prevent overloading
                                      the peer with too much data.
    
    # Flow control algorithms: Mechanisms that regulate the rate of data transmission 
    # between sender and receiver to prevent overwhelming either side. These algorithms:
    #   - Prevent fast senders from overwhelming slow receivers (backpressure)
    #   - Manage memory usage by controlling buffer sizes
    #   - Coordinate when to pause/resume data transfer based on system capacity
    #   - In asyncio, implemented through methods like drain() which pauses sending
    #     until the receiver has processed the data
    
    This coroutine processes incoming client connections, reads data sent by clients,
    and sends responses back.
    """
    global connection_count, command_count
    
    # Increment connection counter
    connection_count += 1
    
    # Get client's address information (IP, port)
    addr = writer.get_extra_info('peername')
    
    # Create a connection-specific logger
    conn_logger = LoggerAdapter(logger, {"client": f"{addr[0]}:{addr[1]}", "conn_id": connection_count})
    
    conn_logger.info(f"New connection established (total: {connection_count})")
    print(f"Client connected from {addr[0]}:{addr[1]} (connection #{connection_count})")
    
    try:
        # Continuously process client requests until connection is closed
        while True:
            # Read up to 4096 bytes of data asynchronously from the client
            # This is a non-blocking operation that yields control until data is available
            data = await reader.read(4096)
            
            # If client closed the connection, data will be empty
            if not data:
                conn_logger.debug(f"Client closed connection")
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
                
                # Send the result back to the client asynchronously
                await send_resp_async(writer, result)
                conn_logger.debug(f"Response sent ({exec_time_ms:.2f}ms)")
                
            except Exception as e:
                # Log the error
                conn_logger.error(f"Error processing command: {e}", exc_info=True)
                print(f"Error processing command from {addr[0]}:{addr[1]}: {e}")
                # Send error response
                await send_resp_async(writer, f"Error: {str(e)}")
            
    except asyncio.CancelledError:
        conn_logger.info("Connection handling was cancelled")
        print(f"Connection handling for {addr[0]}:{addr[1]} was cancelled")
    except Exception as e:
        # Handle any exceptions that occur during client communication
        conn_logger.error(f"Error handling client: {e}", exc_info=True)
        print(f"Error handling client {addr[0]}:{addr[1]}: {e}")
    finally:
        # Clean up resources, regardless of how the connection ended
        conn_logger.info(f"Closing connection")
        print(f"Closing connection with {addr[0]}:{addr[1]}")
        
        try:
            # Close the writer stream
            writer.close()
            # Asynchronously wait until the writer is properly closed
            # This ensures all pending data is sent before fully closing the connection
            await writer.wait_closed()
            conn_logger.debug(f"Connection closed successfully")
        except Exception as e:
            conn_logger.error(f"Error closing connection: {e}")
            print(f"Error closing connection with {addr[0]}:{addr[1]}: {e}")

async def start_redis_server():
    """
    Start the Redis server and listen for incoming connections.
    
    This coroutine initializes the server, binds it to the specified address and port,
    and continuously accepts client connections.
    
    Returns:
        None
    """
    global start_time
    
    # Record start time for statistics
    start_time = time.time()
    
    # Create and start a TCP server that listens for connections
    # When a client connects, handle_client coroutine is called with reader/writer objects
    server = await asyncio.start_server(
        handle_client,          # The callback coroutine to handle each client connection
        '127.0.0.1',            # The host address to bind to (localhost)
        6379                    # The port to bind to (standard Redis port)
    )
    
    # Get the server's bound address for informational purposes
    addr = server.sockets[0].getsockname()
    logger.info(f'Redis async server running on {addr[0]}:{addr[1]}')
    print(f"Server listening on {addr[0]}:{addr[1]}")
    
    # Set up periodic stats reporting
    asyncio.create_task(report_stats())
    
    # Using async with ensures proper cleanup when the server is stopped
    async with server:
        # serve_forever() keeps the server running until it's explicitly stopped
        await server.serve_forever()

async def report_stats():
    """Periodically report server statistics"""
    while True:
        await asyncio.sleep(60)  # Report every minute
        
        # Calculate uptime
        uptime = time.time() - start_time
        hours, remainder = divmod(uptime, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        logger.info(f"Server stats: {connection_count} connections, {command_count} commands, "
                   f"uptime: {int(hours)}h {int(minutes)}m {int(seconds)}s")
        print(f"\nServer statistics (running for {int(hours)}h {int(minutes)}m {int(seconds)}s):")
        print(f"- Total connections: {connection_count}")
        print(f"- Total commands processed: {command_count}")

# Entry point of the program
if __name__ == "__main__":
    try:
        # Start the asyncio event loop with our server coroutine
        # asyncio.run() creates a new event loop, runs the coroutine, and closes the loop
        asyncio.run(start_redis_server())
    except KeyboardInterrupt:
        # Calculate uptime
        if start_time > 0:
            uptime = time.time() - start_time
            hours, remainder = divmod(uptime, 3600)
            minutes, seconds = divmod(remainder, 60)
            
            # Log server shutdown with statistics
            logger.info(f"Server shutting down. Stats: {connection_count} connections, {command_count} commands, "
                       f"uptime: {int(hours)}h {int(minutes)}m {int(seconds)}s")
            print(f"\nServer shutting down - Final Statistics:")
            print(f"- Total connections: {connection_count}")
            print(f"- Total commands processed: {command_count}")
            print(f"- Server uptime: {int(hours)}h {int(minutes)}m {int(seconds)}s")
        else:
            logger.info("Server stopped before starting")
            print("Server stopped before starting")
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        print(f"Unexpected error: {e}")

