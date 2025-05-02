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

# Add parent directory to sys.path to resolve imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.logging_config import get_logger

# Set up logger
logger = get_logger(__name__)

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
    # Get client's address information (IP, port)
    addr = writer.get_extra_info('peername')
    logger.info(f"Connected to {addr}")
    
    try:
        # Continuously process client requests until connection is closed
        while True:
            # Read up to 1024 bytes of data asynchronously from the client
            # This is a non-blocking operation that yields control until data is available
            data = await reader.read(1024)
            
            # If client closed the connection, data will be empty
            if not data:
                break
            
            # Convert binary data to string for display purposes    
            message = data.decode()
            logger.debug(f"Received {message} from {addr}")
            
            # TODO: Implement actual Redis command processing:
            # 1. Parse the RESP protocol format (Redis serialization protocol)
            # 2. Identify the command (GET, SET, DEL, etc.)
            # 3. Execute the command against the in-memory data store
            # 4. Format the response according to RESP protocol
            # 5. Return appropriate response based on the command result
            #
            # For now, we're just echoing back the received data
            writer.write(data)
            
            # Ensure the data is actually sent (buffer is flushed)
            # drain() is a flow control method that prevents flooding the client
            await writer.drain()
            logger.debug(f"Echo response sent to {addr}")
            
    except Exception as e:
        # Handle any exceptions that occur during client communication
        logger.error(f"Error handling client {addr}: {e}", exc_info=True)
    finally:
        # Clean up resources, regardless of how the connection ended
        logger.info(f"Closing connection with {addr}")
        # Close the writer stream
        writer.close()
        # Asynchronously wait until the writer is properly closed
        # This ensures all pending data is sent before fully closing the connection
        await writer.wait_closed()
        logger.debug(f"Connection with {addr} closed successfully")

async def start_redis_server():
    """
    Start the Redis server and listen for incoming connections.
    
    This coroutine initializes the server, binds it to the specified address and port,
    and continuously accepts client connections.
    
    Returns:
        None
    """
    # Create and start a TCP server that listens for connections
    # When a client connects, handle_client coroutine is called with reader/writer objects
    server = await asyncio.start_server(
        handle_client,          # The callback coroutine to handle each client connection
        '127.0.0.1',            # The host address to bind to (localhost)
        6379                    # The port to bind to (standard Redis port)
    )
    
    # Get the server's bound address for informational purposes
    addr = server.sockets[0].getsockname()
    logger.info(f'Redis server running on {addr}')
    
    # Using async with ensures proper cleanup when the server is stopped
    # The async with statement creates a context manager that:
    #   1. Starts the server (already done by the await asyncio.start_server call above)
    #   2. Acquires resources needed for the server to run
    #   3. Automatically closes these resources when the block exits, even if exceptions occur
    #   4. Calls server.close() and server.wait_closed() to ensure graceful shutdown
    #   5. Prevents resource leaks (like open socket connections) by ensuring cleanup
    async with server:
        # serve_forever() keeps the server running until it's explicitly stopped
        # This is a non-blocking call that yields control periodically
        # 
        # What happens internally:
        #   1. The server enters an infinite loop accepting new connections
        #   2. For each connection, it creates reader/writer objects
        #   3. It calls our handle_client coroutine with these objects
        #   4. Multiple client connections are handled concurrently by the event loop
        #   5. The server will run until:
        #      - The server is closed (server.close() is called)
        #      - A cancellation is requested (server task is cancelled)
        #      - An unhandled exception occurs
        await server.serve_forever()

# Entry point of the program
if __name__ == "__main__":
    try:
        # Start the asyncio event loop with our server coroutine
        # asyncio.run() creates a new event loop, runs the coroutine, and closes the loop
        asyncio.run(start_redis_server())
    except KeyboardInterrupt:
        # Handle graceful shutdown when user presses Ctrl+C
        logger.info("Server stopped")

