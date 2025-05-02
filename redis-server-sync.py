# Import the socket module for network communication
import socket

# Create a new TCP/IP socket using IPv4
# Parameters:
#   socket.AF_INET: Address Family IPv4
#   socket.SOCK_STREAM: Socket Type TCP
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
# Print a message indicating the server has started
print("Redis server started on port 6379")

# Use a try...finally block to ensure the server socket is always closed
try:
    # Start an infinite loop to continuously accept client connections
    while True:
        # Print a message indicating the server is waiting for a connection
        print("Waiting for a connection...")
        # Accept an incoming connection
        # This blocks until a client connects
        # Returns:
        #   client_socket: A new socket object for communication with the client
        #   addr: A tuple containing the client's (IP address, port number)
        client_socket, addr = server_socket.accept()
        # Print the address of the connected client
        print(f"Connection from {addr}")

        # TODO: Handle client communication (e.g., read commands, send responses)
        # In a real server, communication logic would go here.
        # To handle multiple clients concurrently with the socket module,
        # each new client connection (client_socket) returned by accept()
        # must be passed to a separate thread (using the threading module)
        # or process (using the multiprocessing module).
        # This allows the main loop to quickly return to accept() for new connections
        # while the thread/process handles the communication for the existing client.

        # For now, just close the client socket immediately
        # This ends the connection with the current client
        client_socket.close()
# Catch KeyboardInterrupt (Ctrl+C) to allow graceful shutdown
except KeyboardInterrupt:
    # Print a message indicating the server is shutting down
    print("Server shutting down.")
# The finally block ensures this code runs regardless of exceptions
finally:
    # Close the main server listening socket
    server_socket.close()
    # Print a confirmation that the server socket has been closed
    print("Server socket closed.")
