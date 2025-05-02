# Redis Python Implementation

A lightweight Redis-compatible server implementation written in Python. This project provides both synchronous and asynchronous server implementations that support core Redis functionality.

## Features

- In-memory key-value storage
- Support for Redis RESP (Redis Serialization Protocol) v2
- Multiple server implementations:
  - Synchronous server using standard sockets
  - Asynchronous server using Python's asyncio
- Core Redis commands implementation
- TTL (Time-To-Live) support for keys
- Comprehensive logging
- Command validation

## Requirements

- Python 3.7+

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/redis.git
   cd redis
   ```

## Usage

### Running the synchronous server

```bash
python servers/redis-server-sync.py
```

### Running the asynchronous server

```bash
python servers/redis-server-async.py
```

### Connecting to the server

You can connect to the server using any Redis client. The server runs on the default Redis port (6379):

Using `redis-cli`:
```bash
redis-cli -h localhost -p 6379
```

Using Python with `redis` package:
```python
import redis
r = redis.Redis(host='localhost', port=6379)
r.set('foo', 'bar')
print(r.get('foo'))  # Returns b'bar'
```

## Project Structure

- `servers/`: Server implementations
  - `redis-server-sync.py`: Synchronous implementation
  - `redis-server-async.py`: Asynchronous implementation using asyncio
- `protocols/resp_v2/`: RESP protocol implementation
  - `parser.py`: Parses Redis protocol messages
  - `serializer.py`: Serializes data into Redis protocol format
- `commands/`: Command handling
  - `processor.py`: Command execution and data store
  - `validator.py`: Command validation
- `utils/`: Utility modules
  - `logging_config.py`: Logging configuration

## Supported Commands

The server currently supports the following Redis commands:

- `GET key`: Get the value of a key
- `SET key value [EX seconds|PX milliseconds]`: Set the value of a key, with optional expiration
- `DEL key [key ...]`: Delete one or more keys
- `EXISTS key [key ...]`: Check if keys exist
- `EXPIRE key seconds`: Set key expiration time in seconds
- `TTL key`: Get remaining time to live for a key
- `PING [message]`: Test connection
- `FLUSHALL`: Remove all keys from the database

## Performance Characteristics

- The synchronous server can handle one connection at a time
- The asynchronous server can handle many concurrent connections
- Both servers use in-memory storage with no persistence

## Future Improvements

- Add persistence (RDB/AOF)
- Implement more Redis commands
- Add clustering support
- Add Redis Pub/Sub mechanism
- Add transactions (MULTI/EXEC)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Author

omkarkh1
