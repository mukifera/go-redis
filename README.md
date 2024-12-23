# go-redis
A clone of Redis, written in Go.

Redis is an in-memory data store, often used as a database, cache, message broker, and streaming engine

Highlight of functionalities supported by this implementation:
- Read and write values in an in-memory store, from multiple clients concurrently
- Support data persistence and server cloning through snapshot files
- Build a fault tolerant fleet of data stores by applying redundancy through command replication between master/replica server instances
- Store, query, and consume streams of data
- Maintain store state consistency by supporting the atomic application of a sequence of commands through transactions

## Usage
The server can be run with the `go-redis.sh` script.

### Supported Flags
| Flag | Description |
| :-----  | :-------  |
|`--port {port}` | Bind the server to listen for commands on the given port |
| `--dir {directory}` | The directory to store the snapshot file |
| `--dbfilename {filename}` | The name of the snapshot file |
| `--replicaof "{master_host} {master_port}"` | Declare the server as a replica of the given master server|
|

## Supported Commands
### Client-Server Commands
| Command | Behavior |
| :-----  | :-------  |
| `PING`| Respond with `PONG`|
| `ECHO {message}` | Echo back the given message |
|`SET {key} {value} `| Set a value for a given key in the store|
|`SET {key} {value} px {expiry}`| Set a value for a given key with an expiry|
| `INCR {key}` | Increment the value of a given key |
| `GET {key}` | Respond with the value of a given key |
| `TYPE {key}` | Report the value type of a given key |
| `XADD {stream_key} {entry_id} [{key} {value}]` | Add a new stream entry with the given key value pairs |
| `XRANGE {stream_key} {from_id} {to_id}` | Retrieve a range of entries from the given stream key |
| `XREAD streams [{stream_key}] [{from_id}]` | Retrieve stream entries starting from the given entry ids, for all the given streams |
| `XREAD streams block {time} [{stream_key}] [{from_id}]` | Same as above, but block the client until more stream entries are added |
| `INFO replication` | Provide replication information for the current server instance. Includes role distinction (master/replica), connected replicas, and replicated commands offset |
| `WAIT {replicas} {timeout}` | Block client until `num_replicas` replicas have acknowledged receiving propagated commands, or until timeout|
| `MULTI` | Declare the start of a transaction |
| `EXEC` | Execute the current transaction |
| `DISCARD` | Abort the current transaction |

### Master-Replica Commands
| Command | Direction | Behavior |
| :-----  | :-------  | :-------- |
| `REPLCONF listening-port` | replica to master | Notify the master of the port the replica is listening on |
| `REPLCONF capa psync2` | replica to master | Notify the master of the supported sync capabilities |
| `PSYNC {replication_id} {offset}` | replica to master | Synchronize the state of the replica to the master |
| `REPLCONF GETACK` | master to replica | Request an acknowledgment of number of command bytes processed by the replica|
