# ErmineDB

ErmineDB is in development and not ready for use yet.
When ErmineDB is ready it will be a key/value database that
duplicates the Redis protocol.  That means you can use any
existing Redis client to connect to ErmineDB.
There are a number of planned advantages.  Logging of CRUD,
cluster replication using Raft just to name two.


# Run from cli
`go run cmd/ermined/main.go` will run a standalone server.  Redis will be served on port 8888 and Raft will listen on 12001.

`go run cmd/ermined/main.go -port 9001 -raftPort 12001 -data ./data -join 127.0.0.1:12001:9001,127.0.0.1:12002:9002,127.0.0.1:12003:9003` will start one instance of a 3 node cluster.  -data needs to be unique for each instance if you are running all 3 nodes on one machine.

# Supported commands
## Connection management
PING [message]
Returns `PONG` if no message is provided, otherwise a copy of the message is returned.

SELECT index
Select an index from 0 - 15. By default connections use index 0. Different indexes are still part of the same database/file, but they are prefixed. This is a type of namespacing. Keys in defferent indexes can have the same name without conflict.
Unlike Redis, all indexes are replicated to the other nodes in the Raft cluster.

## Generic
COPY source destination [DB destination-index] [REPLACE]

DEL key [key...]

DUMP key
This is similer to Redis dump but they are not compatible. Where Redis uses a non-standard serialization format ErmineDB uses msgpack.

EXISTS key [key...]

KEYS pattern

## Hash
HDEL key field [field...]

HEXISTS key field

HGET key field

HGETALL key

HINCRBY key field increment

HINCRBYFLOAT key field increment

HKEYS key

HLEN key

HMGET key field [field...]

HMSET key field value [field value...]
This is the same as HSET

HRANDFIELD key [count [WITHVALUES]]

HSET key field value [field value...]

HSETNX key field value

## Pub/Sub
PUBLISH channel message

SUBSCRIBE channel

UNSUBSCRIBE channel

## Server management
DBSIZE

ROLE

SWAPDB index1 index2

TIME

## String
APPEND key value

GET key

SET key value

## Commands not supported and why
MONITOR
This command is not needed with ErmineDB. ErmineDB can log to standard out.
