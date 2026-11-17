# toy-redis

An asyncio Redis-compatible server, started from the [CodeCrafters Redis
challenge](https://codecrafters.io/challenges/redis) and intended for further development beyond the
challenge. Supports strings, bitmaps, lists, streams, sorted sets, geospatial indexes, pub/sub,
authentication, transactions, replication, and RDB loading over RESP2.

This implements a subset of Redis. The command reference below describes the supported forms.

## Running

Requires Python 3.14 or later and `uv` for the launcher. Install `redis-cli` separately to use the
client examples.

```sh
./run.sh --port 6379
redis-cli PING
```

Alternatively, run `python3 -m app.main` with Python 3.14 or later.

| Option | Default | Purpose |
| --- | --- | --- |
| `--host` | `localhost` | Address to bind |
| `--port` | `6379` | TCP port |
| `--replicaof HOST PORT` | Unset | Follow a primary server |
| `--dir` | Working directory at startup | Base directory for data files |
| `--dbfilename` | `dump.rdb` | RDB filename |
| `--appendonly` | `no` | AOF enablement setting: `yes` or `no` |
| `--appenddirname` | `appendonlydir` | AOF subdirectory under `dir` |
| `--appendfilename` | `appendonly.aof` | AOF base filename |
| `--appendfsync` | `everysec` | AOF flush policy: `always`, `everysec`, or `no` |

Start a replica with:

```sh
./run.sh --port 6380 --replicaof localhost 6379
```

Load an existing RDB snapshot at startup with:

```sh
./run.sh --dir /path/to/snapshots --dbfilename dump.rdb
redis-cli CONFIG GET dir dbfilename
redis-cli KEYS '*'
```

## Data types and commands

Command and subcommand names are case insensitive. Keys, members, channels, usernames, and passwords
are case sensitive. In the syntax below, brackets indicate optional arguments, `or` separates
alternatives, and `...` indicates repetition.

### Strings

Strings hold text or byte values; bitmap operations share the same storage. `STRLEN` measures bytes.

| Command | Behavior |
| --- | --- |
| `SET key value [NX or XX] [EX seconds or PX milliseconds or EXAT timestamp or PXAT timestamp]` | Store a value, optionally with a condition and expiry |
| `GET key` | Read a value; return null for a missing or expired key |
| `INCR key` | Increment an integer value; initialize a missing key to `1` |
| `DECR key` | Decrement a signed 64-bit integer, starting from zero for a missing key; preserve its expiry |
| `STRLEN key` | Return the byte length, or `0` for a missing key |

`SET` replaces the previous value and its expiry. Supports `NX` (only if absent) and `XX` (only if
present), combined with one `EX`, `PX`, `EXAT`, or `PXAT` expiry option in either order. An unmet
condition returns null without changing the key.

### Key expiry

These commands work on every data type.

| Command | Behavior |
| --- | --- |
| `TTL key` | Return the remaining expiry in seconds |
| `PTTL key` | Return the remaining expiry in milliseconds |
| `EXPIRETIME key` | Return the Unix expiry timestamp in seconds |
| `PEXPIRETIME key` | Return the Unix expiry timestamp in milliseconds |
| `EXPIRE key seconds [NX or XX or GT or LT]` | Set relative expiry in seconds |
| `PEXPIRE key milliseconds [NX or XX or GT or LT]` | Set relative expiry in milliseconds |
| `EXPIREAT key timestamp [NX or XX or GT or LT]` | Set absolute Unix expiry in seconds |
| `PEXPIREAT key timestamp [NX or XX or GT or LT]` | Set absolute Unix expiry in milliseconds |
| `PERSIST key` | Remove expiry; return `1` when changed, otherwise `0` |

`TTL`, `PTTL`, `EXPIRETIME`, and `PEXPIRETIME` return `-2` for missing keys and `-1` for persistent
keys. Expiry-setting commands accept one optional `NX`, `XX`, `GT`, or `LT` condition and return `1`
when applied, otherwise `0`. Nonpositive relative deadlines or past absolute deadlines delete the
key.

### Bitmaps

Bit offsets start at the most significant bit of the first byte. Missing bits read as `0`; writes
extend strings with zero bytes.

| Command | Behavior |
| --- | --- |
| `SETBIT key offset value` | Set a bit to `0` or `1`; return its previous value |
| `GETBIT key offset` | Read a bit |
| `BITCOUNT key [start end [BYTE or BIT]]` | Count set bits in the whole string or an inclusive range |
| `BITOP AND destination key [key ...]` | Store the bitwise AND of source strings |
| `BITOP OR destination key [key ...]` | Store the bitwise OR of source strings |

For `BITCOUNT`, choose the literal `BYTE` or `BIT`; the default is `BYTE`. Negative indexes count
from the end. `BITOP` pads shorter sources with zeros and returns the result's byte length. Only
`AND` and `OR` are supported.

### Lists

Lists contain ordered string values.

| Command | Behavior |
| --- | --- |
| `RPUSH key value [value ...]` | Append values; return the resulting length |
| `LPUSH key value [value ...]` | Prepend values; return the resulting length |
| `LLEN key` | Return the list length |
| `LRANGE key start stop` | Read an inclusive range; negative indexes count from the end |
| `LPOP key [count]` | Remove one value or up to `count` values from the left |
| `BLPOP key timeout` | Wait for and remove a value from one list |

`BLPOP` accepts a timeout in seconds, including fractions; `0` waits indefinitely. Multiple-key
`BLPOP` is not implemented.

### Streams

Streams contain entries ordered by IDs, with field/value pairs in each entry.

| Command | Behavior |
| --- | --- |
| `XADD key id field value [field value ...]` | Append an entry; return its ID |
| `XRANGE key start end` | Read entries between inclusive ID bounds; use `-` and `+` for unbounded ends |
| `XREAD [BLOCK milliseconds] STREAMS key [key ...] id [id ...]` | Read entries after the corresponding IDs from one or more streams |

`XADD` accepts explicit IDs, `*` for automatic IDs, and `milliseconds-*` for automatic sequence
numbers. `XREAD BLOCK 0` waits indefinitely; blocking reads accept `$` to wait for entries newer
than the current stream tail. Consumer groups and trimming are not implemented.

### Sorted sets

Sorted sets contain unique members with floating-point scores, ordered by score and then member
name.

| Command | Behavior |
| --- | --- |
| `ZADD key score member [score member ...]` | Add or update members; return the number of new members |
| `ZRANK key member` | Return a member's zero-based rank, or null |
| `ZRANGE key start stop` | Read an inclusive range by rank; negative indexes count from the end |
| `ZCARD key` | Return the number of members |
| `ZSCORE key member` | Return a member's score, or null |
| `ZREM key member [member ...]` | Remove members; delete the key when the set becomes empty |

Optional Redis flags such as `ZADD NX` and `ZRANGE WITHSCORES` are not implemented.

### Geospatial indexes

Geo indexes use sorted sets, so their members and encoded scores are also accessible through
sorted-set commands.

| Command | Behavior |
| --- | --- |
| `GEOADD key longitude latitude member [longitude latitude member ...]` | Add or update locations; return the number of new members |
| `GEOPOS key member [member ...]` | Return longitude/latitude pairs, with null entries for missing members |
| `GEODIST key member1 member2 [unit]` | Return the distance, or null if either member is missing |
| `GEOSEARCH key ...` | Find members inside a circle or box |

`GEOSEARCH` accepts one center and one search shape:

```text
GEOSEARCH key
  (FROMMEMBER member | FROMLONLAT longitude latitude)
  (BYRADIUS radius unit | BYBOX width height unit)
  [ASC | DESC] [COUNT count [ANY]]
  [WITHDIST] [WITHHASH] [WITHCOORD]
```

Distance units are `m`, `km`, `ft`, and `mi`; `GEODIST` defaults to meters. Longitudes must be
within `[-180, 180]` and latitudes within `[-85.05112878, 85.05112878]`, including the endpoints.
Coordinates are decoded to geohash cell centers, and distances use Redis's Haversine formula.
Searches scan the sorted set.

## Connection and key commands

| Command | Behavior |
| --- | --- |
| `PING [message]` | Return `PONG`, or echo the supplied message |
| `ECHO message` | Return the message |
| `TYPE key` | Return `string`, `list`, `stream`, `zset`, or `none` |
| `KEYS pattern` | List matching, unexpired keys; `*` matches all keys |
| `QUIT` | Reply `OK` and close the connection |
| `RESET` | Clear subscriptions, queued transaction, watched keys, and authenticated identity |
| `CONFIG GET parameter [parameter ...]` | Return matching configuration names and values; supports all data-file settings above and wildcard patterns |

Bitmaps report type `string`; geo indexes report type `zset`.

## Authentication

| Command | Behavior |
| --- | --- |
| `ACL WHOAMI` | Return the connection's username |
| `ACL GETUSER username` | Return `flags` and `passwords`, or null for an unknown user |
| `ACL SETUSER username [rule ...]` | Create or update a user with `>password` and `nopass` rules |
| `AUTH password` | Authenticate as `default` |
| `AUTH username password` | Authenticate as a named user |

The default user initially has the `nopass` flag and an empty password list. Adding a password
removes `nopass` and stores its SHA-256 hash. Multiple passwords can be added; `nopass` clears them.
Existing authenticated connections remain authenticated when passwords change. User configuration is
held in memory; command/key permission rules and other ACL rules are not implemented.

Quote password rules in a shell so `>` is passed to Redis:

```sh
redis-cli ACL SETUSER default '>secret'
redis-cli --user default --askpass ACL WHOAMI
```

## Pub/sub

| Command | Behavior |
| --- | --- |
| `SUBSCRIBE channel [channel ...]` | Subscribe to channels |
| `UNSUBSCRIBE [channel ...]` | Unsubscribe from named channels, or all channels |
| `PSUBSCRIBE pattern [pattern ...]` | Subscribe using channel patterns |
| `PUNSUBSCRIBE [pattern ...]` | Remove named patterns, or all patterns |
| `PUBLISH channel message` | Deliver to channel and pattern subscribers; return the delivery count |
| `SSUBSCRIBE channel [channel ...]` | Subscribe to shard channels |
| `SUNSUBSCRIBE [channel ...]` | Remove named shard subscriptions, or all shard subscriptions |
| `SPUBLISH channel message` | Publish to shard subscribers |

While subscribed, a connection accepts only subscription/unsubscription commands, `PING`, `QUIT`,
and `RESET`. `PING` returns a pub/sub `pong` array in this mode. Connections have bounded output
queues; ordinary replies apply backpressure, and pub/sub or replication consumers exceeding 8 MiB of
queued output are disconnected. Shard channels are local to this server; Redis Cluster routing is
not implemented.

## Transactions

| Command | Behavior |
| --- | --- |
| `MULTI` | Start queueing commands |
| `EXEC` | Execute queued commands and return their replies |
| `DISCARD` | Discard queued commands and clear watched keys |
| `WATCH key [key ...]` | Watch keys for changes before `EXEC` |
| `UNWATCH` | Clear watched keys |

If a watched key changes or expires, `EXEC` returns a null array. Unknown commands, invalid
subcommands, and invalid argument counts abort the transaction with `EXECABORT`. Blocking list and
stream reads perform an immediate read inside `EXEC` instead of waiting.

## Replication

| Command | Behavior |
| --- | --- |
| `INFO [replication]` | Return replication role, ID, offset, and link information |
| `REPLCONF LISTENING-PORT port` | Accept a replica's advertised port |
| `REPLCONF IP-ADDRESS address` | Accept a replica's advertised address |
| `REPLCONF CAPA capability [CAPA capability ...]` | Accept replica capabilities |
| `REPLCONF GETACK *` | Request a replica offset acknowledgment |
| `REPLCONF ACK offset` | Record a replica acknowledgment |
| `PSYNC replication-id offset` | Start a full-resynchronization handshake |
| `WAIT replicas timeout` | Wait for acknowledgments of this connection's writes; timeout is in milliseconds |

Successful data mutations are propagated to connected replicas. Full resynchronization transfers and
installs an RDB snapshot of all supported data types and expirations at the matching replication
offset. Replicas reject external data writes with `READONLY`; the internal follower applies
replicated mutations. Partial resynchronization and automatic reconnection are not implemented.

## RDB loading

At startup, the server reads `dir/dbfilename`. A missing file starts an empty store. The loader
restores database 0, including strings, plain lists, binary-score sorted sets, and stream listpacks
without consumer groups. Strings support integer and LZF encodings. Expiry timestamps in seconds or
milliseconds are preserved. Expired keys are skipped; future expirations remain active for `GET` and
`KEYS`.

Other RDB encodings, stream consumer groups, saving snapshots to disk (`SAVE`/`BGSAVE`), checksum
validation, and selecting other databases are not implemented. Replication generates snapshots in
memory.

## AOF persistence

AOF settings can be supplied as startup flags and inspected with `CONFIG GET`:

```sh
./run.sh --appendonly yes --appendfsync everysec
redis-cli CONFIG GET 'append*' dir
```

`appenddirname` specifies the subdirectory beneath `dir` intended for AOF and manifest files;
`appendfilename` specifies the AOF base filename. With `--appendonly yes`, startup creates the
directory and these files:

```text
<dir>/<appenddirname>/
├── <appendfilename>.1.incr.aof
└── <appendfilename>.manifest
```

The incremental AOF starts empty. The manifest contains:

```text
file <appendfilename>.1.incr.aof seq 1 type i
```

Existing AOF and manifest contents are preserved on restart. With `appendonly no`, no AOF directory
or files are created. Successful data mutations are appended as RESP commands, using the same
concrete mutation metadata as replication. Reads, errors, discarded transactions, and commands that
produce no mutation are excluded. Queued writes are recorded when `EXEC` executes them; trusted
writes applied on replicas are recorded locally too. Construction initializes the files; startup
then replays the manifest's incremental AOF files in sequence before accepting connections. Replay
does not append records or propagate mutations to replicas. `SELECT 0` is accepted; other databases,
RDB base files, and AOF rewriting are not supported. Invalid commands and truncated records stop
startup.

With `appendonly yes`, AOF is the startup data source instead of the standalone RDB file. With
`appendonly no`, normal RDB loading applies. New expiring `SET` records use absolute `PXAT`
timestamps, preserving their deadlines across restarts. Existing relative `EX`/`PX` records cannot
recover their original write time and are interpreted relative to replay time.

Transaction-boundary records and scheduled fsync operations are not implemented.

## Tests

The suites use Python's standard-library `unittest`; no additional test packages are required. Run
commands from the repository root with Python 3.14 or later.

```sh
# All unit and integration tests (integration tests bind ephemeral loopback ports).
uv run python -m unittest discover -s tests -v

# Unit tests only; no listening sockets are required.
uv run python -m unittest discover -s tests/unit -v

# Socket-based client/server, pub/sub, restart, and replication tests.
uv run python -m unittest discover -s tests/integration -v
```

Unit tests cover RESP framing and binary payloads; supported data commands; authentication;
transactions; configuration; RDB parsing, snapshots, and expiry; and AOF initialization, replay, and
corrupt input. Integration tests cover pipelining, protocol error recovery, subscription cleanup,
blocking-client disconnects, replication snapshots and acknowledgments, and AOF-backed server
restarts.

### Stress tests

```sh
# Run all workloads; write a machine-readable report.
uv run python -m tests.stress --output .test-results/stress.json

# Increase load or rerun selected workloads.
uv run python -m tests.stress --scale 2 --scenario pipeline-5000 --scenario replication-burst
```

The default run checks 32 concurrent clients, authenticated writes, pipelines through 5,000
commands, channel/pattern fanout, 1,000 publications inside one transaction, disconnect cleanup,
replicated write bursts, 10,000-command AOF replay, 10,000 deterministic randomized storage
operations, and a 20,000-entry RDB load with expirations. `--scale` accepts values from 1 to 10.
`--scenario` can be repeated; omit it to run every workload.

The runner prints JSON results and exits nonzero if any scenario fails. It uses temporary data
directories and ephemeral loopback ports, cleans up its connections/tasks, and does not touch your
running server or data files. Timings describe a local test run, not a production capacity
guarantee.

## Code organization

| Path | Responsibility |
| --- | --- |
| `app/main.py`, `app/config.py` | Startup and configuration |
| `app/network/server.py`, `app/protocol/` | Connections, output queues, and RESP encoding/decoding |
| `app/commands/registry.py`, `app/commands/executor.py` | Command/subcommand definitions, transaction execution, and mutation propagation |
| `app/commands/handlers/data/` | String, bitmap, list, stream, sorted-set, geo, and key handlers |
| `app/commands/handlers/` | Authentication, configuration, connections, pub/sub, replication, and transactions |
| `app/commands/common.py` | Client sessions, command context, and result types |
| `app/data/common.py` | Storage dataclasses and shared lookup helpers |
| `app/data/store.py`, `app/data/entries/` | Key storage, expiry, versions, and data operations |
| `app/persistence/` | RDB parsing, in-memory snapshots, and AOF initialization, write logging, and replay |
| `app/network/auth.py`, `app/network/pubsub.py` | User state and subscription registries |
| `app/replication/` | Primary write propagation and replica following |
| `tests/unit/`, `tests/integration/`, `tests/stress/` | Regression suites and bounded stress workloads |

Data helpers ending in `_locked` require the caller to hold the store lock. Handlers return
responses and mutation metadata without depending on the executor or server.

## Attribution

The geospatial score encoding was originally based on the [Redis geocoding algorithm
explainer](https://github.com/codecrafters-io/redis-geocoding-algorithm). The code now describes the
algorithm directly; its origin is recorded here.
