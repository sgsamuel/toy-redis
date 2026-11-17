# toy-redis

An asyncio Redis-compatible server built from the
[CodeCrafters Redis challenge](https://codecrafters.io/challenges/redis). Implements a subset of
Redis over RESP2, including strings, bitmaps, lists, sets, streams, sorted sets, geospatial indexes,
pub/sub, authentication, transactions, replication, and RDB/AOF persistence.

[Quick start](#quick-start) · [Configuration](#configuration) ·
[Command reference](#command-reference) · [Persistence](#persistence) ·
[Tests](#tests) · [Code organization](#code-organization)

## Quick start

Requires Python 3.14 or later and `uv`. Install `redis-cli` separately to try the examples.

Start the server:

```sh
./run.sh --port 6379
```

In another terminal:

```sh
redis-cli PING
redis-cli SET greeting hello
redis-cli GET greeting
```

You can also start the server with `python3 -m app.main` using Python 3.14 or later.

## Configuration

| Option | Default | Purpose |
| --- | --- | --- |
| `--host` | `localhost` | Address to bind |
| `--port` | `6379` | TCP port |
| `--replicaof HOST PORT` | Unset | Follow a primary server |
| `--dir` | Working directory at startup | Base directory for data files |
| `--dbfilename` | `dump.rdb` | RDB filename |
| `--appendonly` | `no` | Enable AOF logging and replay: `yes` or `no` |
| `--appenddirname` | `appendonlydir` | AOF subdirectory under `dir` |
| `--appendfilename` | `appendonly.aof` | AOF base filename |
| `--appendfsync` | `everysec` | Accepted policy: `always`, `everysec`, or `no`; configuration only |

The `--appendfsync` setting is exposed through configuration; fsync scheduling is not implemented.
Inspect data-file settings with `redis-cli CONFIG GET '*'`.

Start a replica of the server on port 6379:

```sh
./run.sh --port 6380 --replicaof localhost 6379
```

Load a snapshot at startup:

```sh
./run.sh --dir /path/to/snapshots --dbfilename dump.rdb
```

## Command reference

Command and subcommand names are case insensitive. Keys, members, channels, usernames, and passwords
are case sensitive. In the syntax below, brackets indicate optional arguments, `|` separates
alternatives, and `...` indicates repetition.

### Strings

Strings hold text or byte values; bitmap operations share the same storage. `STRLEN` measures bytes.

| Command | Behavior |
| --- | --- |
| `SET key value [condition] [expiry]` | Store a value, optionally with a condition and expiry |
| `GET key` | Read a value; return null for a missing or expired key |
| `INCR key` | Increment an integer value; initialize a missing key to `1` |
| `DECR key` | Decrement a signed 64-bit integer, starting from zero for a missing key; preserve its expiry |
| `STRLEN key` | Return the byte length, or `0` for a missing key |

`SET` replaces the previous value and expiry. Its optional arguments are:

```text
condition: NX | XX
expiry:    EX seconds | PX milliseconds | EXAT timestamp | PXAT timestamp
```

`NX` writes only if the key is absent; `XX` writes only if it exists. An unmet condition returns
null without changing the key. Use at most one condition and one expiry option, in either order.
`EXAT` and `PXAT` use absolute Unix timestamps. Both `INCR` and `DECR` preserve existing expiry.

### Key expiry

These commands work on every data type.

| Command | Behavior |
| --- | --- |
| `TTL key` | Return the remaining expiry in seconds |
| `PTTL key` | Return the remaining expiry in milliseconds |
| `EXPIRETIME key` | Return the Unix expiry timestamp in seconds |
| `PEXPIRETIME key` | Return the Unix expiry timestamp in milliseconds |
| `EXPIRE key seconds [NX \| XX \| GT \| LT]` | Set relative expiry in seconds |
| `PEXPIRE key milliseconds [NX \| XX \| GT \| LT]` | Set relative expiry in milliseconds |
| `EXPIREAT key timestamp [NX \| XX \| GT \| LT]` | Set absolute Unix expiry in seconds |
| `PEXPIREAT key timestamp [NX \| XX \| GT \| LT]` | Set absolute Unix expiry in milliseconds |
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
| `BITCOUNT key [start end [BYTE \| BIT]]` | Count set bits in the whole string or an inclusive range |
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

### Sets

Sets contain unique, unordered string members.

| Command | Behavior |
| --- | --- |
| `SADD key member [member ...]` | Add members; return the number of new members |
| `SISMEMBER key member` | Return `1` if the member exists, otherwise `0` |
| `SCARD key` | Return the number of members |
| `SREM key member [member ...]` | Remove members; return the number removed |
| `SMEMBERS key` | Return all members in unspecified order |

Missing keys behave as empty sets. Duplicate members count once. Removing the last member deletes
its key; adding or removing members preserves existing expiry.

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
| `ZADD key [NX \| XX] score member [score member ...]` | Add or update members; return the number of new members |
| `ZRANK key member` | Return a member's zero-based rank, or null |
| `ZRANGE key start stop [WITHSCORES]` | Read an inclusive range by rank, optionally with scores |
| `ZCARD key` | Return the number of members |
| `ZSCORE key member` | Return a member's score, or null |
| `ZREM key member [member ...]` | Remove members; delete the key when the set becomes empty |

`ZADD NX` adds only new members; `ZADD XX` updates only existing members. The flags are mutually
exclusive and case insensitive. The return value counts new members, so `XX` returns `0` even when
scores change. Negative `ZRANGE` indexes count from the end. With `WITHSCORES`, the reply alternates
between members and score strings.

### Geospatial indexes

Geo indexes use sorted sets, so their members and encoded scores are also accessible through
sorted-set commands.

| Command | Behavior |
| --- | --- |
| `GEOADD key [NX \| XX] longitude latitude member [longitude latitude member ...]` | Add or update locations; return the number of new members |
| `GEOPOS key member [member ...]` | Return longitude/latitude pairs, with null entries for missing members |
| `GEODIST key member1 member2 [unit]` | Return the distance, or null if either member is missing |
| `GEOSEARCH key ...` | Find members inside a circle or box |

`GEOADD` supports the same `NX` and `XX` conditions as `ZADD`.

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

### Connection and key commands

| Command | Behavior |
| --- | --- |
| `PING [message]` | Return `PONG`, or echo the supplied message |
| `ECHO message` | Return the message |
| `TYPE key` | Return `string`, `list`, `set`, `stream`, `zset`, or `none` |
| `KEYS pattern` | List matching, unexpired keys; `*` matches all keys |
| `QUIT` | Reply `OK` and close the connection |
| `RESET` | Clear subscriptions, queued transaction, watched keys, and authenticated identity |
| `CONFIG GET parameter [parameter ...]` | Return matching configuration names and values; supports all data-file settings above and wildcard patterns |

Bitmaps report type `string`; geo indexes report type `zset`.

### Authentication

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

### Pub/sub

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

### Transactions

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

### Replication

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

## Persistence

By default, startup loads `dir/dbfilename`. With `--appendonly yes`, it replays the AOF instead.

### RDB snapshots

A missing snapshot starts an empty store. The loader restores database 0, including strings, plain
lists and sets, binary-score sorted sets, and stream listpacks without consumer groups. Strings
support integer and LZF encodings. Expiry timestamps are preserved; expired keys are skipped.

Other RDB encodings, stream consumer groups, checksum validation, and selecting other databases are
not implemented. Replication generates snapshots in memory; saving snapshots to disk with `SAVE` or
`BGSAVE` is not supported.

### Append-only files

Enable AOF logging and startup replay:

```sh
./run.sh --appendonly yes
```

New AOF directories contain an incremental log and a manifest:

```text
<dir>/<appenddirname>/
├── <appendfilename>.1.incr.aof
└── <appendfilename>.manifest
```

Existing files are preserved on restart. Startup replays the manifest's incremental files in
sequence before accepting connections. Invalid commands or truncated records stop startup. Replay
does not append duplicate records or propagate writes to replicas. With `appendonly no`, no AOF
files or directories are created.

Successful mutations are logged as RESP commands. Reads, errors, discarded transactions, and
commands that produce no mutation are excluded. Queued writes are logged when `EXEC` executes them;
writes received by replicas are logged locally too.

New expiring `SET` records use absolute `PXAT` timestamps so deadlines survive restarts. Existing
relative `EX`/`PX` records are interpreted relative to replay time because their original write time
is unavailable.

AOF replay accepts `SELECT 0`. Other databases, RDB base files, AOF rewriting, transaction-boundary
logging, and fsync scheduling are not implemented.

## Tests

Run from the repository root. The suites use Python's standard-library `unittest` with no additional
test packages. Integration tests bind ephemeral loopback ports.

```sh
# All unit and integration tests.
uv run python -m unittest discover -s tests -v

# Run one suite.
uv run python -m unittest discover -s tests/unit -v
uv run python -m unittest discover -s tests/integration -v

# Run a target or an individual module.
uv run python -m unittest discover -s tests/unit/commands/data -v
uv run python -m unittest tests.unit.commands.data.test_sorted_sets -v
uv run python -m unittest tests.integration.replication.test_sync -v
```

Tests are grouped by target:

| Location | Coverage |
| --- | --- |
| `tests/unit/commands/` | Authentication, configuration, transactions, and command errors |
| `tests/unit/commands/data/` | Strings, bitmaps, lists, sets, streams, sorted sets, geo, and expiry |
| `tests/unit/network/`, `tests/unit/protocol/` | Pub/sub state, output queues, and RESP framing |
| `tests/unit/persistence/`, `tests/unit/replication/`, `tests/unit/startup/` | RDB, AOF, replica command handling, and startup |
| `tests/integration/` | Socket-based command, network, persistence, and replication tests |
| `tests/stress/` | Data, network, persistence, and replication workloads |
| `tests/support.py` | Shared command fixtures, clients, and server helpers |

### Stress tests

```sh
# Run all workloads and write a JSON report.
uv run python -m tests.stress --output .test-results/stress.json

# Increase load or select workloads.
uv run python -m tests.stress --scale 2 --scenario pipeline-5000 --scenario replication-burst
```

Workloads cover concurrent and authenticated clients, pipelining, pub/sub fanout, disconnect
cleanup, replication bursts, AOF replay, randomized storage operations, and RDB loading. `--scale`
accepts values from 1 to 10; repeat `--scenario` to select multiple workloads, or omit it to run all
of them. The runner prints JSON results and exits nonzero if any scenario fails.

Stress tests use temporary data directories and ephemeral ports, clean up their connections and
tasks, and leave running servers and existing data files untouched. Timings describe local test runs
rather than production capacity.

## Code organization

| Path | Responsibility |
| --- | --- |
| `app/main.py`, `app/network/config.py` | Startup, command-line options, and configuration |
| `app/network/` | Connections, output queues, authentication state, and subscription registries |
| `app/protocol/` | RESP parsing and response encoding |
| `app/commands/` | Command definitions, handlers, transactions, and mutation propagation |
| `app/data/` | Key storage, data-type operations, expiry, and key versions |
| `app/persistence/` | RDB parsing and snapshots; AOF logging and replay |
| `app/replication/` | Primary write propagation and replica following |
| `tests/` | Unit, integration, and stress suites |

Sorted sets keep a member-to-score dictionary and a list ordered by `(score, member)`. Score
lookups use the dictionary; ranks use binary search. Single inserts and removals still require
list shifts, while batch writes merge or filter the ordered index once. Sets use Python hash sets.

Data helpers ending in `_locked` require the caller to hold the store lock. Command handlers return
responses and mutation metadata for the executor to process.

## Attribution

Geospatial score encoding was originally based on the
[Redis geocoding algorithm explainer](https://github.com/codecrafters-io/redis-geocoding-algorithm).
