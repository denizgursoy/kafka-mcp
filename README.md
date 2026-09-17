
# kafka-mcp

An MCP server that exposes Kafka debugging as tools an LLM can call. It speaks
MCP over stdio and talks to Kafka with [franz-go](https://github.com/twmb/franz-go).

## Start the server

The server is configured by a JSON file, named by `KAFKA_MCP_CONFIG`. That is
the only environment variable it reads.

```sh
make up                                  # local Redpanda + Console
make build                               # builds bin/kafka-debugger
KAFKA_MCP_CONFIG=kafka-mcp.local.json ./bin/kafka-debugger
```

`kafka-mcp.local.json` is committed and points at the compose broker, so a
clone works without writing any configuration. `make up` publishes the broker
on `localhost:19092`, the Schema Registry on `localhost:18081` and the Redpanda
Console on <http://localhost:8080>.

A relative path is resolved from the working directory, so a config file kept
beside the code needs no absolute path.

## Configuration

```jsonc
{
  "environment": "production",          // free-form label, informational only
  "broker": "kafka-1:9093,kafka-2:9093",
  "read_only": true,
  "output_dir": "/var/tmp/kafka-mcp",
  "tls": { "enabled": true, "ca_file": "/etc/kafka/ca.pem" },
  "sasl": {
    "mechanism": "scram-sha-256",       // plain, scram-sha-256, scram-sha-512
    "user": "kafka-mcp-readonly",
    "password": "{env:KAFKA_PASSWORD}"  // or "password_file": "/run/secrets/kafka"
  }
}
```

Every field is optional except `broker`. A minimal local file:

```json
{ "environment": "local", "broker": "localhost:19092" }
```

The file is the only source of configuration. The server refuses to start
without one rather than guessing a broker address, and `output_dir` defaults to
the system temp directory when omitted. Exports are confined to that directory:
`output_file` takes a file name, never a path.

Keep secrets out of the file with `{env:VAR}` or `password_file`. Unknown keys
are rejected, so a typo like `"readonly"` fails at startup rather than silently
leaving writes enabled.

`server_config` reports the effective configuration at runtime. It never
reports the password.

## Permissions

Three layers, and only one of them is real security:

| Layer | Protects against | Real security? |
| ----- | ---------------- | -------------- |
| `confirm: true` on writes | An LLM changing things on one ambiguous request | No — a guardrail |
| `read_only: true` | Accidental writes to a cluster with no ACLs | No — anyone who can edit the config can turn it off |
| **Kafka ACLs on the SASL principal** | **An unauthorised person** | **Yes — the broker decides** |

### Giving two people different permissions

Kafka enforces permissions against the SASL principal, so two people running
the same server with different credentials get different rights. Adding
partitions requires `ALTER` on the topic:

```sh
# ali may read but not reshape topics
rpk acl create --allow-principal User:ali \
  --operation read,describe --topic orders

# deniz may also add partitions
rpk acl create --allow-principal User:deniz \
  --operation read,describe,alter --topic orders
```

Each points `KAFKA_MCP_CONFIG` at their own file, differing only in `sasl.user`
and the password. When ali calls `add_partitions`, the broker refuses:

```
not authorized to add partitions to "orders": the broker refused this request.
Adding partitions requires ALTER permission on the topic for the principal
this server connects as
```

ali cannot bypass that by editing config or rebuilding the binary, because the
decision is made by Kafka rather than by this server. On a cluster without
ACLs, `read_only: true` is the available protection.

## Running against several environments

Register one MCP server per cluster, each with its own config file:

```jsonc
{
  "mcp": {
    "kafka-local": {
      "type": "local",
      "command": ["kafka-mcp"],
      "environment": { "KAFKA_MCP_CONFIG": "kafka-mcp.local.json" }
    },
    "kafka-prod": {
      "type": "local",
      "command": ["kafka-mcp"],
      "enabled": false,
      "environment": { "KAFKA_MCP_CONFIG": "/etc/kafka-mcp/prod.json" }
    }
  }
}
```

The server name becomes part of every tool name, so `kafka-prod_describe_topic`
is visibly different from `kafka-local_describe_topic`.

Each enabled server costs context: these tools are roughly 8k tokens of
definitions. Enabling three environments spends about 24k tokens before you
type anything, so enable only what you need and put production behind an agent:

```jsonc
{
  "tools": { "kafka-prod*": false },
  "agent": {
    "kafka-prod": {
      "description": "Debugging against production Kafka.",
      "tools": { "kafka-prod*": true }
    }
  }
}
```

## Tools

### `list_topics`

Lists the topics on the cluster, sorted alphabetically, with their count.

| Parameter | Type   | Required | Meaning                                                       |
| --------- | ------ | -------- | ------------------------------------------------------------- |
| `search`  | string | no       | Substring filter on the topic name, matched case-insensitively |

```json
{"name": "list_topics", "arguments": {"search": "orders"}}
```

```json
{"topics": ["orders", "orders-dlq"], "count": 2}
```

### `describe_topic`

Reports a topic's partitions, offset ranges, message count, time span and full
configuration. Use it before searching to see how much data a search would read
and how far back the topic can hold data at all.

| Parameter | Type   | Required | Meaning              |
| --------- | ------ | -------- | -------------------- |
| `topic`   | string | yes      | Topic to describe    |

```json
{"topic": "orders", "partition_count": 1, "message_count": 3,
 "partitions": [{"partition": 0, "start_offset": 0, "end_offset": 3, "message_count": 3}],
 "configs": [{"key": "cleanup.policy", "value": "delete", "source": "DYNAMIC_TOPIC_CONFIG", "is_default": false},
             {"key": "retention.ms", "value": "604800000", "source": "DEFAULT_CONFIG", "is_default": true}]}
```

`configs` lists every topic config as the string Kafka reports, where `-1`
means unlimited. `is_default` is true when the value is inherited rather than
set on the topic. Two entries decide whether a message can still exist at all:
`retention.ms` (how long messages are kept) and `cleanup.policy` (`compact`
keeps only the latest message per key).

### `sample_messages`

Reads a small sample of the newest messages and reports what they look like:
value formats, JSON field paths with their types, key statistics, and which
value fields carry the message key. Use it before searching to decide how to
search.

| Parameter         | Type   | Required | Meaning                                  |
| ----------------- | ------ | -------- | ---------------------------------------- |
| `topic`           | string | yes      | Topic to sample                          |
| `sample_size`     | int    | no       | Messages to read in total. Default 20    |
| `partitions`      | int[]  | no       | Restrict to these partitions             |
| `max_value_bytes` | int    | no       | Value bytes per message. Default 512     |

```json
{"value_formats": {"json": 20, "text": 0, "binary": 0},
 "json_fields": [{"path": "payload.amount", "types": ["number"], "present": 20, "example": "500"}],
 "key_stats": {"present": 20, "absent": 0, "unique": 20, "all_unique": true},
 "key_in_value": ["payload.orderId"],
 "sampled_ranges": [{"partition": 0, "start": 980, "end": 1000}]}
```

`key_in_value` naming a field means the key is that identifier, so searching
the key alone is the precise, cheap lookup.

### `search_messages`

Scans a bounded range of a topic and returns messages matching a text query, a
structured filter over JSON values, or both. Kafka has no server-side search,
so this reads messages and filters them client-side; the result reports what
was covered.

| Parameter              | Type     | Required | Meaning                                                             |
| ---------------------- | -------- | -------- | ------------------------------------------------------------------- |
| `topic`                | string   | yes      | Topic to search                                                      |
| `query`                | string   | no\*     | Text to look for                                                     |
| `filter`               | object   | no\*     | Structured filter over JSON values                                   |
| `search_in`            | string[] | no       | `value`, `key`, `headers`. Default value and key                     |
| `match`                | string   | no       | `contains` (default, case-insensitive), `exact`, `regex`             |
| `partitions`           | int[]    | no       | Restrict to these partitions                                         |
| `from_offset` / `to_offset` | int | no       | Offset window, end exclusive                                         |
| `from_timestamp` / `to_timestamp` | string | no | RFC3339 time window                                             |
| `direction`            | string   | no       | `newest_first` (default) or `oldest_first`                           |
| `max_matches`          | int      | no       | Stop after this many matches. Default 10                             |
| `max_messages_scanned` | int      | no       | Read at most this many messages. Default 10000                       |
| `max_value_bytes`      | int      | no       | Value bytes per match. Default 512                                   |
| `timeout_seconds`      | int      | no       | Wall-clock limit. Default 30                                         |
| `count_only`           | bool     | no       | Return counts only, no message bodies                                |
| `output_file`          | string   | no       | Write every match to this file as JSONL, return the path             |

\* at least one of `query` or `filter` is required. Given both, a message must
satisfy both.

Searching by key is far more precise when the key is the identifier:

```json
{"topic": "orders", "query": "order-123", "search_in": ["key"], "match": "exact"}
```

With the default value search, `123` would also match `"amount": 1123`.

#### Filter grammar

A node is `{"and":[…]}`, `{"or":[…]}`, `{"not":{…}}`, or a leaf
`{"field":…, "op":…, "value":…}`.

```json
{"and": [
  {"field": "eventType", "op": "eq", "value": "NEW"},
  {"field": "payload.amount", "op": "gte", "value": 500}
]}
```

- **Paths** are dotted, with `items[0]` for an index and `items[*]` for any element
- **Operators:** `eq ne gt gte lt lte contains starts_with ends_with regex in exists is_null is_not_null is_true is_false`
- `exists`, `is_null`, `is_not_null`, `is_true`, `is_false` take no `value`
- A missing field never matches. `is_null` requires the field to be present and
  null; `{"not": {… "is_null"}}` also matches messages lacking the field
- `is_true` and `is_false` match only real JSON booleans, never `"true"` or `1`
- Messages whose value is not JSON are counted in `non_json_skipped`

#### Result

```json
{"topic": "orders", "match_count": 1,
 "matches": [{"partition": 0, "offset": 17, "key": "order-42", "value": "...", "encoding": "utf8"}],
 "scanned_messages": 120, "scanned_ranges": [{"partition": 0, "start": 0, "end": 120}],
 "stopped_reason": "range_exhausted", "complete": true}
```

`complete` is true only when the whole range was read. An empty match list
means "not there" only if `complete` is true; otherwise check `stopped_reason`
and narrow the search.

For large result sets, use `count_only` to learn how many matches exist, then
`output_file` to write them out instead of returning them.

### `get_message`

Reads one message at an exact offset, plus optional neighbours.

| Parameter         | Type   | Required | Meaning                                       |
| ----------------- | ------ | -------- | --------------------------------------------- |
| `topic`           | string | yes      | Topic to read from                            |
| `partition`       | int    | yes      | Partition to read from                        |
| `offset`          | int    | yes      | Exact offset to read                          |
| `context`         | int    | no       | Also return this many messages either side    |
| `max_value_bytes` | int    | no       | Value bytes to return. Default 4096           |

```json
{"name": "get_message", "arguments": {"topic": "orders", "partition": 0, "offset": 17, "context": 1}}
```

Values that are not valid UTF-8 are base64 encoded, with `encoding` set to
`base64`.

### `list_consumer_groups`

Lists consumer groups with their state, member count and the topics they
consume.

| Parameter | Type     | Required | Meaning                                        |
| --------- | -------- | -------- | ---------------------------------------------- |
| `topic`   | string   | no       | Only groups consuming or committed to this topic |
| `states`  | string[] | no       | Filter by state, e.g. `Stable`, `Empty`        |

```json
{"groups": [{"group": "payments", "state": "Stable", "members": 2, "topics": ["orders"]}], "count": 1}
```

A group in state `Empty` can still report lag: committed offsets outlive the
consumers that made them. Kafka has no topic-to-group index, so filtering by
`topic` describes every group on the cluster.

### `consumer_lag`

Measures how far behind a topic's consumers are, how fast messages are produced
and consumed, and when the backlog will clear.

| Parameter            | Type   | Required | Meaning                                                     |
| -------------------- | ------ | -------- | ----------------------------------------------------------- |
| `topic`              | string | yes      | Topic to measure                                             |
| `group`              | string | no       | Defaults to every group consuming the topic                  |
| `sample_seconds`     | int    | no       | Consume-rate sample window. Default 5. **The call blocks**   |
| `skip_consume_rate`  | bool   | no       | Return immediately, without a rate or estimate               |

```json
{"topic": "orders", "total_lag": 4200,
 "produce_rate": {"last_minute": {"messages": 3000, "per_second": 50, "per_minute": 3000, "per_hour": 180000}},
 "groups": [{"group": "payments", "state": "Stable", "members": 2, "lag": 4200,
             "consume_rate": {"per_second": 120, "sampled_seconds": 5},
             "drain_per_second": 70, "eta_seconds": 60, "eta_human": "1m 0s",
             "status": "draining"}]}
```

The two rates are measured differently, and the output says so:

- **`produce_rate`** — historical fact, from message timestamps, over the last
  second, minute and hour. `window_truncated` marks a topic younger than the
  window.
- **`consume_rate`** — a sample: the committed offset is read, then read again
  `sample_seconds` later. `sample_inconclusive` means nothing moved.

The backlog drains at the consume rate **minus** the produce rate. `status`
says what the numbers mean: `caught_up`, `draining` (with an ETA), `growing`
(never clears, with `growing_by_per_minute`), `stalled`, `no_active_consumers`,
or `not_measured`. An ETA is only given when the lag is genuinely shrinking.

### `server_config`

Reports the effective configuration: brokers, environment label, authentication
mechanism and principal, TLS, read-only state, export directory and the tools
this server exposes. Takes no parameters. The password is never reported.

Use it when a result is surprising: an empty topic list means something very
different on a local broker than on production.

### `add_partitions`

Adds partitions to a topic. **Irreversible** — Kafka cannot reduce a partition
count.

| Parameter | Type | Required | Meaning |
| --------- | ---- | -------- | ------- |
| `topic` | string | yes | Topic to change |
| `partitions` | int | yes | Final total, not the number to add. Repeating a call is safe |
| `confirm` | bool | no | Default false: preview only, nothing changes |
| `acknowledge_key_ordering` | bool | no | Required when messages are keyed |
| `sample_size` | int | no | Messages inspected for keys. Default 20 |

Without `confirm` it reports what would happen: current and target counts,
whether messages are keyed, which consumer groups will rebalance, and warnings.

Adding partitions changes which partition a key hashes to, so existing keys
lose their ordering guarantee. A keyed topic therefore requires
`acknowledge_key_ordering` as well. Requesting fewer partitions than the topic
has is refused with an explanation rather than attempted.

## Skills

- `internal/skills/find-message` — locating a message from something the user
  knows about it.
- `internal/skills/check-lag` — measuring lag and throughput, and judging when
  a backlog will clear.
- `internal/skills/scale-partitions` — deciding whether more partitions will
  help, and adding them safely.

## Development

| Command                | Purpose                                  |
| ---------------------- | ---------------------------------------- |
| `make up` / `make down`| Start / stop Redpanda and Console        |
| `make build`           | Build `bin/kafka-debugger`               |
| `make run`             | Run the server from source               |
| `go test ./...`        | All tests, including container tests     |
| `go test -short ./...` | Tests that need no containers            |
| `go vet ./...`         | Vet all packages                         |

Tests run against real containers started by `internal/domain/testenv` (a Redpanda
broker plus Console), so Docker must be available for the full suite.
