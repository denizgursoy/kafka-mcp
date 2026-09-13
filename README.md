
# kafka-mcp

An MCP server that exposes Kafka debugging as tools an LLM can call. It speaks
MCP over stdio and talks to Kafka with [franz-go](https://github.com/twmb/franz-go).

## Start the server

The server reads the `KAFKA_BROKER` environment variable and defaults to
`localhost:9092`.

```sh
make up                                  # local Redpanda + Console (optional)
make build                               # builds bin/kafka-debugger
KAFKA_BROKER=localhost:19092 ./bin/kafka-debugger
```

`make up` publishes the broker on `localhost:19092`, the Schema Registry on
`localhost:18081` and the Redpanda Console on <http://localhost:8080>. The
default `localhost:9092` does not match that broker port, so set
`KAFKA_BROKER` as shown when running against local compose.

`KAFKA_MCP_OUTPUT_DIR` chooses where `search_messages` writes exported results.
It defaults to the system temp directory. Exports are confined to that
directory: `output_file` takes a file name, never a path.

To register it with an MCP client, run the binary as the client's stdio server
command with `KAFKA_BROKER` set in its environment.

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

## Skills

`internal/skills/find-message` describes how these tools are combined to
locate a message from something the user knows about it.

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
