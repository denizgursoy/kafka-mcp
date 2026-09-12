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

To register it with an MCP client, run the binary as the client's stdio server
command with `KAFKA_BROKER` set in its environment.

## Tools

### `list_topics`

Lists the topics on the cluster, sorted alphabetically, with their count.

| Parameter | Type   | Required | Meaning                                                       |
| --------- | ------ | -------- | ------------------------------------------------------------- |
| `search`  | string | no       | Substring filter on the topic name, matched case-insensitively |

Omit `search` to list every topic. A search matching nothing returns an empty
list, not an error.

```json
{"name": "list_topics", "arguments": {"search": "orders"}}
```

```json
{"topics": ["orders", "orders-dlq"], "count": 2}
```

## Development

| Command                | Purpose                                  |
| ---------------------- | ---------------------------------------- |
| `make up` / `make down`| Start / stop Redpanda and Console        |
| `make build`           | Build `bin/kafka-debugger`               |
| `make run`             | Run the server from source               |
| `go test ./...`        | All tests, including container tests     |
| `go test -short ./...` | Tests that need no containers            |
| `go vet ./...`         | Vet all packages                         |

Tests run against real containers started by `internal/testenv` (a Redpanda
broker plus Console), so Docker must be available for the full suite.
