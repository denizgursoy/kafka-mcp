# AGENTS.md

Guidance for coding agents working in this repository.

## Project

`kafka-mcp` is an MCP (Model Context Protocol) server that exposes Kafka
debugging capabilities as MCP tools. It is written in Go and uses:

- `github.com/modelcontextprotocol/go-sdk/mcp` — MCP server and tool registration
- `github.com/twmb/franz-go` — Kafka client
- `github.com/twmb/franz-go/pkg/kadm` — Kafka admin operations

Layout:

```
cmd/server/main.go        MCP server entrypoint, tool registration
internal/kafka/           Kafka packages, one per tool
internal/testenv/         Shared test container environment (broker + Console)
skills/kafka-debugger/    Skills describing how tools are used together
docker-compose.yml        Local Redpanda + Redpanda Console
Makefile                  Build, run and compose targets
```

## Workflow for adding a new tool

Follow these steps in order. Do not skip ahead.

### 1. Write the test first

Tests are written **before** the implementation. Every new tool starts with a
failing test.

- Write the test in the tool's own package (see step 2).
- Assert on real behaviour against a real broker, not on mocks.
- Run the test and confirm it fails for the right reason (missing behaviour,
  not a compile error in unrelated code) before writing any implementation.

### 2. Create a new package under `internal/kafka` for each tool

Each tool gets its own package. Do not add new tools to an existing package.

```
internal/kafka/listtopics/
internal/kafka/getmessage/
internal/kafka/topicmetadata/
```

Each package contains:

| File            | Contents                                                  |
| --------------- | --------------------------------------------------------- |
| `<tool>.go`     | Input struct, output struct, and the tool logic            |
| `<tool>_test.go`| Tests for that tool                                        |

Rules:

- The package owns its own `Input` and `Output` types. These types carry the
  `json` and `jsonschema` tags used to generate the MCP tool schema.
- The package exposes a single exported entrypoint that takes a
  `context.Context`, a Kafka client, and the `Input`, and returns the `Output`.
- The package does not import `cmd/server`. Dependencies point inwards only.
- Keep results deterministic. `kadm` returns maps, and Go map iteration order is
  random, so sort any slice before returning it. Non-deterministic output makes
  tests flaky and confuses MCP clients.
- Return an empty slice rather than `nil` for "no results", so the JSON output
  is `[]` instead of `null`.

### 3. Write tests against real containers, using `internal/testenv`

Tests must run against a real Kafka broker and a real Redpanda Console. Do not
mock the Kafka client, and do not start containers yourself: every suite starts
its environment through `internal/testenv`.

`testenv.Start` takes nothing but a `*testing.T`. It creates a Docker network,
starts a Redpanda broker and a Redpanda Console attached to that broker,
connects a Kafka client, and verifies the broker answers a metadata request.

- If anything fails to start, the reason is logged and the test is **failed**.
  A broken Docker setup must never look like a passing test suite.
- If everything starts, the connection details (broker, Schema Registry, Admin
  API, Console URL) are logged, so a human can attach to the same broker or open
  the Console while the suite runs.
- `Stop` tears down the client, Console, broker and network, and deletes every
  topic the environment created. Teardown failures are logged, not fatal.

### 3a. Use a testify suite, one environment per suite

Every test file uses `github.com/stretchr/testify/suite`. Start the environment
in `SetupSuite` and release it in `TearDownSuite`. Those two hooks call nothing
but `testenv`:

```go
type ListTopicsSuite struct {
    suite.Suite

    env *testenv.Environment
}

func TestListTopicsSuite(t *testing.T) {
    suite.Run(t, new(ListTopicsSuite))
}

func (s *ListTopicsSuite) SetupSuite() {
    s.env = testenv.Start(s.T())
}

func (s *ListTopicsSuite) TearDownSuite() {
    s.env.Stop()
}
```

One environment per suite, not per test case: starting containers per case is
far too slow.

### 3b. Utilities live in `testenv`, not in test files

`testenv.Environment` exposes the helpers tests need:

| Method                        | Purpose                                        |
| ----------------------------- | ---------------------------------------------- |
| `Admin()`                     | `*kadm.Client` connected to the broker         |
| `Kafka()`                     | `*kgo.Client` for record-level operations      |
| `Broker()`                    | Host address for `kgo.SeedBrokers`             |
| `SchemaRegistry()`            | Schema Registry host address                   |
| `AdminAPI()`                  | Redpanda Admin API host address                |
| `ConsoleURL()`                | Redpanda Console browser URL                   |
| `CreateTopic(t, prefix)`      | Uniquely named topic, deleted by `Stop`        |
| `CreateTopics(t, prefixes...)`| Same, for several topics at once               |
| `DeleteTopics(t, topics...)`  | Delete topics early                            |
| `UniqueName(prefix)`          | Unique name for topics, groups, and so on      |

Helpers that create or assert on broker state take the **running test's**
`*testing.T` (`s.T()`), not the suite's, so a failure aborts the test that is
actually running.

Topic names are always generated with a unique suffix, because a suite shares
one broker across its cases and a fixed name would let one case see another's
topics.

If a helper is needed in more than one test and does not exist in `testenv`
yet, move it into `testenv` and refactor the call sites. **Ask the user for
approval before performing that refactor.**

Test rules:

- Assert with `s.Require()`, never `s.Assert()` or the bare `s.Equal` forms. A
  failed expectation means the rest of the case is testing garbage, so stop
  there.
- Every assertion carries a message explaining what the failure means. Not
  "topics must contain x", but why that matters:

  ```go
  s.Require().IsIncreasing(out.Topics,
      "topics must be sorted, because kadm returns a map and Go map order is random")
  ```

- Always pass `s.T().Context()` as the `context.Context`. Never
  `context.Background()` in a test: the test's context is canceled when the test
  ends, so a hung call cannot outlive it. The single exception is teardown,
  which must not use a context that is already canceled.
- Cover at least: the normal case, the filtered/parameterised case, the empty
  result case, and the error case.
- `testenv.Start` already skips the test in `-short` mode, so `go test -short
  ./...` stays fast without a `testing.Short()` check in every suite.

Two verified traps worth remembering:

- `kadm.ListTopics` with no topic filter is served from franz-go's metadata
  cache, which is `MetadataMinAge` (5s) old by default, so a freshly created
  topic is invisible for seconds. `testenv` builds its client with a short
  `kgo.MetadataMinAge` to avoid this.
- A suite's `TearDownSuite` runs *before* the cleanups registered on the
  suite-level `*testing.T`. Topic cleanup therefore happens inside `Stop`, not
  via `t.Cleanup`, otherwise it would run after the client was closed and panic.

Redpanda Console has no testcontainers module (`modules/redpandaconsole` does
not exist). `testenv` runs it as a generic container on the same Docker network
as the broker, waiting on its `/admin/health` endpoint.

### 4. Implement the tool

Write the minimum code that makes the tests pass, then refactor.

### 5. Register the tool in the MCP server

Each tool package registers itself. The package exposes a `Register` function
that owns the tool's name, description, schema and handler:

```go
// internal/kafka/listtopics/listtopics.go
func Register(server *mcp.Server, admin *kadm.Client) {
    mcp.AddTool(
        server,
        &mcp.Tool{
            Name:        "list_topics",
            Description: description,
        },
        func(ctx context.Context, req *mcp.CallToolRequest, input Input) (*mcp.CallToolResult, Output, error) {
            out, err := Run(ctx, admin, input)
            if err != nil {
                return nil, Output{}, fmt.Errorf("list topics: %w", err)
            }
            return nil, out, nil
        },
    )
}
```

`main.go` then contains exactly one line per tool, and nothing else about it:

```go
listtopics.Register(server, kafka.Admin())
```

No tool names, descriptions, schemas, handlers or Kafka logic in `main.go`.
Adding a tool must never mean growing `main`.

Write tool descriptions and `jsonschema` tags for an LLM caller. State what the
tool returns and what each parameter does, including whether it is optional and
whether matching is case-sensitive.

### 6. Verify end to end

Building is not verification. Confirm the tool works over the real MCP stdio
protocol:

```sh
make up
go build -o bin/kafka-debugger ./cmd/server

{ printf '%s\n' \
  '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"t","version":"1"}}}' \
  '{"jsonrpc":"2.0","method":"notifications/initialized"}' \
  '{"jsonrpc":"2.0","id":2,"method":"tools/list"}' \
  '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"list_topics","arguments":{}}}'; sleep 5; } \
  | KAFKA_BROKER=localhost:19092 ./bin/kafka-debugger
```

The `sleep` matters: the server exits when stdin closes, so piping input without
holding the pipe open kills it before responses are flushed. Responses may
arrive out of order because requests are handled concurrently; match them by
`id`, not by position.

## Commands

| Command             | Purpose                                     |
| ------------------- | ------------------------------------------- |
| `make up`           | Start Redpanda and Console                  |
| `make down`         | Stop containers                             |
| `make build`        | Build the server binary                     |
| `make run`          | Run the server                              |
| `go test ./...`     | Run all tests, including container tests    |
| `go test -short ./...` | Run tests without containers             |
| `go vet ./...`      | Vet all packages                            |

Local endpoints from `docker-compose.yml`:

- Kafka broker (host): `localhost:19092`
- Schema Registry: `localhost:18081`
- Redpanda Console: <http://localhost:8080>

The server reads `KAFKA_BROKER` and defaults to `localhost:9092`. That default
does not match the compose file's host port, so set `KAFKA_BROKER=localhost:19092`
when running against local compose.

## Conventions

- Never commit unless the user explicitly asks.
- Keep `README.md` current. Any new tool, changed flag, changed environment
  variable or changed startup step must be reflected there in the same change.
  The README stays short: what the server is, how to start it, and how to use
  each tool. It always documents how to start the server and how to call the
  tools.
- Prefer editing existing files over creating new ones. The exception is the
  per-tool package structure above, which requires new files.
- Shared test utilities belong in `internal/testenv`, never duplicated across
  test files. Ask for approval before refactoring call sites to move one there.
- Do not add a dependency without checking that it exists and that the API used
  is real. Verify with `go doc` after adding it.
- Run `go vet ./...` and the test suite before reporting work as done.
- Report honestly. If something was not verified, say so rather than implying it
  passed.
