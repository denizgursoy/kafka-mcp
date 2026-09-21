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
cmd/server/main.go        MCP server entrypoint: config, HTTP, one server per cluster
internal/tools/
  tools.go                Which tools an endpoint exposes, and their registration
  <tool>/                 One package per MCP tool
internal/domain/          Everything shared by more than one tool
  config/                 The server's own configuration
  kafkaclient/            The Kafka connections, one per cluster
  records/                Reading and rendering Kafka records
  testenv/                Test container environment (broker + Console)
skills/kafka-debugging/   One umbrella skill, one guide per scenario
docker-compose.yml        Local Redpanda + Redpanda Console
Makefile                  Build, run and compose targets
```

## Where code lives

There are three places code can go, and which one is decided by how many tools
use it.

Every directory under `internal/tools` is one MCP tool, so the list of
directories is the list of tools the server has. The single file at the top of
it, `tools.go`, is the registry: it says which of those tools a given endpoint
exposes and wires each one to its cluster. Nothing else goes there.

- **Used by one tool** — keep it in that tool's own package, as another file in
  the same directory. Do not give it a package of its own: a package used from
  exactly one place is indirection without a reader.
- **Used by more than one tool** — give it its own package under
  `internal/domain`, named for what it does (`internal/domain/records`,
  `internal/domain/kafkaclient`).

Nothing else belongs at the top of `internal`: shared code goes in
`internal/domain` and tools in `internal/tools`. Skills are not code and live
at the repository root, under `skills/`.

Move a helper out of a tool package the moment a second tool needs it, and move
it back if it ever drops to one caller again. **Ask the user for approval
before performing that move**, since it changes the layout other work depends
on.

## Skill-driven development

Tools exist to serve skills. `skills/kafka-debugging/` holds one umbrella
`SKILL.md` that routes to a guide per scenario, and each guide describes a real
debugging scenario. The tools are whatever those scenarios need — not a
wishlist of Kafka features. So development starts from the skill, never from
the tool.

A new scenario is a new guide beside the others plus a row in the umbrella's
routing table, not a new `SKILL.md`: a second `SKILL.md` under `skills/` would
be discovered as a separate skill, and the discovery rule requires its `name`
to match its directory.

When asked to build or extend a skill, work in this order and **do not write
code before step 4**.

### 1. Understand the scenario

Read the skill and restate what the user is actually trying to do: what they
have at the start, what they need at the end, and what decisions happen in
between. Ask about anything ambiguous. A tool built for a misunderstood
scenario is wasted work no matter how well it is written.

### 2. Work out which existing tools already cover it

List the tools the server already exposes and map each step of the scenario to
one. Reuse beats addition: a parameter on an existing tool is usually better
than a new tool. Check `internal/tools/tools.go` for what is actually
registered, not what a skill file claims — skills may name tools that do not
exist yet.

### 3. State the gap and get approval

For every step no existing tool covers, tell the user, before implementing:

- the tool name,
- what it does and what it returns,
- its parameters and which are optional,
- which step of the skill needs it and why an existing tool cannot serve it.

Then **wait for approval**. Do not start implementing tools that have not been
agreed. If the scenario turns out to need no new tools, say so instead of
inventing work.

### 4. Build what was agreed

Implement the approved tools with the per-tool workflow below, then update the
skill so its steps name the tools that now exist, and update `README.md`.

A skill must never reference a tool the server does not expose. If a skill
names a missing tool, that is a gap to raise in step 3, not something to leave
in place.

## Workflow for adding a new tool

Follow these steps in order. Do not skip ahead.

### 1. Write the test first

Tests are written **before** the implementation. Every new tool starts with a
failing test.

- Write the test in the tool's own package (see step 2).
- Assert on real behaviour against a real broker, not on mocks.
- Run the test and confirm it fails for the right reason (missing behaviour,
  not a compile error in unrelated code) before writing any implementation.

### 2. Create a new package under `internal/tools` for each tool

Each tool gets its own package. Do not add new tools to an existing package.

```
internal/tools/listtopics/
internal/tools/getmessage/
internal/tools/topicmetadata/
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

### 3. Write tests against real containers, using `internal/domain/testenv`

Tests must run against a real Kafka broker and a real Redpanda Console. Do not
mock the Kafka client, and do not start containers yourself: every suite starts
its environment through `internal/domain/testenv`.

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
| `Reader()`                    | `*records.Reader` for tools that read messages |
| `ClusterClient(t, readOnly)`  | Production-shaped client for testing a tool    |
| `Broker()`                    | Host address for `kgo.SeedBrokers`             |
| `SchemaRegistry()`            | Schema Registry host address                   |
| `AdminAPI()`                  | Redpanda Admin API host address                |
| `ConsoleURL()`                | Redpanda Console browser URL                   |
| `CreateTopic(t, prefix)`      | Uniquely named topic, deleted by `Stop`        |
| `CreateTopics(t, prefixes...)`| Same, for several topics at once               |
| `CreateTopicWithPartitions(t, prefix, n)` | Topic with a chosen partition count |
| `CreateTopicWithConfig(t, prefix, configs)` | Topic with topic-level configs   |
| `Produce(t, topic, messages...)` | Produce records, returns their offsets      |
| `ConsumeAndCommit(t, topic, group, n)` | Consume and commit n records as a real group member |
| `DeleteTopics(t, topics...)`  | Delete topics early                            |
| `UniqueName(prefix)`          | Unique name for topics, groups, and so on      |
| `TopicExists(t, topic)`       | Whether the broker has the exact topic         |
| `PartitionCount(t, topic)`    | Current partition count from broker metadata   |
| `TopicConfig(t, topic, key)`  | One effective topic config value               |

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
- Write every case out in full with its own `s.Run("name", func() { ... })`.
  Do not build a slice of cases and iterate over it: a table hides what each
  case actually asserts, and a failure points at the loop rather than at the
  behaviour that broke.

  ```go
  s.Run("is_null matches an explicit null", func() {
      s.Require().True(s.match(`{"field":"payload.cancelledAt","op":"is_null"}`),
          "a field present and set to null must match, because that is the state is_null names")
  })

  s.Run("is_null does not match a missing field", func() {
      s.Require().False(s.match(`{"field":"payload.missing","op":"is_null"}`),
          "a field that is absent is a different state from one set to null, and conflating them hides schema drift")
  })
  ```

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
// internal/tools/listtopics/listtopics.go
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

`internal/tools/tools.go` then contains exactly one line per tool, and nothing
else about it:

```go
endpoint.add("list_topics", func() { listtopics.Register(server, kafka.Admin()) })
```

The name is passed with the registration rather than kept in a second list,
because `server_config` reports what the endpoint exposes and the MCP server
cannot be asked what it holds. `add` skips a tool the cluster switched off in
its `tools` configuration and records the name only when it registered, so the
two can never disagree.

The name also goes in `tools.Names()`, which is every tool this server has,
whatever a given endpoint exposes. That list is what a cluster's `tools`
configuration is validated against, so a tool missing from it could never be
switched off. `tools_test.go` asserts a writable cluster with nothing disabled
exposes exactly `Names()`, so the two cannot drift.

No tool names, descriptions, schemas, handlers or Kafka logic in `cmd/server`.
Adding a tool must never mean touching `main`, which knows only that
`tools.Register` exists.

Write tool descriptions and `jsonschema` tags for an LLM caller. State what the
tool returns and what each parameter does, including whether it is optional and
whether matching is case-sensitive.

### 6. Verify end to end

Building is not verification. Confirm the tool works over the real MCP protocol,
against a running server:

```sh
make up
make run &                    # serves kafka-mcp.local.yaml on :8090

curl http://localhost:8090/healthz

# Initialize, keeping the session id the server returns.
SID=$(curl -s -D- -X POST http://localhost:8090/mcp/local \
  -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"t","version":"1"}}}' \
  | grep -i '^mcp-session-id:' | tr -d '\r' | awk '{print $2}')

curl -s -X POST http://localhost:8090/mcp/local -H "Mcp-Session-Id: $SID" \
  -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","method":"notifications/initialized"}' > /dev/null

curl -s -X POST http://localhost:8090/mcp/local -H "Mcp-Session-Id: $SID" \
  -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"list_topics","arguments":{}}}'
```

Responses come back as `text/event-stream`, so each is a line beginning
`data: `. The session id is required on every request after `initialize`; without
it the server starts a new session and the call fails.

Also confirm an unknown cluster is refused, since the routing is what binds a
session to one cluster:

```sh
curl -s -o /dev/null -w '%{http_code}\n' -X POST http://localhost:8090/mcp/nope \
  -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"t","version":"1"}}}'
# 400
```

## Commands

| Command             | Purpose                                     |
| ------------------- | ------------------------------------------- |
| `make up`           | Start Redpanda and Console                  |
| `make down`         | Stop containers                             |
| `make build`        | Build the server binary                     |
| `make run`          | Run the server on :8090                     |
| `go test ./...`     | Run all tests, including container tests    |
| `go test -short ./...` | Run tests without containers             |
| `go vet ./...`      | Vet all packages                            |

Local endpoints from `docker-compose.yml`:

- Kafka broker (host): `localhost:19092`
- Schema Registry: `localhost:18081`
- Redpanda Console: <http://localhost:8080>

The server requires Go 1.27 and uses `into` for lifecycle, `ada` for HTTP with
`StartWithContext`, `logi` for logging, and `chu` for configuration.
`CONFIG_FILE` selects a YAML or JSON file; chu's
standard loaders also support file discovery, HTTP and environment overrides
with the `KAFKA_MCP_` prefix. Unknown fields are ignored. At least one cluster
with a broker is required. `kafka-mcp.local.yaml` in the repository root points
at the compose broker and is what local runs use.

## Conventions

- A tool that changes anything — cluster configuration, consumer offsets, or
  message data — must call `kafkaclient.RequireWritable` before the call that
  mutates, so a read-only server refuses it. Put the check immediately before
  the mutation, so every other refusal is reported on its own terms. A tool
  whose only purpose is to write refuses outright instead, preview included,
  because a preview of a capability the server does not have is misleading.
  Destructive changes also take a `confirm` parameter and do nothing without
  it.
- A tool whose only purpose is to change the cluster it is bound to is not
  registered at all on a read-only cluster. `tools.Register` gates it on
  `kafka.Config().ReadOnly`, so the endpoint's tool list is the list of what it
  can do, and a caller spends no call discovering a refusal. This decides what
  is advertised, not what is permitted: `RequireWritable` stays inside the tool,
  so a registration mistake still cannot write. A tool that only reads, and a
  tool that writes to a cluster chosen per call (`copy_message`), stay
  registered everywhere.
- `server_config` reports the tools of **its own endpoint**. Its list is built
  in `tools.Register` by the same call that registers each tool, because the MCP
  server offers no way to read back what was added. A registration that does not
  go through `endpoint.add` makes `server_config` lie about what the endpoint
  has, which is worse than not reporting the list at all.
  `internal/tools/tools_test.go` asserts the two agree on a read-only cluster, a
  writable one, and one that switches tools off.
- A cluster may withhold individual tools with a `tools` map in its
  configuration, keyed by tool name. Absence means enabled, so a deployment
  lists only what it withholds. The names are checked against `tools.Names()` at
  startup and an unknown one stops the server, because a typo would leave the
  tool it was meant to withhold exposed. `server_config` may not be switched
  off: without it a session cannot tell a withheld tool from a missing feature.
  This narrows an endpoint and never widens one — `read_only` still decides
  first.
- `read_only` is enforced by this server and never delegated to Kafka ACLs.
  Many clusters have no ACLs at all, so a tool that relies on the broker to
  refuse it has no protection there. Where ACLs do exist they are a second and
  stronger barrier, not a replacement.
- A skill whose purpose is to change something checks `server_config` before
  walking the user through the steps, so a read-only server fails at the start
  rather than after the user has already acted.
- `read_only` protects the cluster being **written to**, not the one being read
  from. A tool that moves data between clusters checks the destination, so a
  message can still be rescued out of a read-only production cluster.
- One server serves several clusters, each on its own HTTP path under `/mcp/`.
  A tool is bound to one cluster at registration and must not take a cluster
  parameter, so a caller cannot redirect it. The exceptions are `copy_message`,
  which needs a destination, and `list_clusters`, which reports the roster;
  both take the registry rather than a single client.
- Never commit unless the user explicitly asks.
- Keep `README.md` current. Any new tool, changed flag, changed environment
  variable or changed startup step must be reflected there in the same change.
  The README stays short: what the server is, how to start it, and how to use
  each tool. It always documents how to start the server and how to call the
  tools.
- Prefer editing existing files over creating new ones. The exception is the
  per-tool package structure above, which requires new files.
- Shared test utilities belong in `internal/domain/testenv`, never duplicated across
  test files. Ask for approval before refactoring call sites to move one there.
- Do not add a dependency without checking that it exists and that the API used
  is real. Verify with `go doc` after adding it.
- Run `go vet ./...` and the test suite before reporting work as done.
- Report honestly. If something was not verified, say so rather than implying it
  passed.
