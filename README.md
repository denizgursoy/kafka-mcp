# kafka-mcp

[![License](https://img.shields.io/github/license/denizgursoy/kafka-mcp?color=blue&style=flat-square)](https://raw.githubusercontent.com/denizgursoy/kafka-mcp/main/LICENSE)
[![Coverage](https://img.shields.io/sonar/coverage/denizgursoy_kafka-mcp?logo=sonarcloud&server=https%3A%2F%2Fsonarcloud.io&style=flat-square)](https://sonarcloud.io/summary/overall?id=denizgursoy_kafka-mcp)

An MCP server that exposes Kafka debugging as tools an LLM can call. It speaks
MCP over HTTP and talks to Kafka with [franz-go](https://github.com/twmb/franz-go).

One server can serve several Kafka clusters. Each is served on its own path, so
a session is bound to one cluster by the endpoint it connects to rather than by
a parameter a caller could forget to send.

## Configuration

`kafka-mcp.{toml,yaml,yml,json}` in the working directory, `~/.config/kafka-mcp/` or `/etc` configures the server.

```yaml
http:
  address: ":8090"
  base_path: /kafka-mcp # optional; prefixes every endpoint and /healthz
output_dir: /var/tmp/kafka-mcp
clusters:
  prod:
    brokers:
      - kafka-1:9093
      - kafka-2:9093
    security:
      tls:
        enabled: true
        ca_file: /etc/kafka/ca.pem
        # cert_file: /etc/kafka/client.pem # optional mTLS; requires key_file
        # key_file: /run/secrets/client.key
      sasl:
        - scram:
            enabled: true
            algorithm: SCRAM-SHA-256 # or SCRAM-SHA-512
            user: kafka-mcp-readonly
            pass: "{env:KAFKA_PASSWORD}" # or password_file: /run/secrets/kafka
  preprod:
    brokers: kafka-preprod:9093
endpoints:
  prod-read:
    cluster: prod
    path: /mcp
    description: Production investigation and debugging
    read_only: true
  prod-write:
    cluster: prod
    path: /mcp/rw
    description: Approved production changes
    tools:
      copy_message: false
  preprod:
    cluster: preprod
    path: /mcp/preprod
```

`clusters` owns Kafka connection details: brokers, TLS and SASL. `endpoints`
owns the MCP route and policy. Several endpoints may reference one cluster, so
the example reuses one production connection at `/kafka-mcp/mcp` in read-only
mode and `/kafka-mcp/mcp/rw` in writable mode. Paths are exact: `/mcp` does not
capture `/mcp/rw`. `description` is optional and is reported by
`server_config` so a caller knows what the endpoint is intended for.

`http.base_path` prefixes endpoint paths and the liveness route. A leading or
trailing slash on an endpoint path is optional. Paths must be unique and may
not be `/`, `/healthz`, or contain a query or fragment. When `path` is omitted,
it defaults to `/mcp/<endpoint-name>`.

For compatibility, a file with no `endpoints` block still creates one endpoint
per cluster at `/mcp/<cluster-name>`. Existing cluster-level `read_only` and
`tools` values continue to apply as a lower bound during migration; an explicit
endpoint cannot widen them. New configurations should put both fields under
`endpoints`.

An endpoint may switch individual tools off, by name:

```yaml
endpoints:
  prod-read:
    cluster: prod
    path: /mcp
    read_only: true
    tools:
      create_topic: false
      commit_offset: false
```

A tool the map does not mention stays exposed, so the file states only what it
withholds rather than relisting every tool and silently losing whatever is added
later. Names are exact and lowercase, as listed by `server_config`. They are
checked at startup: a name that is not a tool stops the
server, because a typo would leave the tool it was meant to withhold exposed.
`server_config` cannot be switched off, since it is how a session learns which
cluster and policy it reached and which tools that endpoint has. The `tools`
map only narrows an endpoint: `read_only: true` still withholds the writing
tools regardless of what the map says.

A minimal local file:

```yaml
clusters:
  local:
    brokers: localhost:19092
endpoints:
  local:
    cluster: local
    path: /mcp/local
```

Run a custom configuration with `CONFIG_FILE=/path/to/config.yaml go run ./cmd/server`.
Without `CONFIG_FILE`, chu discovers `kafka-mcp.{toml,yaml,yml,json}` first in
the working directory, then in the operating system's user config directory
(`~/.config/kafka-mcp/` on Linux), and finally in `/etc`. It uses the first
matching file rather than merging files. Its standard loader order is defaults,
file, HTTP, then environment; environment overrides use the `KAFKA_MCP_` prefix (for example,
`KAFKA_MCP_HTTP_ADDRESS=:9000` or
`KAFKA_MCP_HTTP_BASE_PATH=/kafka-mcp`). Logging can be configured with
`LOG_LEVEL` and `LOG_PRETTY`.

At least one cluster with a broker is required. `http.address` defaults to
`:8090`, `http.base_path` defaults to the HTTP root, and `output_dir` defaults
to the system temp directory. Exports are confined to that directory:
`output_file` takes a file name, never a path.

`brokers` accepts either one address as a scalar or several addresses as a YAML
list. Each address is passed to Kafka as a separate seed broker.

Keep secrets out of the file with `{env:VAR}` or `password_file`. Unknown fields
are ignored by chu.

TLS is configured with `twmb/tlscfg`, using its TLS 1.2 minimum and recommended
cipher suites. TLS uses the system trust store;
`ca_file` adds a custom CA. For mTLS, supply both `cert_file` and `key_file`.
`security.sasl` is a preference-ordered list: each entry enables either `scram`
or `plain`, or uses `oauth` as shown below. For PLAIN, use `plain: {enabled: true, user: alice, pass: "{env:KAFKA_PASSWORD}"}`.
Both accept optional `zid` (authorization identity) and `password_file` instead
of `pass`; SCRAM also accepts `is_token: true` for delegation tokens. Algorithms
are case-insensitive. Disabled entries are ignored; repeated mechanisms and
entries enabling multiple mechanisms are rejected. Fallback negotiates a
broker-supported mechanism; it does not retry invalid credentials.
All Kafka connections, including message-reading sessions, use these settings.

Legacy per-cluster `tls` and `sasl: {mechanism, user, password, password_file}`
remain supported, but cannot be combined with `security` on the same cluster.

For OAUTHBEARER client credentials, use this cluster security block:

```yaml
security:
  tls:
    enabled: true
  sasl:
    - oauth:
        enabled: true
        token_url: https://identity.example.com/realms/apps/protocol/openid-connect/token
        client_id: kafka-mcp
        client_secret: "{env:KAFKA_CLIENT_SECRET}"
        scopes: [kafka]
        timeout: 10s
        # zid: optional-authorization-id
        # extensions: {tenant: example}
```

Alternatively, use `oauth: {enabled: true, token: "{env:KAFKA_TOKEN}"}` for a
static token. Configure exactly one mode. Client-credentials tokens are fetched
on authentication, cached per cluster across admin and reading connections,
and renewed near expiry using `expires_in`. Token requests use the current
authentication context and a timeout (default 10 seconds). Static tokens are
not renewed. The broker must support OAUTHBEARER and trust the identity provider.
Broker `security.tls` settings apply to Kafka connections; HTTPS token endpoints
use the HTTP client's system trust store.

A cluster that cannot be reached at startup is still served, and
`list_clusters` reports it as disconnected. One cluster being down must not
block debugging the others.

`server_config` reports the endpoint name, exact path, description, policy and
the cluster it serves, and never the password. Its `authentication` and
`sasl_user` describe the first configured option; `sasl_options` lists all
configured identities in preference order, not the mechanism negotiated by an
individual broker connection.

### Browser clients (CORS)

A command-line client sends no `Origin` header and needs none of this. A client
running in a browser does: the browser discards the response unless the server
allows the origin, so the defaults already cover the MCP transport.

```yaml
http:
  address: ":8090"
  cors:
    allow_origins: ["https://mcp-client.example"]
    allow_methods: [GET, POST, DELETE, OPTIONS]
    allow_headers: [content-type, accept, authorization, cache-control, last-event-id, mcp-session-id, mcp-protocol-version]
    expose_headers: [Mcp-Session-Id]
    allow_private_network: true
    max_age: 600
```

`http.cors` is ada's CORS middleware configuration, read straight from the
file, so every option that middleware has is available here. Each key is
optional and keeps its own default, so setting `allow_origins` alone does not
drop the rest. The defaults are the values above with `allow_origins: ["*"]`.

Three of them are load-bearing for the MCP transport. `allow_methods` needs
`GET`, `POST` and `DELETE`: requests are posted, the event stream is a GET,
and a client ends its session with DELETE. `allow_headers` needs
`mcp-session-id` and `mcp-protocol-version`, which the client sends from the
second request onwards, and a header missing there fails the whole preflight
rather than being dropped. `Mcp-Session-Id` must stay in `expose_headers`: the
session id arrives on the `initialize` response, and a page that cannot read
it cannot make a second call.

`allow_private_network` answers Chrome's Private Network Access preflight,
which a page on a public address must pass before it may reach a server on a
private or loopback address; it defaults to on, and is granted only on a
preflight the rest of the policy already allowed. Setting `allow_credentials`
together with a wildcard `allow_origins` is refused by the middleware at
startup unless `unsafe_wildcard_origin_with_allow_credentials` is also set,
which it should not be.

**These endpoints have no authentication of their own.** An allowed origin can
drive every tool with the server's Kafka credentials, from any page the
browser's user happens to visit. `allow_origins` is the only built-in HTTP
barrier, so narrow it to the pages that should have that power, protect writable
paths in a reverse proxy, and set `read_only: true` on endpoints that should not
write. A less obvious path such as `/mcp/rw` is not authentication.

## Connecting a client

Register one entry per endpoint you want the client to use:

```jsonc
{
  "mcp": {
    "kafka-local": {
      "type": "remote",
      "url": "http://localhost:8090/mcp/local"
    },
    "kafka-prod-read": {
      "type": "remote",
      "url": "http://localhost:8090/mcp",
      "enabled": false
    }
  }
}
```

The key becomes the tool prefix, so these appear as `kafka-local_list_topics`
and `kafka-prod-read_list_topics`. Name entries after both the cluster and the
endpoint policy: the prefix is the clearest signal of what a call can hit and
change. Use `server_config` to confirm rather than trusting the client-side
name.

Each enabled cluster costs context: these tools are roughly 8k tokens of
definitions. Enable only what you need, and put production behind an agent:

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

## Permissions

Three layers, and only one of them is real security:

| Layer | Protects against | Real security? |
| ----- | ---------------- | -------------- |
| `confirm: true` on writes | An LLM changing things on one ambiguous request | No — a guardrail |
| endpoint `read_only: true` | Accidental writes with the server's Kafka identity | No — anyone who can edit the config can turn it off |
| **Kafka ACLs on the SASL principal** | **An unauthorised person** | **Yes — the broker decides** |

### What a read-only endpoint exposes

`read_only: true` does more than refuse a write: the endpoint does not list the
tools whose only purpose is to write. `add_partitions`, `commit_offset` and
`create_topic` are absent from `tools/list` on a read-only endpoint, so a client
never sees a tool it could not have used, and their preview cannot describe a
change this endpoint would never apply.

A writable endpoint can withhold individual tools too, with its `tools` map.
That is the same mechanism seen from the client: the tool is
not registered, so it is absent from `tools/list` and from `server_config`.

`copy_message` stays, because `read_only` protects the cluster being written
to and the destination is chosen per call. Copying a message out of a
read-only production cluster is exactly what it is for.

Hiding a tool decides what is advertised, not what is permitted: both tools
still refuse at the point of mutation, so a registration mistake cannot turn
into a write. `server_config` reports the tools the endpoint actually exposes,
which is how a session can tell the two cases apart.

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

Each points `CONFIG_FILE` at their own file, differing only in `security.sasl[0].scram.user`
and the password. When ali calls `add_partitions`, the broker refuses:

```
not authorized to add partitions to "orders": the broker refused this request.
Adding partitions requires ALTER permission on the topic for the principal
this server connects as
```

ali cannot bypass that by editing config or rebuilding the binary, because the
decision is made by Kafka rather than by this server. On a cluster without
ACLs, `read_only: true` is the available protection.

## Tools

### Batch operations

`describe_topic`, `sample_messages`, `get_message`, `consumer_lag`,
`add_partitions`, `create_topic`, `commit_offset` and `copy_message` accept an
optional `items` array as an alternative to their single-operation fields. The
message-heavy tools accept at most 20 items; lag and administrative tools
accept at most 100.

Batch results stay in input order. Each entry has `index` and either `result`
or `error`, followed by `succeeded`, `failed` and `atomic: false`. An item error
does not hide successful items. Batch writes first preview every item, then
apply the valid items only when the top-level `confirm` is true. They are not
transactions: Kafka cannot roll back a topic, partition, offset or produced
message after a later item fails. Duplicate write targets are refused before
anything changes.

Do not combine `items` with the tool's single-operation fields. Existing single
calls keep their original input and output shape.

### `list_clusters`

Lists the clusters this server serves, with whether each is reachable and
whether it accepts writes. Takes no parameters. Available from every endpoint,
so a session can discover what `copy_message` may target.

```json
{"clusters": [
  {"name": "prod", "connected": true, "read_only": true},
  {"name": "preprod", "connected": true, "read_only": false}
], "count": 2}
```

`connected` is checked when you call, not recorded at startup, so a cluster
that has since gone down is reported honestly. Only the name, reachability and
writability are reported: broker addresses and credentials are deliberately
not, because this tool is reachable from every endpoint.

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
| `items`   | object[] | no     | Up to 20 topic objects; alternative to `topic` |

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
| `items`           | object[] | no     | Up to 20 topic sample requests           |

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

Scans a bounded range of a topic, filtering messages with a JavaScript
expression. Kafka has no server-side search, so this reads messages and filters
them client-side; the result reports what was covered.

| Parameter | Type | Required | Meaning |
| --------- | ---- | -------- | ------- |
| `topic` | string | yes | Topic to search |
| `script` | string | no | JavaScript filter. Omit to match every message |
| `parallelism` | int | no | Concurrent readers, 1–16. Default 1 |
| `partitions` | int[] | no | Restrict to these partitions |
| `from_offset` / `to_offset` | int | no | Offset window, end exclusive |
| `from_timestamp` / `to_timestamp` | string | no | RFC3339 time window |
| `direction` | string | no | `newest_first` (default) or `oldest_first` |
| `max_matches` | int | no | Stop after this many matches. Default 10 |
| `max_messages_scanned` | int | no | Read at most this many. Default 10000 |
| `max_value_bytes` | int | no | Value bytes per match. Default 512 |
| `timeout_seconds` | int | no | Wall-clock limit. Default 30 |
| `count_only` | bool | no | Return counts only, no message bodies |
| `output_file` | string | no | Write every match to this file as JSONL |

#### The script

Return true to keep a message. In scope:

| Name | Value |
| ---- | ----- |
| `value` | parsed JSON document, or the raw text when the message is not JSON |
| `key` | string, or `null` when absent |
| `headers` | object of header name to string |
| `partition`, `offset` | numbers |
| `timestamp` | a `Date` |

```js
return key === 'order-123'
return value.eventType === 'NEW' && value.payload.amount >= 500
return value.payload.cancelledAt === null      // present and null
return value.payload.cancelledAt === undefined // field absent
return headers['correlation-id'] === 'corr-999'
return /ORD-\d{4}/.test(value.payload.orderId)
return value.indexOf('ERROR') >= 0             // non-JSON topic: value is a string
```

Searching by key exactly is far more precise than searching the body: `123`
also appears inside `"amount": 1123`, and those false positives can fill
`max_matches` and hide the message wanted.

A script that throws on a message is counted in `script_errors` and the scan
continues, so a broken script is distinguishable from a genuine absence of
matches. Scripts run in a sandbox with no filesystem, network or host access,
and are stopped if they exceed the search timeout. They are **not** bounded by
memory: something like `'x'.repeat(1e12)` can exhaust the server process.
Scripts are trusted input; the blast radius is this server, not the cluster.

#### Parallelism

`parallelism` splits each partition's offset range between that many readers,
so a single-partition topic is parallelised too. Partitions are scanned one at
a time, so the number of connections stays at `parallelism` however many
partitions the topic has. A partition too small to divide is read by one
reader.

It pays off for `count_only`, `output_file` and full scans. A narrow
newest-first search is usually faster without it, because a sequential scan
stops after the newest chunk while parallel readers have already read the
older ranges.

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
| `items`           | object[] | no     | Up to 20 exact message addresses             |

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
| `items`              | object[] | no     | Up to 100 topic/group measurements                            |

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

Reports the effective configuration: endpoint name, exact path, description,
cluster, brokers, authentication mechanism and principal, TLS, read-only state,
export directory and the tools this endpoint exposes. Takes no parameters. The
password is never reported.

`tools` is the list for this endpoint, not for the deployment: a read-only
endpoint omits `add_partitions`, `commit_offset` and `create_topic`, because it
does not register them, and any endpoint omits whatever its `tools`
configuration switches off.

Use it when a result is surprising: an empty topic list means something very
different on a local broker than on production.

### `add_partitions`

Adds partitions to a topic. **Irreversible** — Kafka cannot reduce a partition
count. Not exposed on a read-only endpoint.

| Parameter | Type | Required | Meaning |
| --------- | ---- | -------- | ------- |
| `topic` | string | yes | Topic to change |
| `partitions` | int | yes | Final total, not the number to add. Repeating a call is safe |
| `confirm` | bool | no | Default false: preview only, nothing changes |
| `acknowledge_key_ordering` | bool | no | Required when messages are keyed |
| `sample_size` | int | no | Messages inspected for keys. Default 20 |
| `items` | object[] | no | Up to 100 topic targets; `confirm` stays top-level |

Without `confirm` it reports what would happen: current and target counts,
whether messages are keyed, which consumer groups will rebalance, and warnings.

Adding partitions changes which partition a key hashes to, so existing keys
lose their ordering guarantee. A keyed topic therefore requires
`acknowledge_key_ordering` as well. Requesting fewer partitions than the topic
has is refused with an explanation rather than attempted.

### `create_topic`

Creates a topic. Refuses a topic that already exists rather than adjusting it.
Not exposed on a read-only endpoint.

| Parameter | Type | Required | Meaning |
| --------- | ---- | -------- | ------- |
| `topic` | string | yes | Name of the topic to create |
| `partitions` | int | no | Omit for the broker default on Kafka 2.4+. Can grow later, never shrink |
| `replication_factor` | int | no | Omit for the broker default on Kafka 2.4+. Cannot exceed the broker count |
| `configs` | map | no | Topic-level config, such as `retention.ms` or `cleanup.policy` |
| `confirm` | bool | no | Default false: the broker validates the request and creates nothing |
| `items` | object[] | no | Up to 100 topic specifications; `confirm` stays top-level |

Without `confirm` the request is sent to the broker with `ValidateOnly`, so the
preview reports the cluster's own answer — an invalid name, an unknown config
key, a replication factor larger than the cluster — rather than a guess. With
`confirm` the topic is created and the resulting partition count and
replication factor are read back from the cluster, which is how an omitted
count is reported as the number the broker actually chose.

Using broker defaults requires Kafka 2.4 or newer, whose CreateTopics v4 API
introduced `-1` as "use the broker default". On an older broker, pass both
counts explicitly.

A replication factor larger than the number of brokers is refused here, with
the broker count in the message, because brokers differ on whether a
validate-only request catches it.

Use `add_partitions` to change an existing topic's partition count; this tool
never modifies a topic it did not create.

### `commit_offset`

Moves a consumer group's committed offset for one partition. Forward to skip
messages, backward to replay them. **Irreversible** in the sense that skipped
messages are never processed. Not exposed on a read-only endpoint.

| Parameter | Type | Required | Meaning |
| --------- | ---- | -------- | ------- |
| `topic`, `group`, `partition` | | yes | What to move |
| `offset` | int | yes | The offset the group reads next. To skip offset 42, commit 43 |
| `confirm` | bool | no | Default false: preview only, nothing changes |
| `allow_active_members` | bool | no | Proceed despite running consumers |
| `items` | object[] | no | Up to 100 offset moves; active-member acknowledgement is per item |

The group must have no active members. A running consumer keeps its position in
memory and only reads the committed offset when it joins, so a commit made
while it runs is overwritten by its next commit and the group does not move.
Stop the consumers first.

### `copy_message`

Copies one message to another topic, preserving key, value and headers. Takes
the message's address, never its content, so it can only duplicate a message
the cluster already holds.

| Parameter | Type | Required | Meaning |
| --------- | ---- | -------- | ------- |
| `source_topic`, `source_partition`, `source_offset` | | yes | Message to copy |
| `destination_topic` | string | yes | Where to write it. Must already exist |
| `destination_cluster` | string | no | Another cluster to write to. Defaults to this endpoint's own |
| `confirm` | bool | no | Default false: preview only, nothing is written |
| `items` | object[] | no | Up to 20 copies; destination and preview limit are per item |

Set `destination_cluster` to copy into another cluster this server serves,
which is how a production message is taken into a preproduction topic to be
debugged safely. Use `list_clusters` to see which names are valid.

Every copy carries provenance headers — `kafka-mcp-copied-from-cluster`,
`-from-topic`, `-from-partition`, `-from-offset`, `-copied-at`,
`-copied-by-tool`, `-copied-by-principal` — so a message in a dead letter or
preproduction topic can be traced back to its original. If the message already
carries one of those headers, the original is kept and the collision is
reported.

For a copy within the endpoint's own cluster, its `read_only` policy protects
the destination. A read-only endpoint can still be the source of a
cross-cluster copy, because copying out changes nothing there. A different
destination cluster is writable when it has at least one writable endpoint;
`list_clusters` reports that effective state. The tool is refused entirely
when the destination is read-only, preview included, because writing is all it
does.

## Skills

`skills/kafka-debugging/SKILL.md` is the one skill an agent loads. It routes to
the scenario guides under `skills/kafka-debugging/references/`, rather than
holding all five workflows itself, so a session reads only the one it needs:

- `find-message.md` — locating a message from something the user knows about it.
- `check-lag.md` — measuring lag and throughput, and judging when a backlog will
  clear.
- `scale-partitions.md` — deciding whether more partitions will help, and adding
  them safely.
- `skip-poison-message.md` — unblocking a consumer stuck on a message it cannot
  process, preserving the message first.
- `create-topic.md` — creating a topic with a partition count and retention
  chosen on purpose, including as a `copy_message` destination.

The umbrella also resolves the overlap between them: "the consumer is behind"
opens three of these guides, and `consumer_lag`'s `status` is what decides which
one is right.

## Development

| Command                | Purpose                                  |
| ---------------------- | ---------------------------------------- |
| `make up` / `make down`| Start / stop Redpanda and Console        |
| `make build`           | Build with goreleaser into `dist/`       |
| `make run`             | Run the server from source               |
| `go test ./...`        | All tests, including container tests     |
| `go test -short ./...` | Tests that need no containers            |
| `go vet ./...`         | Vet all packages                         |

Tests run against real containers started by `internal/domain/testenv` (a Redpanda
broker plus Console), so Docker must be available for the full suite.


### Start the server

Requires Go 1.27 or later. Configuration is loaded with `chu`; set `CONFIG_FILE`
to select a YAML or JSON file. `into` manages the process lifecycle, `ada` serves
HTTP with context-driven shutdown, and `logi` initializes structured logging.

```sh
make env-up                                  # local Redpanda + Console
make run                                 # serves kafka-mcp.local.yaml
```

`kafka-mcp.local.yaml` is committed and points at the compose broker, so a
clone works without writing any configuration. `make env-up` publishes the broker
on `localhost:19092`, the Schema Registry on `localhost:18081` and the Redpanda
Console on <http://localhost:8080>.

Check it is up:

```sh
curl http://localhost:8090/healthz
```
