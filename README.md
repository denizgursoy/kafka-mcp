# kafka-mcp

[![License](https://img.shields.io/github/license/denizgursoy/kafka-mcp?color=blue&style=flat-square)](https://raw.githubusercontent.com/denizgursoy/kafka-mcp/main/LICENSE)
[![Coverage](https://img.shields.io/sonar/coverage/denizgursoy_kafka-mcp?logo=sonarcloud&server=https%3A%2F%2Fsonarcloud.io&style=flat-square)](https://sonarcloud.io/summary/overall?id=denizgursoy_kafka-mcp)
[![Web](https://img.shields.io/badge/web-document-blueviolet?style=flat-square)](https://denizgursoy.github.io/kafka-mcp/)
[![Release](https://img.shields.io/github/v/release/denizgursoy/kafka-mcp?style=flat-square)](https://github.com/denizgursoy/kafka-mcp/releases/latest)
[![Kafka MCP server – quality and maintenance score on Glama](https://glama.ai/mcp/servers/denizgursoy/kafka-mcp/badges/score.svg)](https://glama.ai/mcp/servers/denizgursoy/kafka-mcp)

An MCP server that exposes Kafka debugging as tools an LLM can call. It speaks
MCP over stdio by default, optionally serves streamable HTTP, and talks to Kafka
with [franz-go](https://github.com/twmb/franz-go).

One process can connect to several Kafka clusters. In stdio mode, one endpoint
is selected for the session. In HTTP mode, each endpoint has its own path, so a
session is bound to one cluster by how it connects rather than by a parameter a
caller could forget to send.

## Install

Every release ships the same server four ways:

- **Binary**: `kafka-mcp_<Os>_<arch>` archives on the
  [releases page](https://github.com/denizgursoy/kafka-mcp/releases/latest).
  Put `kafka-mcp` on your `PATH`.
- **MCP Bundle**: `kafka-mcp_<version>_<os>_<arch>.mcpb` on the same page,
  for macOS on Apple Silicon, Linux (amd64, arm64) and Windows (amd64). Open
  it in a client that installs bundles, such as Claude Desktop. It asks for
  your configuration file and, when that file has several endpoints, which
  one to serve. Intel Macs use the binary or the image.
- **Container image**: `ghcr.io/denizgursoy/kafka-mcp:<tag>`. It starts with
  `--server`; for stdio, mount the file and pass `--server=false`:

  ```sh
  docker run -i --rm \
    --mount type=bind,src=$PWD/kafka-mcp.yaml,dst=/config/kafka-mcp.yaml,readonly \
    -e CONFIG_FILE=/config/kafka-mcp.yaml \
    ghcr.io/denizgursoy/kafka-mcp:latest --server=false --endpoint local
  ```

- **MCP Registry**: listed as `io.github.denizgursoy/kafka-mcp` in the
  [official registry](https://registry.modelcontextprotocol.io/v0.1/servers?search=io.github.denizgursoy/kafka-mcp),
  with the image and the bundles, so a client that reads the registry can
  install it from there.

How releases reach the registry and the other catalogs is in
[PUBLISHING.md](PUBLISHING.md).

## Configuration

`kafka-mcp.{toml,yaml,yml,json}` in the working directory, `~/.config/kafka-mcp/` or `/etc` configures the server. The `http` block is used only with `--server`.

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
owns the policy and, in HTTP mode, the MCP route. Stdio selects an endpoint by
name and does not use its path. Several endpoints may reference one cluster, so
the example reuses one production connection at `/kafka-mcp/mcp` in read-only
mode and `/kafka-mcp/mcp/rw` in writable mode. Paths are exact: `/mcp` does not
capture `/mcp/rw`. `description` is optional and is reported by `server_config`
so a caller knows what the endpoint is intended for.

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

Cross-cluster writes are granted per endpoint. `copy_message` and
`produce_message` may name a `destination_cluster`, and that cluster must be
listed in the calling endpoint's `destinations`:

```yaml
endpoints:
  prod-read:
    cluster: prod
    read_only: true
    destinations: [preprod]   # this session may copy into preprod, nowhere else
```

An endpoint with no `destinations` writes to no other cluster. A listed cluster
configured `read_only` still refuses, and listing the endpoint's own cluster is
an error, because that is governed by `read_only`. Names are checked at
startup. A file with no `endpoints` block keeps its old behaviour: each
cluster's endpoint lists every other cluster. Clusters that no endpoint serves
are invisible to `list_clusters`, `compare_clusters` and `destination_cluster`.

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
    schema_registry:
      urls: ["http://localhost:18081"]
endpoints:
  local:
    cluster: local
    path: /mcp/local
```

Run a custom configuration over stdio with
`CONFIG_FILE=/path/to/config.yaml go run ./cmd/server`. If it defines several
endpoints, select one with `--endpoint <name>`. Add `--server` to serve every
configured endpoint over HTTP instead; `--endpoint` is not used in HTTP mode.
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
`output_file` takes a new file name, never a path; an existing name is refused
rather than overwritten.

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
static token. Configure exactly one mode: static `token`, client credentials, or
`gcp`. Client-credentials tokens are fetched
on authentication, cached per cluster across admin and reading connections,
and renewed near expiry using `expires_in`. Token requests use the current
authentication context and a timeout (default 10 seconds). Static tokens are
not renewed. The broker must support OAUTHBEARER and trust the identity provider.
Token endpoint error bodies are never reported, only their HTTP status, because
they can echo secrets.

`oauth.proxy` sends token requests through an `http`, `https`, `socks5` or
`socks5h` proxy; when unset, `HTTPS_PROXY`, `HTTP_PROXY` and `NO_PROXY` apply.

Broker `security.tls` settings apply to Kafka connections only. The token
endpoint uses `oauth.tls`, which can also present a client certificate (RFC
8705). With a certificate, `client_secret` is optional: when empty, `client_id`
goes in the request body and the certificate authenticates the client.

```yaml
    - oauth:
        enabled: true
        token_url: https://identity.example.com/token
        client_id: kafka-mcp
        scopes: [kafka]
        tls:
          cert_file: /etc/certs/oauth/client-cert.pem
          key_file: /etc/certs/oauth/client-key.pem
          ca_file: /etc/certs/oauth/ca.pem # optional; system roots otherwise
```

`oauth.gcp` gets Google Cloud access tokens with an X.509 client certificate
(workload identity federation over mTLS), optionally impersonating a service
account. Use Google's `external_account` credential file; its
`credential_source` is ignored, because the certificate below is used instead.

```yaml
    - oauth:
        enabled: true
        gcp:
          enabled: true
          credentials_file: /etc/certs/gcp/wif-credentials.json
          cert_file: /etc/certs/kafka/client-cert.pem
          key_file: /etc/certs/kafka/client-key.pem
          format: managed_kafka # Google Managed Service for Apache Kafka; default raw
          # proxy: http://proxy:3128 # default: oauth.proxy, then the environment
          # lifetime: 1h             # impersonated token lifetime
          # refresh_before: 5m       # refresh this long before expiry
          # scopes: [https://www.googleapis.com/auth/cloud-platform]
```

`audience`, `token_url` and `service_account_impersonation_url` may be set
directly and override the credential file. `managed_kafka` requires a service
account to impersonate. A failed refresh keeps using the cached token until it
actually expires.

In `oauth.tls` and `oauth.gcp`, `cert`, `key`, `ca` (and `credentials` for
`gcp`) may be given inline instead of as `_file`, not both; inline keys and
credentials accept `{env:VAR}`. Files are re-read on each token refresh, so a
rotated certificate applies without a restart.

`insecure_skip_verify: true` disables server certificate and hostname
verification. It is accepted under `security.tls` (brokers),
`schema_registry.tls`, `oauth.tls` (token endpoint) and `oauth.gcp` (Google STS
and impersonation), and defaults to `false`. It exposes the connection to
man-in-the-middle attacks: prefer `ca_file`, and do not use it in production.

A cluster that cannot be reached at startup is still served, and
`list_clusters` reports it as disconnected. One cluster being down must not
block debugging the others.

### Message formats

Messages are decoded to JSON for every tool that shows or searches them, by the
first rule that applies:

1. The topic has a format in `topic_formats`.
2. The bytes carry a Schema Registry header (Confluent wire format) and the
   cluster has a `schema_registry`: Avro, Protobuf (with message index and
   referenced schemas) or JSON Schema.
3. Otherwise JSON, then UTF-8 text, then base64.

```yaml
clusters:
  prod:
    brokers: kafka-1:9093
    schema_registry:
      urls: ["https://schema-registry:8081"]
      user: kafka-mcp                     # or bearer_token: "{env:SR_TOKEN}"
      password: "{env:SR_PASSWORD}"       # or password_file
      # tls: {enabled: true, ca_file: /etc/sr/ca.pem}
    topic_formats:                        # topics whose messages carry no schema id
      "orders.*":                         # exact name or glob; longest match wins
        value:
          format: protobuf
          proto_files: [shop/order.proto]
          import_paths: [./protos]        # or descriptor_set: ./orders.binpb
          message_type: shop.Order
      events:
        value: {format: avro, schema_file: ./schemas/event.avsc}
      metrics:
        key: {format: text}
        value: {format: msgpack}
```

Formats are `avro`, `protobuf`, `json`, `msgpack`, `text` and `binary`. Schema
files and `.proto` sources are loaded at startup, so a missing file or type stops
the server. A message that names a schema but cannot be decoded is returned as
base64 with `decode_error` saying why, never silently. JSON output has sorted
keys; Avro longs beyond 2^53 keep their exact value; unset Protobuf fields are
shown with their defaults.

Each rendered message reports `format`, `schema_id` and `message_type` for the
value, and `key_format` / `key_schema_id` when the key is not plain text.

`server_config` reports the endpoint name, exact path, description, policy and
the cluster it serves, and never the password. Its `authentication` and
`sasl_user` describe the first configured option; `sasl_options` lists all
configured identities in preference order, not the mechanism negotiated by an
individual broker connection. It also reports `schema_registry` URLs and
`topic_formats`.

### Authentication and browser clients

`http.auth_token` makes every MCP request carry `Authorization: Bearer <token>`;
requests without it get 401. It accepts `{env:VAR}`. `/healthz` and CORS
preflights need no token. Without it, the endpoints have no authentication of
their own, which is safe only where nothing untrusted can reach the listener.

Set it with an environment variable, which keeps the secret out of the
config file entirely:

```sh
export KAFKA_MCP_HTTP_AUTH_TOKEN="$(openssl rand -hex 32)"
CONFIG_FILE=kafka-mcp.yaml kafka-mcp --server
```

Every config key can be overridden this way: `KAFKA_MCP_` followed by the key
path in upper case with `_` between levels, so `http.auth_token` is
`KAFKA_MCP_HTTP_AUTH_TOKEN`. To keep the token in the file but the value in the
environment under a name of your choice, reference it instead:

```yaml
http:
  auth_token: "{env:MY_MCP_TOKEN}"   # the server refuses to start if MY_MCP_TOKEN is unset
```

Clients then send the token on every request:

```sh
curl -X POST http://localhost:8090/mcp/local \
  -H "Authorization: Bearer $KAFKA_MCP_HTTP_AUTH_TOKEN" \
  -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"t","version":"1"}}}'
```

In the client configuration used under [Connecting a client](#connecting-a-client),
add the header to each remote entry. `{env:...}` is the client's own
substitution, so the token stays out of that file too:

```jsonc
{
  "mcp": {
    "kafka-local": {
      "type": "remote",
      "url": "http://localhost:8090/mcp/local",
      "headers": { "Authorization": "Bearer {env:KAFKA_MCP_HTTP_AUTH_TOKEN}" }
    }
  }
}
```

In a container, pass it like any other secret:

```sh
docker run -e KAFKA_MCP_HTTP_AUTH_TOKEN="$TOKEN" \
  -e KAFKA_MCP_HTTP_CORS_ALLOW_ORIGINS="https://mcp-client.example" \
  -v "$PWD/kafka-mcp.yaml:/etc/kafka-mcp.yaml" -e CONFIG_FILE=/etc/kafka-mcp.yaml \
  -p 8090:8090 kafka-mcp
```

A command-line client sends no `Origin` header and is unaffected by CORS. A
request that does carry an `Origin` is refused with 403 unless that origin is
in `allow_origins`. This happens before any tool runs, because CORS on its own
only hides the response from the page and the request itself would still
execute. **No origin is allowed by default**, so no web page the user visits
can drive the tools. To use a browser client, name its origin:

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

The same list from the environment is comma-separated:
`KAFKA_MCP_HTTP_CORS_ALLOW_ORIGINS="https://a.example,https://b.example"`.
Setting it to `*` restores the old allow-everything behaviour.

`http.cors` is ada's CORS middleware configuration, read straight from the
file, so every option that middleware has is available here. Each key is
optional and keeps its own default, so setting `allow_origins` alone does not
drop the rest. The defaults are the values above with no `allow_origins`.
`allow_origins: ["*"]` restores the old allow-everything behaviour.

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

An allowed origin can drive every tool with the server's Kafka credentials, so
list only pages that should have that power, set `auth_token` (a browser client
then has to send it), and set `read_only: true` on endpoints that should not
write. A less obvious path such as `/mcp/rw` is not authentication.

## Connecting a client

For the default stdio transport, configure the client to launch the binary. A
configuration with one endpoint needs no arguments:

```jsonc
{
  "mcp": {
    "kafka-local": {
      "type": "local",
      "command": ["kafka-mcp"],
      "environment": {"CONFIG_FILE": "/path/to/kafka-mcp.yaml"}
    }
  }
}
```

When the file contains several endpoints, add the endpoint selection to the
command, for example `"command": ["kafka-mcp", "--endpoint", "prod-read"]`.
The process refuses to start without it so it cannot silently connect a session
to the wrong cluster or permission policy.

To use HTTP, start `kafka-mcp --server` and register one remote client entry per
endpoint:

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

When the server sets `http.auth_token`, add
`"headers": {"Authorization": "Bearer {env:KAFKA_MCP_HTTP_AUTH_TOKEN}"}` to each
entry; see [Authentication and browser clients](#authentication-and-browser-clients).

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
tools whose only purpose is to write. `add_partitions`, `alter_topic_config`,
`commit_offset`, `create_topic`, `delete_consumer_group`, `delete_records` and
`delete_topic` are absent from `tools/list` on a read-only endpoint, so a client never sees a tool it could not have used, and their
preview cannot describe a change this endpoint would never apply.

A writable endpoint can withhold individual tools too, with its `tools` map.
That is the same mechanism seen from the client: the tool is
not registered, so it is absent from `tools/list` and from `server_config`.

`copy_message` and `produce_message` stay, because `read_only` protects the
cluster being written to and the destination is chosen per call. Copying a
message out of a read-only production cluster, or seeding a writable preprod
cluster from a protected session, is exactly what they are for. The destination
must be in the endpoint's `destinations`, and both refuse outright when the
destination is the read-only cluster itself.

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

### Audit logging

Every tool call is logged. One `slog` record per call, on the server's own log
stream, so nothing extra has to be configured:

```json
{"level":"INFO","msg":"tool call","tool":"produce_message","outcome":"ok",
 "duration_ms":12,"endpoint":"prod-write","cluster":"prod","read_only":false,
 "principal":"kafka-mcp-rw","session":"QY7MZM...","client":"claude-code",
 "client_version":"1.0.0","confirm":true,"item_count":1,"targets":"orders"}
```

The tools that change a cluster — `add_partitions`, `alter_topic_config`,
`commit_offset`, `create_topic`, `copy_message`, `delete_consumer_group`,
`delete_records`, `delete_topic`, `produce_message` — are logged at `INFO`.
Everything else is logged at `DEBUG`, because reads are constant and change
nothing, so recording them at the same level would bury the writes among them.
Raise the log level to see them.

Two fields carry most of the weight. `confirm` separates a real write from a
preview, and `targets` names the topic, group, partition and offset each item
pointed at, so a record says which topic was touched rather than only that some
topic was.

**Message content is never logged.** `produce_message` and `copy_message` carry
arbitrary payloads, and an audit log is usually readable by more people than the
data it describes, so keys, values and headers are left out. The audit code has
no field to unmarshal them into, so content cannot reach a log even by mistake.

**None of the recorded identities is a person, and the server cannot make one
up.** What each actually means:

| Field | What it is |
| ----- | ---------- |
| `principal` | The Kafka credential this server connects as, which is what ACLs are enforced against. Everyone reaching the same endpoint shares it |
| `endpoint` | Which endpoint policy allowed the call, and so which cluster was touched |
| `session` | One MCP session, which groups a sequence of calls into one investigation |
| `client`, `client_version` | The program that connected, as it identified itself at `initialize`. Self-reported and not verified |
| `user` | Present only when an inbound bearer token established it. This server installs no token verifier, so it is absent today |
| `request_id` | The HTTP request id, which joins a record to the access log |

For attribution to a person, give each person their own endpoint and SASL
credentials, as above: the `principal` in the record is then the answer to who
acted. A shared credential cannot be made to answer it.

## Tools

### Batch operations

`describe_topic`, `sample_messages`, `get_message`, `get_schema`,
`consumer_lag`, `describe_consumer_group`, `open_transactions`,
`add_partitions`, `alter_topic_config`, `create_topic`, `commit_offset`,
`delete_consumer_group`, `delete_records`, `delete_topic`, `copy_message` and
`produce_message` take their target **only** as a required `items` array. There
is no single-target form: one operation is an `items` array of length one.

```json
{"items": [{"topic": "orders"}]}
{"items": [{"topic": "orders"}, {"topic": "payments"}]}
```

Everything naming or shaping an operation lives on the item, so each field and
its description exist in exactly one place. What governs the whole call stays at
the top level, which in practice means `confirm`.

The message-heavy tools accept at most 20 items; lag, schema and administrative
tools accept at most 100.

Every response is the same envelope. Results stay in input order, each entry
carrying `index` and either `result` or `error`, followed by `succeeded`,
`failed`, `applied` and `atomic: false`. An item's failure is data in the
response rather than an error for the call, so it never hides the items that
worked. Batch writes preview every item first, then apply the valid ones only
when the top-level `confirm` is true. They are not transactions: Kafka cannot
roll back a topic, partition, offset or produced message after a later item
fails. Duplicate write targets are refused before anything changes, except in
`produce_message`, where two identical items mean two messages rather than a
mistake.

### `list_clusters`

Lists the clusters this server's endpoints serve, with whether each is
reachable, whether it is configured `read_only`, and whether this session may
write to it (`writable`: its own cluster unless the endpoint is read-only,
another only when listed in `destinations`). Takes no parameters. Available from every endpoint,
so a session can discover what `copy_message`, `produce_message` and
`compare_clusters` may target.

```json
{"clusters": [
  {"name": "prod", "connected": true, "read_only": true, "writable": false},
  {"name": "preprod", "connected": true, "read_only": false, "writable": true}
], "count": 2}
```

`connected` is checked when you call, not recorded at startup, so a cluster
that has since gone down is reported honestly. Only the name, reachability and
writability are reported: broker addresses and credentials are deliberately
not, because this tool is reachable from every endpoint.

### `compare_clusters`

Compares the topics of other clusters against the one this endpoint serves, and
reports what differs. Use it to find what preproduction has that production does
not, or to check whether two environments still match.

| Parameter | Type     | Required | Meaning                          |
| --------- | -------- | -------- | -------------------------------- |
| `items`   | object[] | yes      | 1 to 100 clusters to compare against |

Item fields:

| Field              | Type   | Required | Meaning                                                    |
| ------------------ | ------ | -------- | ---------------------------------------------------------- |
| `cluster`          | string | yes      | The other cluster. Use `list_clusters` for valid names     |
| `search`           | string | no       | Case-insensitive substring a topic name must contain       |
| `include_internal` | bool   | no       | Default false: internal topics are excluded                |

```json
{"here": "preprod", "there": "prod",
 "here_cluster": {"name": "preprod", "brokers": 1, "topics": 12},
 "there_cluster": {"name": "prod", "brokers": 3, "topics": 11},
 "only_here": [{"topic": "orders-v2", "partitions": 6, "replication_factor": 1,
                "configs": {"retention.ms": "604800000"}}],
 "only_there": [],
 "differing": [{"topic": "orders", "differences": ["partitions"],
                "here": {"partitions": 1}, "there": {"partitions": 12}}],
 "in_both": 11}
```

`only_here` and `only_there` name the direction, which is decided by the
endpoint you call: "here" is always the cluster this endpoint serves. Entries
carry the partition count, replication factor and explicitly-set configs of the
cluster that has the topic, so they can be passed straight to `create_topic`.

**This tool creates and changes nothing.** To create the missing topics, hand
the chosen entries to `create_topic`, which previews them against the broker
first and warns that a partition count can never be reduced.

`differing` is usually the more valuable half: a topic that exists on both sides
with a different partition count or `retention.ms` is the common reason a bug
reproduces in one environment and not the other. Only configs a topic sets for
itself are compared, because two clusters may carry different broker defaults
and comparing inherited values would report every topic as different.

A difference is not necessarily a mistake. A topic missing from production is
often deliberate, so the report says what differs, never what is correct.

Topic listings come from the Kafka client's metadata cache, which is a few
seconds old, so a topic created moments earlier may still appear in
`only_there`. Repeat the comparison rather than creating it twice.

### `list_topics`

Lists the topics on the cluster, sorted by name, each with its partition count,
replication factor, the configs it sets for itself, and its size on disk.

| Parameter         | Type   | Required | Meaning                                                  |
| ----------------- | ------ | -------- | -------------------------------------------------------- |
| `script`          | string | no       | JavaScript predicate deciding whether a topic is listed  |
| `timeout_seconds` | int    | no       | Limit for evaluating the script. Default 30, at most 3600 |

The predicate sees:

| Variable             | Type    | Meaning                                              |
| -------------------- | ------- | ---------------------------------------------------- |
| `topic`              | string  | The topic name                                       |
| `partitions`         | number  | Partition count                                      |
| `replication_factor` | number  | Replicas of the first partition                      |
| `internal`           | boolean | Kafka's own topics, such as `__consumer_offsets`     |
| `configs`            | object  | Values this topic sets for itself, e.g. `configs['retention.ms']` |
| `size_bytes`         | number  | One copy of the topic's log segments on disk; -1 when not reported |

```js
return topic.indexOf('orders') >= 0
return size_bytes > 10e9                       // the topics filling the disk
return partitions > 6
return configs['cleanup.policy'] === 'compact'
return replication_factor === 1 && !internal
```

```json
{"name": "list_topics", "arguments": {"script": "return partitions > 6"}}
```

```json
{"topics": [{"topic": "orders", "partitions": 12, "replication_factor": 3,
             "configs": {"retention.ms": "604800000"}, "size_bytes": 48318382080}],
 "count": 1}
```

Filtering is JavaScript only, as it is for `search_messages`: a name match is
`return topic.indexOf('orders') >= 0`. The predicate can also answer what a
substring never could — which topics have more than six partitions, only one
replica, or a compacted cleanup policy.

Only configs a topic sets for itself are reported. Inherited cluster defaults
are excluded, because including them would make every topic look configured.

A topic whose predicate throws, or is cut short by the timeout, is counted in
`script_errors` rather than listed, so a broken filter is never mistaken for an
empty cluster.

Sizes come from DescribeLogDirs, which needs `describe` on the cluster. When the
brokers do not answer it, every `size_bytes` is -1 and `warnings` says why.
Sizes are bytes on disk after compression.

### `describe_topic`

Reports a topic's partitions, offset ranges, message count, size on disk, time
span and full configuration. Use it before searching to see how much data a search would read
and how far back the topic can hold data at all.

| Parameter | Type     | Required | Meaning                      |
| --------- | -------- | -------- | ---------------------------- |
| `items`   | object[] | yes      | 1 to 20 topics to describe   |

Item fields:

| Field   | Type   | Required | Meaning           |
| ------- | ------ | -------- | ----------------- |
| `topic` | string | yes      | Topic to describe |

```json
{"results": [{"index": 0, "result": {
   "topic": "orders", "partition_count": 1, "message_count": 3,
   "size_bytes": 2048, "replicated_size_bytes": 6144,
   "partitions": [{"partition": 0, "start_offset": 0, "end_offset": 3, "message_count": 3, "size_bytes": 2048}],
   "configs": [{"key": "cleanup.policy", "value": "delete", "source": "DYNAMIC_TOPIC_CONFIG", "is_default": false},
               {"key": "retention.ms", "value": "604800000", "source": "DEFAULT_CONFIG", "is_default": true}]}}],
 "succeeded": 1, "failed": 0, "applied": 0, "atomic": false}
```

`configs` lists every topic config as the string Kafka reports, where `-1`
means unlimited. `is_default` is true when the value is inherited rather than
set on the topic. Two entries decide whether a message can still exist at all:
`retention.ms` (how long messages are kept) and `cleanup.policy` (`compact`
keeps only the latest message per key).

`size_bytes` is one copy of the topic, per partition and in total;
`replicated_size_bytes` counts every replica, which is what the brokers' disks
hold. Both are omitted, with a warning, when the brokers do not report log dirs.

### `sample_messages`

Reads a small sample of the newest messages and reports what they look like:
value formats, field paths with their types, key statistics, which value fields
carry the message key, and the schemas the values were written with. Avro,
Protobuf and JSON Schema values are decoded first, so their field paths are
reported like JSON. Use it before searching to decide how to search, and before
producing to find the schema to write against.

| Parameter | Type     | Required | Meaning                   |
| --------- | -------- | -------- | ------------------------- |
| `items`   | object[] | yes      | 1 to 20 topics to sample  |

Item fields:

| Field             | Type   | Required | Meaning                               |
| ----------------- | ------ | -------- | ------------------------------------- |
| `topic`           | string | yes      | Topic to sample                       |
| `sample_size`     | int    | no       | Messages to read in total. Default 20, at most 1000 |
| `partitions`      | int[]  | no       | Restrict to these partitions          |
| `max_value_bytes` | int    | no       | Value bytes per message. Default 512, at most 1 MiB |

```json
{"results": [{"index": 0, "result": {
   "value_formats": {"json": 20, "text": 0, "binary": 0},
   "json_fields": [{"path": "payload.amount", "types": ["number"], "present": 20, "example": "500"}],
   "key_stats": {"present": 20, "absent": 0, "unique": 20, "all_unique": true},
   "key_in_value": ["payload.orderId"],
   "schemas": [{"format": "avro", "schema_id": 7, "message_type": "shop.Order", "count": 20}],
   "value_bytes": {"min": 180, "p50": 412, "max": 9020},
   "sampled_ranges": [{"partition": 0, "start": 980, "end": 1000}]}}],
 "succeeded": 1, "failed": 0, "applied": 0, "atomic": false}
```

`key_in_value` naming a field means the key is that identifier, so searching
the key alone is the precise, cheap lookup. `value_formats` also counts `avro`,
`protobuf`, `json_schema`, `msgpack`, `null` and `undecodable` — values that
named a schema but could not be decoded, which is a configuration problem, not
binary data.

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
| `max_matches` | int | no | Stop after this many matches. Default 10, at most 1000 |
| `max_messages_scanned` | int | no | Read at most this many. Default 10000, at most 100000000 |
| `max_value_bytes` | int | no | Value bytes per match. Default 512, at most 1 MiB |
| `timeout_seconds` | int | no | Wall-clock limit. Default 30, at most 3600 |
| `count_only` | bool | no | Return counts only, no message bodies |
| `output_file` | string | no | Write every match to this new file as JSONL |
| `group_by` | string | no | Expression returning a bucket per match; counts per bucket |
| `max_groups` | int | no | Largest buckets returned with `group_by`. Default 20, at most 1000 |

#### The script

Return true to keep a message. In scope:

| Name | Value |
| ---- | ----- |
| `value` | parsed JSON document; the decoded record for Avro, Protobuf, JSON Schema or a configured format; the raw text otherwise |
| `key` | string, the decoded document when the key has a schema, or `null` when absent |
| `headers` | object of header name to string |
| `partition`, `offset` | numbers |
| `timestamp` | a `Date` |
| `value_bytes`, `key_bytes` | raw sizes in bytes, before decoding |
| `format` | `json`, `avro`, `protobuf`, `json_schema`, `msgpack`, `text`, `binary` or `null` |
| `schema_id` | the registry id the value names, or `null` |
| `decode_error` | why a value that named a schema could not be decoded, or `null` |

```js
return key === 'order-123'
return value.eventType === 'NEW' && value.payload.amount >= 500
return value.payload.cancelledAt === null      // present and null
return value.payload.cancelledAt === undefined // field absent
return headers['correlation-id'] === 'corr-999'
return /ORD-\d{4}/.test(value.payload.orderId)
return value.indexOf('ERROR') >= 0             // non-JSON topic: value is a string
return schema_id === 57                        // written with one schema
return value_bytes > 900000                    // close to max.message.bytes
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

#### Scan order

Every partition is read together, one chunk deep at a time: the newest chunk of
every partition, then the chunk behind it, and so on. A limited newest-first
search therefore returns the newest matches **in the topic**, not the newest in
whichever partition happened to be read first.

Kafka orders records within a partition and never across them, so matches are
merged and reported by **timestamp**, with partition and offset breaking ties.
Timestamps are set by the producer unless the topic uses `LogAppendTime`, so
they can be skewed; it is still the only thing comparable between partitions.

One scan reads every partition through a single connection, so a wide topic
costs no more connections than a narrow one. It does read more: a limited
search on a 12-partition topic examines the newest chunk of all twelve rather
than stopping inside the first. `max_messages_scanned` still bounds it, and may
become the `stopped_reason` on a wide topic sooner than on a narrow one.

#### Parallelism

`parallelism` reads a **single-partition** topic with that many concurrent
readers, which is what makes a full scan of one large partition fast. Readers
take chunks newest first (or oldest first), and a `max_matches` stop is decided
only on chunks completed in that order, so the matches returned are the same as
a sequential search would return. Because readers finish out of order,
`scanned_ranges` may list several runs for the partition. A
multi-partition topic is already read in parallel across its partitions, so the
setting does not apply there, and a range too small to divide is read by one
reader.

It pays off for `count_only`, `group_by`, `output_file` and full scans of a
single partition.

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

#### Grouping

`group_by` summarises instead of listing. It is an expression over the same
variables as `script` that returns a bucket name. Every match in the range is
counted per bucket, as with `count_only`, and no bodies are returned. `null` and
`undefined` form the bucket `null`; an object is grouped by its JSON.

```json
{"name": "search_messages", "arguments": {"topic": "orders-dlq",
  "group_by": "return headers['error-reason']", "from_timestamp": "2026-10-10T06:00:00Z"}}
```

```json
{"topic": "orders-dlq", "match_count": 1840, "matches": [],
 "groups": [{"key": "timeout", "count": 1702, "example": {"partition": 2, "offset": 90311}},
            {"key": "bad_schema", "count": 138, "example": {"partition": 0, "offset": 4410}}],
 "complete": true}
```

Buckets are sorted by count. `groups_truncated` is true when more than
`max_groups` existed. A message the expression throws on is counted in
`script_errors` and not bucketed. `group_by` cannot be combined with
`output_file`.

### `get_message`

Reads messages at exact offsets, plus optional neighbours.

| Parameter | Type     | Required | Meaning                             |
| --------- | -------- | -------- | ----------------------------------- |
| `items`   | object[] | yes      | 1 to 20 addresses to read           |

Item fields:

| Field             | Type   | Required | Meaning                                    |
| ----------------- | ------ | -------- | ------------------------------------------ |
| `topic`           | string | yes      | Topic to read from                         |
| `partition`       | int    | yes      | Partition to read from                     |
| `offset`          | int    | yes      | Exact offset to read                       |
| `context`         | int    | no       | Also return this many messages either side, at most 100 |
| `max_value_bytes` | int    | no       | Value bytes to return. Default 4096, at most 1 MiB |

```json
{"name": "get_message", "arguments": {"items": [
  {"topic": "orders", "partition": 0, "offset": 17, "context": 1}]}}
```

Schema-encoded values are decoded to JSON, with `format`, `schema_id` and
`message_type` saying what they were. Values that could not be decoded and are
not valid UTF-8 are base64 encoded, with `encoding` set to `base64`;
`decode_error` explains a value that named a schema but could not be decoded.

```json
{"partition": 0, "offset": 17, "key": "o-1",
 "value": "{\"amount\":42,\"id\":\"o-1\"}", "format": "avro",
 "schema_id": 7, "message_type": "shop.Order", "encoding": "utf8", "value_bytes": 12}
```

### `get_schema`

Reads schemas from the cluster's Schema Registry, by subject or by the
`schema_id` a message carries. Read the schema before producing to a
schema-encoded topic: it names every field and enum a value needs, which a
sampled message may not show.

| Parameter | Type     | Required | Meaning                       |
| --------- | -------- | -------- | ----------------------------- |
| `items`   | object[] | yes      | 1 to 100 schemas to look up   |

Item fields, giving `subject` or `id`:

| Field     | Type   | Required | Meaning                                         |
| --------- | ------ | -------- | ----------------------------------------------- |
| `subject` | string | either   | Subject, usually `<topic>-value` or `<topic>-key` |
| `version` | int    | no       | Subject version. Defaults to the latest         |
| `id`      | int    | either   | Schema id, e.g. from `get_message`              |
| `check_schema` | object | no  | `{"schema": "...", "type": "avro"}`: test a candidate against the subject, without registering it. Needs `subject` |

```json
{"results": [{"index": 0, "result": {
   "schema_id": 7, "subject": "orders-value", "version": 3, "versions": [1, 2, 3],
   "type": "avro", "schema": "{...}", "references": [],
   "message_types": null, "used_by": null, "compatibility": "BACKWARD",
   "check": {"compatible": false, "messages": ["{errorType:'TYPE_MISMATCH', …}"]}}}],
 "succeeded": 1, "failed": 0, "applied": 0, "atomic": false}
```

`message_types` lists Protobuf messages, which `produce_message` takes as
`message_type`. Looking up an `id` fills `used_by` with the subject versions
that use it. A subject lookup reports `compatibility`, the level in force,
inherited from the registry's global level when the subject sets none.
`check_schema` asks the registry whether the candidate could be registered as
the subject's next version under that level. `check.messages` gives the
registry's reasons when it could not. Nothing is registered. The call fails as
a whole when the cluster has no `schema_registry`.

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

### `describe_consumer_group`

Describes consumer groups in detail: who the members are, which partitions each
one owns, and where the group stands on every partition. Use it to turn "a
partition is stuck" into "this pod on this host is stuck".

| Parameter | Type     | Required | Meaning                   |
| --------- | -------- | -------- | ------------------------- |
| `items`   | object[] | yes      | 1 to 100 groups, each `{"group": "...", "sample_seconds": 30}` |

`sample_seconds` (optional, at most 60) watches the group for that long before
describing it, reading it every second, and adds `observation`:

```json
"observation": {"seconds": 30, "samples": 31,
  "states": ["Stable", "PreparingRebalance", "CompletingRebalance", "Stable"],
  "joined": [{"member_id": "payments-7-b…", "client_id": "payments-7", "host": "/10.0.4.17"}],
  "left":   [{"member_id": "payments-7-a…", "client_id": "payments-7", "host": "/10.0.4.17"}],
  "unstable": true}
```

A member that restarts rejoins with a new `member_id`, so it appears in both
`left` and `joined` with the same host. That is how a crash-looping consumer
behind a rebalance storm is found. The call blocks for the window.

```json
{"results": [{"index": 0, "result": {
   "group": "payments", "state": "Stable", "protocol_type": "consumer", "assignor": "cooperative-sticky",
   "coordinator": 1, "total_lag": 4200,
   "members": [{"member_id": "payments-7-…", "client_id": "payments-7", "host": "/10.0.4.17",
                "assignments": [{"topic": "orders", "partitions": [0, 1]}]}],
   "partitions": [{"topic": "orders", "partition": 0, "has_commit": true, "committed_offset": 812,
                   "end_offset": 5012, "lag": 4200, "member_id": "payments-7-…",
                   "client_id": "payments-7", "host": "/10.0.4.17"}]}}],
 "succeeded": 1, "failed": 0, "applied": 0, "atomic": false}
```

Partitions are the union of what members own and what the group has committed,
so an `Empty` group still shows its positions. `has_commit: false` means the
group owns a partition it has never committed on, so where it starts is decided
by the consumer's `auto.offset.reset`, not by an offset.

### `open_transactions`

Finds open transactions holding back `read_committed` consumers. A transactional
producer that hangs or dies mid-transaction leaves the partition's last stable
offset stuck, and every `read_committed` consumer stops there. In
`consumer_lag` that looks exactly like a poison message.

| Parameter | Type     | Required | Meaning                          |
| --------- | -------- | -------- | -------------------------------- |
| `items`   | object[] | yes      | 1 to 100 topics, each `{"topic": "..."}` |

```json
{"results": [{"index": 0, "result": {
   "topic": "orders", "blocked": true,
   "partitions": [{"partition": 0, "last_stable_offset": 812, "high_watermark": 5012,
                   "unreadable_messages": 4200,
                   "producers": [{"producer_id": 2004, "producer_epoch": 0, "transaction_start_offset": 812,
                                  "transactional_id": "payments-writer-1", "state": "Ongoing",
                                  "started_at": "2026-10-02T08:14:03Z", "open_for": "41m12s", "timeout_ms": 900000}]}]}}],
 "succeeded": 1, "failed": 0, "applied": 0, "atomic": false}
```

The fix is in the producer: restart or fence the application named by
`transactional_id`, or wait for `timeout_ms`, after which the broker aborts the
transaction. Moving the consumer's offset does not help.

### `consumer_lag`

Measures how far behind a topic's consumers are, how fast messages are produced
and consumed, and when the backlog will clear.

| Parameter | Type     | Required | Meaning                        |
| --------- | -------- | -------- | ------------------------------ |
| `items`   | object[] | yes      | 1 to 100 measurements to take  |

Item fields:

| Field               | Type   | Required | Meaning                                                   |
| ------------------- | ------ | -------- | --------------------------------------------------------- |
| `topic`             | string | yes      | Topic to measure                                          |
| `group`             | string | no       | Defaults to every group consuming the topic               |
| `sample_seconds`    | int    | no       | Consume-rate sample window. Default 5, at most 300. **The call blocks** |
| `skip_consume_rate` | bool   | no       | Return immediately, without a rate or estimate            |
| `measure_backlog_age` | bool | no       | Also report how old each backlog is, against retention. One fetch per lagging partition |

Up to 4 sampling windows run at a time, so the call takes about
`sample_seconds` for every 4 items.

```json
{"results": [{"index": 0, "result": {
   "topic": "orders", "total_lag": 4200,
   "produce_rate": {"last_minute": {"messages": 3000, "per_second": 50, "per_minute": 3000, "per_hour": 180000}},
   "groups": [{"group": "payments", "state": "Stable", "members": 2, "lag": 4200,
               "consume_rate": {"per_second": 120, "sampled_seconds": 5},
               "drain_per_second": 70, "eta_seconds": 60, "eta_human": "1m 0s",
               "status": "draining"}]}}],
 "succeeded": 1, "failed": 0, "applied": 0, "atomic": false}
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

Every partition also reports `start_offset`, and `offset_expired: true` when the
committed offset is below it. Retention has then deleted the group's position:
the consumer resets by its `auto.offset.reset` instead of resuming, and the
group carries a warning. `produce_rate.partitions` gives per-partition counts
for the last minute and hour, which separates key skew from a slow consumer.

With `measure_backlog_age`, each lagging partition reports
`committed_timestamp` (the next message to consume) and `lag_seconds`. Each
group reports `oldest_unconsumed_at`, and the topic its `retention_ms`.
`retention_risk` is set, with a warning, once the oldest unconsumed message has
used half of the retention: unless the group catches up, retention deletes it
before it is consumed.

### `cluster_health`

Checks the cluster this endpoint serves in one call: brokers, controller, and
every partition that is not fully healthy.

| Parameter          | Type   | Required | Meaning                                                         |
| ------------------ | ------ | -------- | --------------------------------------------------------------- |
| `search`           | string | no       | Only topics whose name contains this. Case-insensitive          |
| `include_internal` | bool   | no       | Also check `__consumer_offsets` and other internal topics        |

```json
{"cluster_id": "…", "controller": 1, "healthy": false,
 "brokers": [{"id": 1, "host": "kafka-1", "port": 9092, "rack": "eu-1a", "controller": true, "leaders": 61}],
 "summary": {"topics": 40, "partitions": 182, "offline": 0, "under_replicated": 3, "under_min_isr": 1, "errored": 0},
 "problems": [{"topic": "orders", "partition": 4, "issues": ["under_replicated", "under_min_isr"],
               "leader": 1, "replicas": [1, 2, 3], "isr": [1], "min_insync_replicas": 2}],
 "warnings": []}
```

`offline` means the partition has no leader, so nothing can be read or written.
`under_replicated` means a replica is out of sync. `under_min_isr` is the
condition behind `NOT_ENOUGH_REPLICAS`: producers using `acks=all` fail until
the in-sync replicas recover. A broker leading no partitions while others lead
many is usually one that restarted and was never given leadership back.

A problem partition being moved between brokers carries `reassigning: true` with
its `adding_replicas` and `removing_replicas`, and `summary.reassigning` counts
them. An added replica is out of sync until it has copied the log, so
`under_replicated` there is expected during the move rather than an outage.

Some Kafka-compatible brokers, Redpanda among them, do not report
`min.insync.replicas`. The topics affected are listed in `min_isr_unknown`, and
`under_min_isr` is not judged for them rather than guessed.

### `list_acls`

Lists access control entries, for when a client fails with
`TOPIC_AUTHORIZATION_FAILED` or `GROUP_AUTHORIZATION_FAILED`.

| Parameter       | Type   | Required | Meaning                                                              |
| --------------- | ------ | -------- | -------------------------------------------------------------------- |
| `principal`     | string | no       | Only this principal, e.g. `User:payments`                            |
| `resource_type` | string | no       | `topic`, `group`, `cluster`, `transactional_id`, `delegation_token`  |
| `resource_name` | string | no       | Every ACL applied to this name, including prefixed and `*`. Needs `resource_type` |

```json
{"acls": [{"principal": "User:payments", "host": "*", "resource_type": "topic", "resource_name": "orders",
           "pattern_type": "literal", "operation": "read", "permission": "allow"}], "count": 1}
```

A `deny` overrides every `allow`. A broker without an authorizer fails with
`SECURITY_DISABLED`, which means ACLs are not enforced at all.

### `server_config`

Reports the effective configuration: endpoint name, exact path, description,
cluster, brokers, authentication mechanism and principal, TLS, read-only state,
export directory and the tools this endpoint exposes. Takes no parameters. The
password is never reported.

`tools` is the list for this endpoint, not for the deployment: a read-only
endpoint omits `add_partitions`, `alter_topic_config`, `commit_offset`,
`create_topic`, `delete_consumer_group`, `delete_records` and `delete_topic`,
because it
does not register them, and any endpoint omits whatever its `tools`
configuration switches off.

Use it when a result is surprising: an empty topic list means something very
different on a local broker than on production.

### `add_partitions`

Adds partitions to a topic. **Irreversible** — Kafka cannot reduce a partition
count. Not exposed on a read-only endpoint.

| Parameter | Type     | Required | Meaning                                      |
| --------- | -------- | -------- | -------------------------------------------- |
| `items`   | object[] | yes      | 1 to 100 topics to change                    |
| `confirm` | bool     | no       | Default false: preview only, nothing changes |

Item fields:

| Field                      | Type   | Required | Meaning                                                      |
| -------------------------- | ------ | -------- | ------------------------------------------------------------ |
| `topic`                    | string | yes      | Topic to change                                              |
| `partitions`               | int    | yes      | Final total, not the number to add. Repeating a call is safe |
| `acknowledge_key_ordering` | bool   | no       | Required when messages are keyed                             |
| `sample_size`              | int    | no       | Messages inspected for keys. Default 20                      |

Without `confirm` it reports what would happen: current and target counts,
whether messages are keyed, which consumer groups will rebalance, and warnings.

Adding partitions changes which partition a key hashes to, so existing keys
lose their ordering guarantee. A keyed topic therefore requires
`acknowledge_key_ordering` as well. Requesting fewer partitions than the topic
has is refused with an explanation rather than attempted.

### `alter_topic_config`

Changes topic-level configuration: retention, cleanup policy, maximum message
size and anything else Kafka allows per topic. Changes are incremental, so every
key not named keeps its value. Not exposed on a read-only endpoint.

| Parameter | Type     | Required | Meaning                                       |
| --------- | -------- | -------- | --------------------------------------------- |
| `items`   | object[] | yes      | 1 to 100 topics to change                     |
| `confirm` | bool     | no       | Default false: preview only, nothing changes  |

Item fields:

| Field    | Type     | Required | Meaning                                                   |
| -------- | -------- | -------- | --------------------------------------------------------- |
| `topic`  | string   | yes      | Topic to change                                           |
| `set`    | object   | no       | Keys to set, e.g. `{"retention.ms": "86400000"}`          |
| `delete` | string[] | no       | Overrides to remove, so the cluster default applies again |

```json
{"results": [{"index": 0, "result": {
   "topic": "orders", "applied": false, "messages_past_retention": 18000,
   "changes": [{"key": "retention.ms", "action": "set", "current": "604800000",
                "current_source": "DYNAMIC_TOPIC_CONFIG", "requested": "86400000"}],
   "warnings": ["18000 message(s) are already older than the new retention of 24h0m0s and become eligible for deletion as soon as it applies; Kafka cannot bring them back"]}}],
 "succeeded": 1, "failed": 0, "applied": 0, "atomic": false}
```

The preview asks the broker to validate the change, so an unknown key or an
invalid value is refused before `confirm`. Shortening `retention.ms` reports how
many messages are already past the new limit; changing `cleanup.policy` or
setting `retention.bytes` is warned about. After applying, every key is re-read
from the broker, which is the only way the inherited value of a deleted override
is known.

### `create_topic`

Creates a topic. Refuses a topic that already exists rather than adjusting it.
Not exposed on a read-only endpoint.

| Parameter | Type     | Required | Meaning                                                             |
| --------- | -------- | -------- | ------------------------------------------------------------------- |
| `items`   | object[] | yes      | 1 to 100 topics to create                                           |
| `confirm` | bool     | no       | Default false: the broker validates the requests and creates nothing |

Item fields:

| Field                | Type   | Required | Meaning                                                                  |
| -------------------- | ------ | -------- | ------------------------------------------------------------------------ |
| `topic`              | string | yes      | Name of the topic to create                                              |
| `partitions`         | int    | no       | Omit for the broker default on Kafka 2.4+. Can grow later, never shrink   |
| `replication_factor` | int    | no       | Omit for the broker default on Kafka 2.4+. Cannot exceed the broker count |
| `configs`            | map    | no       | Topic-level config, such as `retention.ms` or `cleanup.policy`           |

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

### `delete_topic`

Deletes topics. **The most destructive tool here**: deleting a topic destroys
every message in it, Kafka has no undo, and any consumer group reading it
breaks. Not exposed on a read-only endpoint.

| Parameter | Type     | Required | Meaning                                       |
| --------- | -------- | -------- | --------------------------------------------- |
| `items`   | object[] | yes      | 1 to 100 topics to delete                     |
| `confirm` | bool     | no       | Default false: preview only, nothing is deleted |

Item fields:

| Field                   | Type   | Required | Meaning                                              |
| ----------------------- | ------ | -------- | ---------------------------------------------------- |
| `topic`                 | string | yes      | Topic to delete. It must exist                       |
| `acknowledge_data_loss` | bool   | no       | Required when the topic still holds messages         |

Without `confirm` it reports, per topic, how many messages would be destroyed,
how many partitions it had, and which consumer groups had committed offsets for
it:

```json
{"results": [{"index": 0, "result": {
   "topic": "orders-old", "partitions": 6, "message_count": 41207,
   "consumer_groups": ["payments"], "deleted": false, "would_delete": true,
   "warnings": ["41207 message(s) would be destroyed, and Kafka cannot restore them: the only recovery is a backup taken beforehand"]}}],
 "succeeded": 1, "failed": 0, "applied": 0, "atomic": false}
```

Two separate acknowledgements are required, because they answer different
questions. `confirm` says the caller meant to delete; `acknowledge_data_loss`
says they know what is inside. A topic holding messages is refused without both,
and the count is re-read at deletion time, so a topic that gained messages since
the preview is still caught.

Internal topics such as `__consumer_offsets` are refused outright at any level
of acknowledgement: they hold cluster state rather than a caller's data, and
deleting one breaks every consumer at once.

Duplicate topics in one batch are refused before anything is deleted. Deletion
is not atomic — topics removed before a later item failed stay removed.

### `delete_records`

Deletes the oldest messages of a partition without deleting the topic. The
topic, its configuration and its consumer groups stay. **Irreversible.** Not
exposed on a read-only endpoint.

| Parameter | Type     | Required | Meaning                                         |
| --------- | -------- | -------- | ----------------------------------------------- |
| `items`   | object[] | yes      | 1 to 100 partitions to truncate                 |
| `confirm` | bool     | no       | Default false: preview only, nothing is deleted |

Item fields:

| Field                   | Type   | Required | Meaning                                                         |
| ----------------------- | ------ | -------- | --------------------------------------------------------------- |
| `topic`                 | string | yes      | Topic to delete from                                            |
| `partition`             | int    | yes*     | Partition to delete from. Not used with `all_partitions`        |
| `before_offset`         | int    | one of   | Everything below this offset goes; this one becomes the first  |
| `before_timestamp`      | string | one of   | RFC3339: everything written before this moment goes, resolved per partition |
| `all_partitions`        | bool   | no       | With `before_timestamp`, cut every partition of the topic       |
| `acknowledge_data_loss` | bool   | no       | Required to apply                                               |

```json
{"results": [{"index": 0, "result": {
   "topic": "orders", "partition": 0, "start_offset": 0, "end_offset": 5012, "before_offset": 812,
   "messages_deleted": 812, "would_delete": true, "deleted": false,
   "affected_groups": [{"group": "replay-job", "committed_offset": 100, "unprocessed_lost": 712}]}}],
 "succeeded": 1, "failed": 0, "applied": 0, "atomic": false}
```

`affected_groups` lists every group committed below the cut, with how many
messages it would lose without ever processing them; such a group resumes from
the new start. Use the partition's end offset as `before_offset` to empty it.

`before_timestamp` resolves to the first offset written at or after the moment,
and the preview's `before_offset` shows what it resolved to. With
`all_partitions` one item covers the whole topic: `partitions` lists each
partition's own cut, start and end, and affected groups; the item carries `all_partitions: true` and `partition: -1`, and `messages_deleted`
is their total. Partitions holding nothing older are left alone, and if none
does, the item is refused. An offset means nothing across partitions, so
`all_partitions` takes only a timestamp. Cuts are applied partition by
partition and are not atomic.

### `delete_consumer_group`

Deletes consumer groups and their committed offsets. Use it to clean up groups
whose consumers were decommissioned: their commits keep reporting lag that
nobody will ever drain. Not exposed on a read-only endpoint.

| Parameter | Type     | Required | Meaning                                         |
| --------- | -------- | -------- | ----------------------------------------------- |
| `items`   | object[] | yes      | 1 to 100 groups, each `{"group": "..."}`        |
| `confirm` | bool     | no       | Default false: preview only, nothing is deleted |

The preview lists each group's committed offsets and lag. A group with active
members is refused, and re-checked at deletion time. A consumer that later
starts with the same group id begins from its `auto.offset.reset`, not from
where the group left off.

### `commit_offset`

Moves consumer groups' committed offsets. Forward to skip messages, backward to
replay them, or to a point in time to reprocess everything since. **Irreversible**
in the sense that skipped messages are never processed. Not exposed on a
read-only endpoint.

| Parameter | Type     | Required | Meaning                                      |
| --------- | -------- | -------- | -------------------------------------------- |
| `items`   | object[] | yes      | 1 to 100 moves to make                       |
| `confirm` | bool     | no       | Default false: preview only, nothing changes |

Item fields — give exactly one of `offset`, `timestamp` or `position`:

| Field                  | Type   | Required | Meaning                                                                    |
| ---------------------- | ------ | -------- | -------------------------------------------------------------------------- |
| `topic`                | string | yes      | Topic whose offset is moving                                               |
| `group`                | string | yes      | Consumer group to move                                                     |
| `partition`            | int    | no       | Required with `offset`. Omit with `timestamp` or `position` for every partition |
| `offset`               | int    | —        | The offset the group reads next. To skip offset 42, commit 43              |
| `timestamp`            | string | —        | RFC3339. Each partition moves to its first message at or after this time   |
| `position`             | string | —        | `earliest` or `latest`                                                     |
| `allow_active_members` | bool   | no       | Proceed despite running consumers                                          |

```json
{"items": [{"topic": "orders", "group": "payments", "timestamp": "2026-10-01T09:00:00Z"}]}
```

```json
{"results": [{"index": 0, "result": {
   "topic": "orders", "group": "payments", "state": "Empty", "members": 0,
   "partitions": [{"partition": 0, "current_offset": 5012, "target_offset": 4100, "replayed_messages": 912},
                  {"partition": 1, "current_offset": 4990, "target_offset": 4021, "replayed_messages": 969}],
   "replayed_messages": 1881, "applied": false}}],
 "succeeded": 1, "failed": 0, "applied": 0, "atomic": false}
```

A partition with no message at or after `timestamp` moves to its end, and its
entry carries a `note` saying so. A whole-topic item and a partition item for
the same group and topic in one batch are refused, because which one wins would
depend on order.

The group must have no active members. A running consumer keeps its position in
memory and only reads the committed offset when it joins, so a commit made
while it runs is overwritten by its next commit and the group does not move.
Stop the consumers first.

### `copy_message`

Copies messages to another topic, preserving key, value and headers. Takes each
message's address, never its content, so it can only duplicate a message the
cluster already holds.

| Parameter | Type     | Required | Meaning                                         |
| --------- | -------- | -------- | ----------------------------------------------- |
| `items`   | object[] | yes      | 1 to 20 copies to make                          |
| `confirm` | bool     | no       | Default false: preview only, nothing is written |

Item fields:

| Field                                               | Type   | Required | Meaning                                                      |
| --------------------------------------------------- | ------ | -------- | ------------------------------------------------------------ |
| `source_topic`, `source_partition`, `source_offset` |        | yes      | Message to copy                                              |
| `destination_topic`                                 | string | yes      | Where to write it. Must already exist                        |
| `destination_cluster`                               | string | no       | Another cluster to write to. Defaults to this endpoint's own  |
| `translate_schema`                                  | bool   | no       | Re-register a schema id in the destination's registry         |
| `max_value_bytes`                                   | int    | no       | Preview value limit. The whole value is always copied         |

Set `destination_cluster` to copy into another cluster this server serves,
which is how a production message is taken into a preproduction topic to be
debugged safely. Use `list_clusters` to see which names are valid.

Every copy carries provenance headers — `kafka-mcp-copied-from-cluster`,
`-from-topic`, `-from-partition`, `-from-offset`, `-copied-at`,
`-copied-by-tool`, `-copied-by-principal` — so a message in a dead letter or
preproduction topic can be traced back to its original. If the message already
carries one of those headers, the original is kept and the collision is
reported.

Key and value bytes are copied unchanged. A schema id is only meaningful in the
registry that issued it, so when the destination cluster uses a different
registry, or none, the response warns. With `translate_schema` the schema is
registered in the destination registry under `<destination_topic>-value` (or
`-key`), with its references, and the id in the copy is rewritten; the payload
is untouched. That registration is a write to the destination registry and
happens only with `confirm`.

For a copy within the endpoint's own cluster, its `read_only` policy protects
the destination. A read-only endpoint can still be the source of a
cross-cluster copy, because copying out changes nothing there. A different
destination cluster must be listed in the calling endpoint's `destinations` and
not configured `read_only`; `list_clusters` reports that as `writable`. The tool is refused entirely
when the destination is read-only, preview included, because writing is all it
does.

### `produce_message`

Writes new messages to existing topics. Unlike `copy_message`, the caller
supplies the content, so this can put a message into a topic that no producer
ever sent.

| Parameter | Type     | Required | Meaning                                         |
| --------- | -------- | -------- | ----------------------------------------------- |
| `items`   | object[] | yes      | 1 to 20 messages to write                       |
| `confirm` | bool     | no       | Default false: preview only, nothing is written |

Item fields:

| Field                 | Type   | Required | Meaning                                                     |
| --------------------- | ------ | -------- | ----------------------------------------------------------- |
| `topic`               | string | yes      | Existing topic to write to                                  |
| `value`               | string | yes      | The message body                                            |
| `key`                 | string | no       | Decides the partition when `partition` is omitted           |
| `headers`             | object | no       | Header name to value                                        |
| `partition`           | int    | no       | Exact partition. Omit to let the key decide                 |
| `encoding`            | string | no       | `utf8` (default) or `base64` for exact bytes you already have |
| `value_schema`        | object | no       | Encode `value` (JSON) to a registry schema                  |
| `key_schema`          | object | no       | Encode `key` (JSON) to a registry schema                    |
| `destination_cluster` | string | no       | Another cluster to write to. Defaults to this endpoint's own |
| `max_value_bytes`     | int    | no       | Preview value limit. The whole value is always written       |

Every message carries `kafka-mcp-produced-at`, `kafka-mcp-produced-by-tool`,
`kafka-mcp-produced-by-principal` and, when the client identifies itself,
`kafka-mcp-produced-by-client`, so a fabricated message stays distinguishable
from a genuine one. A header the caller supplies under one of those names is
kept as given and the collision is reported.

`value_schema` and `key_schema` take `{subject, version, id, message_type}`,
all optional: `{}` means the latest version of `<topic>-value` (or `-key`) in
the **destination** cluster's registry, `id` pins an exact schema, and
`message_type` picks a Protobuf message. The value is given as JSON, validated
against the schema in the preview — a missing or misspelt field is refused by
name — and written framed with the schema id, as registry-aware consumers
expect. A topic with a format in `topic_formats` is encoded to it without being
asked. Neither can be combined with `encoding: base64`. The response reports
the schema used in `value_encoding` / `key_encoding`.

The topic must already exist: a missing one is refused rather than left to
auto-creation. Omit `partition` unless the exact partition is the point — the
key decides placement, and naming a partition puts a keyed message where its
key does not hash to, which breaks ordering for that key. The response warns
whenever an explicit partition is used.

`read_only` protects the cluster being written to, so a read-only endpoint may
still produce into a different cluster listed in its `destinations`, and is
refused outright —
preview included — when writing to its own. A produced message cannot be
deleted; it stays until retention removes it.

## Skills

`skills/kafka-debugging/SKILL.md` is the one skill an agent loads. It routes to
the scenario guides under `skills/kafka-debugging/references/`, rather than
holding every workflow itself, so a session reads only the one it needs:

- `find-message.md` — locating a message from something the user knows about it.
- `check-lag.md` — measuring lag and throughput, and judging when a backlog will
  clear.
- `scale-partitions.md` — deciding whether more partitions will help, and adding
  them safely.
- `skip-poison-message.md` — unblocking a consumer stuck on a message it cannot
  process, preserving the message first.
- `create-topic.md` — creating a topic with a partition count and retention
  chosen on purpose, including as a `copy_message` destination.
- `produce-message.md` — writing a message: repairing and re-injecting one,
  reproducing a failure in another cluster, or seeding a topic.
- `compare-clusters.md` — finding what differs between two environments, and
  creating the topics one of them is missing.
- `delete-topic.md` — removing a topic and everything in it, after the user has
  seen what that destroys.
- `replay-messages.md` — reprocessing everything since a moment, after a bug fix
  ships.
- `tune-topic-config.md` — changing retention, cleanup policy or message size on
  a topic, knowing what the change does to data already there.
- `cluster-health.md` — finding offline and under-replicated partitions behind
  `NOT_ENOUGH_REPLICAS` and unreadable topics.
- `purge-messages.md` — deleting old messages from a partition while keeping the
  topic, or removing abandoned consumer groups.
- `authorization-error.md` — working out which ACL is refusing a client.
- `unstable-consumer-group.md` — finding why a group keeps rebalancing and which
  consumer keeps leaving.
- `partition-skew.md` — telling key skew from a slow consumer when one partition
  lags, and finding the hot keys.
- `schema-error.md` — testing a schema change before it ships, and tracing which
  schema broke producers or consumers.

The umbrella also resolves the overlap between them: "the consumer is behind"
opens three of these guides, and `consumer_lag`'s `status` is what decides which
one is right.

## Development

| Command                | Purpose                                  |
| ---------------------- | ---------------------------------------- |
| `make env-up` / `make env-down` | Start / stop Redpanda and Console |
| `make build`           | Build with goreleaser into `dist/`       |
| `make run`             | Run the HTTP server from source          |
| `go test ./...`        | All tests, including container tests     |
| `go test -short ./...` | Tests that need no containers            |
| `go vet ./...`         | Vet all packages                         |

The documentation site lives in `_docs` (Vite, pnpm). `pnpm install && pnpm dev`
there serves it locally; pushing changes under `_docs/` to `main` publishes it to
GitHub Pages through `.github/workflows/docs.yml`.

Tests run against real containers started by `internal/domain/testenv` (a Redpanda
broker plus Console), so Docker must be available for the full suite.


### Start the HTTP server

Requires Go 1.27 or later. Configuration is loaded with `chu`; set `CONFIG_FILE`
to select a YAML or JSON file. `into` manages the process lifecycle, `ada` serves
HTTP with context-driven shutdown, and `logi` initializes structured logging.

```sh
make env-up                                  # local Redpanda + Console
make run                                     # runs with --server and serves kafka-mcp.local.yaml
```

`kafka-mcp.local.yaml` is committed and points at the compose broker, so a
clone works without writing any configuration. `make env-up` publishes the broker
on `localhost:19092`, the Schema Registry on `localhost:18081` and the Redpanda
Console on <http://localhost:8080>.

Check it is up:

```sh
curl http://localhost:8090/healthz
```
