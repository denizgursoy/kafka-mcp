# Tune topic configuration

Change a topic's own configuration — retention, cleanup policy, maximum message
size — knowing what the change does to the data already in it.

Use this when the user says "the disk is filling up, cut retention", "keep
orders for 30 days", "producers get RecordTooLargeException", "make this topic
compacted", or "reset this topic to the cluster defaults".

## Tools

| Tool                 | Use it for                                                    |
| -------------------- | ------------------------------------------------------------- |
| `server_config`      | Checking this endpoint may change configuration at all        |
| `describe_topic`     | Current values, which are deliberate and which inherited      |
| `consumer_lag`       | Whether consumers are behind, before shortening retention     |
| `alter_topic_config` | Previewing and applying                                       |

## Steps

### 0. Check the endpoint may change configuration

Call `server_config` **first** and look for `alter_topic_config` in its `tools`
list. A read-only endpoint does not expose it. Say the configuration can be
inspected but not changed here.

### 1. Read what the topic has now

Call `describe_topic`. For each key the user wants to change, note the value
and its `source`:

- **`DYNAMIC_TOPIC_CONFIG`** — someone set it on this topic deliberately. Ask
  whether they know why before overriding it.
- **anything else** — inherited from the broker or cluster default. Setting it
  makes this topic an exception to the cluster.

### 2. Translate the request into exact values

Kafka takes exact strings, in exact units. Work them out and show them:

| Request | Key | Value |
| ------- | --- | ----- |
| "keep 7 days" | `retention.ms` | `604800000` |
| "keep forever" | `retention.ms` | `-1` |
| "no more than 50 GB per partition" | `retention.bytes` | `53687091200` |
| "allow 5 MB messages" | `max.message.bytes` | `5242880` |
| "keep the latest per key" | `cleanup.policy` | `compact` |
| "back to the default" | — | `delete: ["retention.ms"]` |

`retention.bytes` is **per partition**, not per topic. A topic with 12
partitions and `retention.bytes` of 10 GB can hold 120 GB.

Raising `max.message.bytes` on the topic is rarely enough on its own: the
producer's `max.request.size` and the consumer's `max.partition.fetch.bytes`
must allow it too. Say so.

### 3. Before shortening retention, check the consumers

A shorter `retention.ms` makes every older message eligible for deletion as
soon as it applies — **including messages a lagging consumer has not read yet**.
Call `consumer_lag` on the topic. If any group's backlog is older than the new
retention, those messages are deleted before they are processed. That is data
loss for that consumer, and it must be raised before previewing.

### 4. Preview

Call `alter_topic_config` with **`confirm` omitted**. The broker validates the
change, so an unknown key or invalid value is refused here, before anything
changes. Each change shows `current`, `current_source` and `requested`.

Read the warnings out to the user:

- **`messages_past_retention`** — how many messages are already older than the
  new retention. They will be deleted on the broker's next cleanup pass, and
  Kafka cannot bring them back.
- **`cleanup.policy`** — switching to `compact` keeps only the latest message
  per key and drops messages without a key; switching to `delete` removes
  messages by age whatever their key. Consumers that rely on either behaviour
  break.
- **`retention.bytes`** — may delete the oldest segments of partitions already
  larger than the limit.

### 5. Get consent, then apply

Ask for consent naming the consequence, not the key: "18,000 messages older
than a day will be deleted", not "set retention.ms to 86400000". Then call with
`confirm: true`. Each change reports `resulting` and `resulting_source`, re-read
from the broker.

## Notes

- Changes are incremental. Keys not named keep their value; nothing else on the
  topic is reset.
- `delete` removes an override so the topic inherits the default again. The
  value it falls back to is only known after applying, and is reported in
  `resulting`.
- Retention is enforced asynchronously, per closed segment. Disk usage drops on
  the broker's next cleanup pass, not at the moment of the change, and the
  active segment is never deleted however old its messages are.
- Several topics are one call with several items. One `confirm` covers them, and
  the batch is not atomic.
- Config drift between two clusters is found with `compare_clusters`; see
  [compare-clusters.md](compare-clusters.md). Partition count is not a config
  and is changed with `add_partitions`.
- An authorization error means the principal needs `ALTER_CONFIGS` on the topic;
  see [authorization-error.md](authorization-error.md).
