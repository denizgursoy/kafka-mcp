# Produce a message

Write a message the cluster has never held: a repaired version of one that
could not be processed, a variant that reproduces a bug somewhere safe, or the
first messages in an empty topic.

Use this when the user says "put this message back", "send a corrected
version", "reproduce this in preprod", or "seed the topic with test data".

This is the only tool that writes content, rather than moving content the
cluster already has. Everything below follows from that: a produced message is
indistinguishable to a consumer from a real one, and it will be processed.

## Tools

| Tool              | Use it for                                            |
| ----------------- | ----------------------------------------------------- |
| `server_config`   | Checking the endpoint may write at all                |
| `list_clusters`   | Which cluster names are valid, and which are writable |
| `list_topics`     | Confirming the destination exists and is the right one |
| `describe_topic`  | The partition count, and whether the topic is compacted |
| `get_message`     | Reading the original a repaired message is based on   |
| `search_messages` | Finding the original when its offset is not known     |
| `produce_message` | Previewing and writing                                |
| `create_topic`    | Creating the destination when it does not exist yet   |

## Steps

### 0. Check the endpoint may write

Call `server_config` **first**, and check `produce_message` is in its `tools`
list.

`produce_message` stays registered on a read-only endpoint, unlike
`create_topic` and `commit_offset`, because `read_only` protects the cluster
being **written to** and the destination is chosen per call. A read-only
session can legitimately write into a different, writable cluster — that is how
production data reaches preprod. What it cannot do is write to its own cluster,
and it refuses outright rather than offering a preview.

So the question is not only "is this endpoint writable" but "is the cluster I
am about to write to writable". Use `list_clusters` when a `destination_cluster`
is involved; it reports the effective writability of each.

### 1. Establish which of the three cases this is

They share a tool and almost nothing else. Decide before calling anything,
because the risk is different in each.

- **Repair and re-inject.** A consumer was stuck, the message was skipped, and
  a corrected version has to take its place. The highest-risk case: it writes
  to a production topic that real consumers read.
- **Reproduce in preprod.** A production message, or a variant of it, written
  into another cluster to make a failure happen where it is safe to watch.
  Reading production and writing elsewhere, so the risk is in picking the wrong
  destination.
- **Seed a topic.** Test data into a new or empty topic. Lowest risk, provided
  the topic really is a test topic.

If the user's request does not clearly match one, ask. "Produce a message to
orders" means something very different if `orders` is production.

### 2. Name the consumer that will read it

Before writing to any topic that is not demonstrably a test topic, establish
who consumes it. `list_consumer_groups` with the topic answers this.

A produced message is processed by whatever reads that topic. If the user has
not thought about which consumers those are, they have not finished deciding to
produce. Say what will consume it and wait for confirmation.

This is the step that separates seeding a scratch topic from injecting into a
live pipeline, and the tool cannot tell the difference.

### 3. Base a repair on the original, not on a description

For repair and reproduce, read the original first with `get_message` — or find
it with `search_messages` when the offset is unknown — and build the new value
by changing that payload. Do not retype it from what the user said it contained.

**Keep the original key.** The key decides the partition, so a repaired message
written with the same key lands on the same partition as the one it replaces
and stays ordered with the rest of that key. A repair that loses the key is
processed out of order relative to everything else about that entity.

Carry over the headers that mean something — a correlation id ties the repair
to the original investigation. `produce_message` adds its own provenance
headers alongside them.

### 4. Choose the encoding

`encoding` defaults to `utf8`, which is right for JSON and text.

Use `base64` when the payload is binary — protobuf, Avro, anything that is not
text. `get_message` reports `encoding: base64` for exactly these, and a value
that arrived base64 must go back as base64, or the consumer receives the
characters of the encoding rather than the bytes it expects.

### 5. Leave the partition alone unless it is the point

Omit `partition`. The key decides placement, which is what keeps a key's
messages ordered and on one partition.

Set it only when the exact partition is the thing being tested — reproducing a
failure that only happens on one partition, or putting a message where a
specific consumer instance will see it. `produce_message` warns whenever an
explicit partition is used, because a keyed message written to a chosen
partition is on a partition its key does not hash to, and every later consumer
of that key sees it out of order.

`describe_topic` gives the partition count; a partition the topic does not have
is refused before anything is written.

### 6. Preview

Call `produce_message` **without** `confirm`. Nothing is written. Each item's
result shows the exact message: key, value, headers including the provenance
ones, and the destination cluster and topic.

Show the user the preview and confirm three things in plain words:

- **which cluster and topic** it is going to, naming the cluster explicitly
  whenever `destination_cluster` is set,
- **what will consume it**, from step 2,
- **that it cannot be undone.** Kafka cannot delete a message. It stays until
  retention removes it, and any consumer reading that topic will process it.
  There is no equivalent of moving an offset back.

### 7. Write

Call again with `confirm: true`. The response reports the partition and offset
the broker assigned — record both, because that triple is what identifies the
message for any follow-up call.

Each message is one `items` entry, so several are one call rather than one call
each. Preview the whole batch first; one top-level `confirm` covers all of them.
The batch is **not** atomic: if a later item fails, the messages already written
stay, because Kafka cannot retract a produced record. Report per-item errors
exactly as they come back and never imply a partial batch was rolled back.

### 8. Verify by reading it back

Call `get_message` at the reported partition and offset, or `search_messages`
for the key. Confirm the value is what was intended and the provenance headers
are present.

For a repair, then check the consumer actually moved: `consumer_lag` should
show it processing again rather than stalled.

## Notes

- Every produced message carries `kafka-mcp-produced-at`,
  `kafka-mcp-produced-by-tool`, `kafka-mcp-produced-by-principal` and, when the
  client identifies itself, `kafka-mcp-produced-by-client`. This is not
  optional, because a message this server invented must never be
  indistinguishable from one a real producer sent. A header the caller supplies
  under one of those names is kept as given and the collision is reported.
- The destination topic must already exist. `produce_message` refuses a missing
  topic rather than relying on auto-creation, which would turn a typo into a new
  topic. Create it deliberately first; see
  [create-topic.md](create-topic.md).
- **Re-injecting a skipped message is not the same as replaying it.** If the
  original is still within retention and the payload was never actually broken,
  committing the earlier offset replays the real message and produces nothing;
  see [skip-poison-message.md](skip-poison-message.md). Producing a repaired
  copy is for when the stored payload itself cannot be processed.
- A repaired message arrives at the **end** of the partition, not at the
  original's position. Consumers process it in the order it was written, which
  means after everything produced in between. Say so when order matters.
- Producing into a compacted topic with an existing key replaces the value for
  that key once compaction runs. `describe_topic` reports `cleanup.policy`.
- If the broker refuses with an authorization error, the fix is a Kafka ACL:
  producing needs `WRITE` on the topic for the principal this server connects
  as. That is a request to whoever administers the cluster, not something to
  work around.
