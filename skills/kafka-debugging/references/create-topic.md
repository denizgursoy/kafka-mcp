# Create a topic

Create a topic deliberately, with a partition count and a retention policy
someone chose, rather than the one auto-creation would have picked.

Use this when the user says "create a topic", "we need a new topic for X", "make
a dead letter topic", or asks for somewhere to put a message that has nowhere to
go — a `copy_message` destination must already exist, and this is how it comes
to exist.

## Tools

| Tool             | Use it for                                              |
| ---------------- | ------------------------------------------------------- |
| `server_config`  | Checking the server may change the cluster at all       |
| `list_topics`    | Confirming the name is free, and what the siblings do   |
| `describe_topic` | Copying the layout of an existing topic worth matching  |
| `consumer_lag`   | Sizing partitions from a topic that already carries the load |
| `create_topic`   | Previewing and creating                                 |

## Steps

### 0. Check the server may change anything

Call `server_config` **first**. If `read_only` is true, stop here.

A read-only endpoint does not expose `create_topic` at all, so there is no
preview to fall back on. Say the cluster can be inspected but not changed, and
that creating a topic needs a server configured without `read_only`. An endpoint
may also withhold `create_topic` on its own, through its `tools` configuration,
while staying writable: the tool list in `server_config` is what settles it, and
in both cases the answer is this server's configuration rather than Kafka.

### 1. Confirm the topic does not already exist

Call `list_topics` with a prefix. Two things come out of it:

- Whether the name is taken. `create_topic` refuses an existing topic rather
  than adjusting it, so this is the difference between creating something and
  discovering the user meant a topic they already have.
- What the neighbours are called. Topic names are a convention per cluster, and
  a topic named against that convention is the kind of mistake nobody fixes
  later.

If the user wants the new topic to behave like an existing one, call
`describe_topic` on that one and carry its partition count and configs over
rather than inventing new numbers.

### 2. Choose the partition count on purpose

This is the choice that cannot be taken back cleanly. Kafka can add partitions
later but never remove them, and adding them changes which partition a key
hashes to, which breaks ordering for keys already in the topic.

- **One partition** is right when order across the whole topic matters more
  than throughput. It caps the group at one consumer.
- **A count matching expected consumer parallelism** is the usual answer: a
  partition is read by exactly one member of a group, so the count is the
  ceiling on parallelism, not a source of it.
- **Omit it** on Kafka 2.4 or newer when there is nothing to base a number on.
  The broker default is a better guess than an arbitrary one, and the response
  reports what it was. Older brokers require an explicit count because the
  CreateTopics API did not support creation defaults before version 4.

Where the topic mirrors one already under load, size it from that one:
`consumer_lag` on the existing topic shows whether its own partition count is
already the limit.

The replication factor is the cluster's business more than the topic's. On
Kafka 2.4 or newer, omit it unless the user asks for something specific; it can
never exceed the number of brokers, and `create_topic` says so before the
broker does. Pass it explicitly on an older broker.

### 3. Set the configs that are actually decided

Pass `configs` for what the user has decided, not for everything Kafka can
express. Every key omitted is inherited from the cluster default and can be
changed later on the topic; the partition count is the part that cannot.

The ones that usually matter:

- `retention.ms` — how long messages are kept. The usual reason to create a
  topic explicitly rather than letting a producer do it.
- `cleanup.policy` — `delete` for an event stream, `compact` for a topic that
  holds the latest value per key.
- `max.message.bytes` — only when the payloads are known to be large.

### 4. Preview

Call `create_topic` **without** `confirm`. Nothing is created: the request goes
to the broker for validation, which is what catches an invalid name, a config
key the cluster does not accept, or a replication factor it cannot satisfy.

Show the user the preview, including the warnings. State the partition count in
plain words — "this topic will have 6 partitions, and that number can grow later
but never shrink" — because that is the decision they are actually confirming.

### 5. Create

Call `create_topic` again with `confirm: true`. The response reports the
partition count and replication factor **read back from the cluster**, so
verify they are what was intended, particularly when either was omitted and the
broker chose.

When several related topics are needed (for example main, retry and dead-letter
topics), put their separate specifications in `items`. Preview the whole batch,
then use top-level `confirm: true`. Creation is not atomic: report any per-item
error and never imply successful topics were rolled back.

If the broker refuses with an authorization error, the fix is a Kafka ACL:
creating a topic needs `CREATE` on the topic or the cluster for the principal
this server connects as. That is a request to whoever administers the cluster,
not something to work around.

### 6. Say what exists now, and what does not

A new topic is empty and has no consumers. Tell the user what still has to
happen:

- Producers must be pointed at it; nothing routes to it by itself.
- A consumer group appears only once a consumer joins, so `consumer_lag` and
  `list_consumer_groups` report nothing about it until then.
- If it was created as a `copy_message` destination, the copy is the next step
  and the topic alone changes nothing.

## Notes

- `create_topic` never modifies an existing topic. Changing a partition count is
  `add_partitions`, and it carries its own preview and its own warnings about
  keyed messages.
- Creating a topic to work around a problem in another one is worth questioning
  first. A topic created to escape a poison message leaves the original
  consumer just as stuck; see [skip-poison-message.md](skip-poison-message.md).
- A cluster with auto-creation enabled may have made the topic already, with
  whatever defaults the broker had. That is what step 1 finds.
