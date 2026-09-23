# Scale partitions

Add partitions to a topic, safely and only when it will actually help.

Use this when the user says "add partitions", "increase the partition count",
"scale this topic", or "we need more consumers on this topic".

## Tools

| Tool                   | Use it for                                          |
| ---------------------- | --------------------------------------------------- |
| `consumer_lag`         | Checking whether scaling will help at all           |
| `list_consumer_groups` | Seeing who consumes the topic and how many members  |
| `describe_topic`       | Current partition count and layout                  |
| `sample_messages`      | Whether messages are keyed, and by what             |
| `add_partitions`       | Previewing and applying the change                  |
| `server_config`        | Checking the server may change the cluster at all   |

## Steps

### 0. Check the server may change anything

Call `server_config` **first**. If `read_only` is true, stop here.

A read-only endpoint does not expose `add_partitions` at all, so there is no
preview to fall back on. Tell the user the topic can be diagnosed but not
scaled, and that scaling needs a server configured without `read_only`. Do not
walk them through the investigation below for a change that cannot happen.

Steps 1 and 2 are still worth doing on their own terms, because knowing whether
scaling would help is useful even when this server cannot apply it. What stops
is the promise of a fix.

### 1. Check that scaling is the right answer

This is the step most often skipped, and skipping it means doing something
irreversible for no benefit.

Call `consumer_lag` and `list_consumer_groups` first, then read the result:

- **`status: stalled`** — members are present but consuming nothing. Adding
  partitions will not help. The consumer is stuck; find out why.
- **`status: no_active_consumers`** — nobody is consuming. Start a consumer.
  More partitions change nothing.
- **members < partitions** — the group already has idle capacity, because a
  partition is consumed by exactly one member. Adding partitions gives the
  existing members more partitions each, not more throughput. **Add consumers
  first.**
- **members == partitions and lag is growing** — this is the case where
  scaling genuinely helps. The group cannot add parallelism without more
  partitions.

Say so plainly when scaling is not the answer. "Adding partitions will not fix
this" is more useful than doing it anyway.

### 2. Find out whether the messages are keyed

Call `sample_messages`. If `key_stats` shows keys are present, ordering is at
stake, and the user needs to make an informed decision rather than a quick one.

Kafka chooses a partition by hashing the key modulo the partition count.
Change the count and the same key maps somewhere else:

```
merchant_id=4, 3 partitions  -> partition 1
merchant_id=4, 6 partitions  -> partition 4
```

Messages already written stay where they are. New messages for that key go
elsewhere. Two consumers can now process the same key at the same time, out of
order. For balances, state machines or event sourcing, that is silent
corruption rather than a visible failure.

### 3. Preview the change

Call `add_partitions` **without** `confirm`. Nothing is changed. The response
gives the current count, the target, whether messages are keyed, which consumer
groups will rebalance, and the warnings.

Show the user that preview before going further.

### 4. Get explicit consent

Tell the user plainly:

- **This cannot be undone.** Kafka cannot reduce a partition count. Undoing it
  means creating a new topic with the count you wanted
  ([create-topic.md](create-topic.md)) and migrating the data to it.
- If the topic is keyed, **ordering for existing keys will break**, and say
  which field the key appears to be.
- Which consumer groups will rebalance.

Never infer consent from "make it faster" or "fix the lag". A request to solve
a problem is not a request for this specific irreversible change.

### 5. Apply

Call `add_partitions` again with `confirm: true`, adding
`acknowledge_key_ordering: true` when the topic is keyed. The tool refuses a
keyed topic without it.

`partitions` is the **final total**, not the number to add. Asking for 6 on a
topic that already has 6 does nothing, so repeating a call is safe.

For several topics, use one `items` batch and put each final target and keyed
ordering acknowledgement on its own item. Preview the whole batch first.
Partition changes are not atomic and successful items cannot be rolled back.

If the broker refuses with an authorization error, the fix is a Kafka ACL:
adding partitions needs ALTER on the topic for the principal this server
connects as. That is a request to whoever administers the cluster, not
something to work around.

If `add_partitions` is not among the tools at all, the cluster is read-only and
step 0 was skipped. That is this server's configuration, not Kafka.

### 6. Verify and explain what happens next

The tool re-reads the topic and reports `resulting_partitions`. Confirm it
matches what was asked.

Then tell the user what to expect:

- **New partitions start empty.** Existing data does not move, so the topic
  will look unbalanced until new messages spread across the new partitions.
- **Consumer groups rebalance**, which pauses consumption briefly.
- **Throughput only improves if consumers are added too.** Partitions set the
  ceiling on parallelism; they do not provide it.

## Notes

- The key check samples recent messages. A topic that was keyed in the past but
  is not now will read as unkeyed, so treat it as evidence rather than proof.
- Partition count caps useful consumer parallelism: a group can never usefully
  have more members than the topic has partitions.
