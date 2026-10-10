# Partition skew

Find out why one partition is behind while the others are fine, and whether the
cause is the traffic or the consumer.

Use this when the user says "one partition always lags", "partition 3 is
behind", "one consumer pod is at 100% and the others are idle", or "adding
consumers didn't help".

## Tools

| Tool                      | Use it for                                               |
| ------------------------- | -------------------------------------------------------- |
| `consumer_lag`            | Lag per partition, and produce rate per partition        |
| `describe_consumer_group` | Which member and host owns the lagging partition         |
| `search_messages`         | Which keys send the traffic, with `group_by`             |
| `describe_topic`          | Partition count and size per partition                   |

## Steps

### 1. Measure lag and traffic per partition

Call `consumer_lag` with the topic and group. Read two lists side by side:

- **`groups[].partitions[].lag`** — where the backlog sits.
- **`produce_rate.partitions[]`** — how many messages each partition received
  in the last minute and hour.

### 2. Decide which kind of skew it is

| Traffic per partition | Lag per partition | What it is |
| --------------------- | ----------------- | ---------- |
| Uneven, and the lagging partition gets the most | Uneven | **Key skew.** The keys send more messages to one partition than one consumer can handle |
| Even | Uneven | **A slow consumer.** One member is slower than the others |
| Even | Even | Not skew. The whole group is slow: see [check-lag](check-lag.md) or [scale-partitions](scale-partitions.md) |

Check `offset_expired` before either, because an expired commit shows up as
enormous lag on one partition without being a backlog.

### 3a. Key skew: find the hot keys

Call `search_messages` on the lagging partition with `group_by: "return key"`
and a time window (`from_timestamp` for the last hour). Add `partitions` with
the lagging partition, and `max_groups: 10`. The result counts messages per key,
largest first.

- One or a few keys carrying most of the traffic is the answer. Name them.
- No dominant key, but the partition still gets more traffic: many keys hash to
  the same partition, or a producer sets the partition explicitly. A custom
  partitioner or explicit `partition` on the producer is the usual cause.

Explain the consequence plainly: **adding partitions or consumers cannot spread
one hot key**. Every message with that key goes to one partition, and one
partition is read by one consumer. The fixes are in the producer: a finer key
(for example `customer-42:order-17` instead of `customer-42`), or accepting
that ordering for that key is per sub-key. Adding partitions also moves keys
between partitions; see [scale-partitions](scale-partitions.md) before
suggesting it.

### 3b. Slow consumer: find the member

Call `describe_consumer_group`. The lagging partition names its `client_id` and
`host`. Compare with the members that keep up:

- The same host owning several lagging partitions points at that machine:
  CPU, memory, a noisy neighbour.
- One partition always slow, whichever member owns it, points back at the data.
  Larger messages on that partition are a common cause. `describe_topic`
  reports `size_bytes` per partition, and `search_messages` with
  `group_by: "return value_bytes > 100000"` shows how many large messages
  there are.

## Notes

- Produce rates are counted from offsets over real time windows, so they are
  history, not a sample.
- `group_by` counts every match in the range it reads. Bound it with a time
  window on a busy partition, and check `complete` before treating the counts as
  the whole picture.
