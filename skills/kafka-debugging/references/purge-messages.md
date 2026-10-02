# Purge messages

Delete old messages from a topic while keeping the topic, its configuration and
its consumer groups — and clean up consumer groups nobody uses any more.

Use this when the user says "delete the test data but keep the topic", "remove
everything before offset 812", "empty this partition", "clear out these bad
messages at the start", or "remove the old consumer groups that keep alerting".

## Tools

| Tool                      | Use it for                                                   |
| ------------------------- | ------------------------------------------------------------ |
| `server_config`           | Checking this endpoint may delete at all                     |
| `describe_topic`          | Each partition's start and end offset                        |
| `search_messages`         | Finding the offset to cut at, and preserving data first      |
| `delete_records`          | Previewing and deleting messages                             |
| `list_consumer_groups`    | Finding `Empty` groups on a topic                            |
| `delete_consumer_group`   | Previewing and deleting abandoned groups                     |

## Steps for messages

### 0. Check the endpoint may delete

Call `server_config` **first** and look for `delete_records` (and
`delete_consumer_group` if groups are involved) in its `tools` list. A
read-only endpoint does not expose either.

### 1. Decide whether the topic should be deleted instead

`delete_records` removes the **oldest** messages of a partition, up to an
offset. It cannot remove messages from the middle, and it cannot select by
content. If the user wants the whole topic gone and nobody depends on its name
or configuration, [delete-topic.md](delete-topic.md) is simpler.

### 2. Establish the cut, per partition

Every item is one partition and one `before_offset`: every message below it is
deleted, the message at it is kept and becomes the first readable one.

- **Emptying a partition** — use its end offset from `describe_topic`.
- **Up to a moment** — find the first offset at or after that time with
  `search_messages` (`from_timestamp`, `direction: oldest_first`,
  `max_matches: 1`) on each partition.
- **Up to a bad run of messages** — find the first good message's offset and
  cut there.

Partitions are independent. Offset 812 in partition 0 and offset 812 in
partition 1 are unrelated messages, so never reuse one partition's cut for
another.

### 3. Preview

Call `delete_records` with **`confirm` omitted**. Each item reports
`messages_deleted` and `affected_groups`: every consumer group whose committed
offset is below the cut, with `unprocessed_lost` — messages it will never
process. Those groups resume from the new start offset.

A non-empty `affected_groups` needs its own sentence to the user. That consumer
is behind, and this deletes work it still has to do.

### 4. Offer preservation

Before deleting, offer `search_messages` with `output_file` over the same range,
which writes every message to the server as JSON lines. Let the user decide;
do not skip the offer because the data looks like test data.

### 5. Get two acknowledgements, then delete

`confirm: true` at the top level, and `acknowledge_data_loss: true` on each
item. Ask for them in terms of the count: "812 messages in partition 0 will be
deleted, and Kafka cannot restore them". The result reports
`resulting_start_offset`, read from the broker.

## Steps for abandoned consumer groups

### 1. Find candidates, and let the user choose

`list_consumer_groups` with the topic and `states: ["Empty"]` lists groups with
no running members. **Empty is not the same as abandoned**: a consumer stopped
for a deploy is Empty too. Present the list and let the user name the ones that
are really gone.

### 2. Preview

Call `delete_consumer_group` with **`confirm` omitted**. Each result lists the
group's committed offsets and `total_lag` — the lag that stops being reported.
A group with active members is refused.

### 3. Delete

With the user's consent, call with `confirm: true`. If anything later starts
with the same group id, it begins from its `auto.offset.reset`, not from where
the group left off. Say so.

## Notes

- Internal topics are refused.
- Two items for the same partition are refused before anything is deleted.
- The batch is not atomic: partitions truncated or groups deleted before a later
  item failed stay that way.
- Compacted topics are not supported for record deletion by every broker; the
  broker's error is reported on the item.
- Deleting records does not reclaim disk at once: whole segments below the new
  start offset are removed, and the rest become unreadable but stay on disk
  until their segment rolls.
- An authorization error means the principal needs `DELETE` on the topic or the
  group; see [authorization-error.md](authorization-error.md).
