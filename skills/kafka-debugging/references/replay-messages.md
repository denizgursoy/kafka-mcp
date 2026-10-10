# Replay messages

Move a consumer group back so it processes messages again — everything since a
moment, everything the topic still holds, or a range of one partition.

Use this when the user says "reprocess everything since 9 this morning", "the
fix is deployed, replay yesterday's orders", "start this consumer from the
beginning", or "rewind payments to before the bad deploy".

## Tools

| Tool                      | Use it for                                              |
| ------------------------- | ------------------------------------------------------- |
| `server_config`           | Checking this endpoint may move offsets at all          |
| `describe_consumer_group` | Where the group stands now, and whether it is running   |
| `describe_topic`          | Oldest timestamp, to check the moment is still retained |
| `commit_offset`           | Previewing and moving the group                         |
| `consumer_lag`            | Confirming the replay is being consumed                 |
| `list_consumer_groups`    | Finding the group when the user names an application    |

## Steps

### 0. Check the endpoint may move offsets

Call `server_config` **first** and look for `commit_offset` in its `tools` list.
A read-only endpoint does not expose it. Say the replay can be planned but not
applied here, and stop before the user stops any consumers.

### 1. Establish group, topic and moment exactly

A replay needs three things, and each must be explicit:

- **The group.** Replaying moves one group; every other group reading the topic
  is untouched. If the user names an application rather than a group, ask, or
  use `list_consumer_groups` with the topic and let them choose.
- **The topic.** Never guess between plausible names.
- **The moment**, as an RFC3339 time **with a zone**: `2026-10-02T09:00:00+02:00`
  or `...Z`. "9 this morning" is ambiguous until the zone is settled — ask which
  one, rather than assuming UTC.

`earliest` means everything the topic still holds; `latest` means skip
everything unread. Both are `position`, not `timestamp`.

A group whose `consumer_lag` shows `offset_expired` needs this guide even when
the user did not ask for a replay: retention deleted its position, and unless
an offset is committed deliberately the consumer's `auto.offset.reset` decides
for it, silently skipping or reprocessing. Moving it to `earliest` keeps every
message still retained; `latest` drops the backlog. Make that choice with the
user.

### 2. Check the moment is still in the topic

Call `describe_topic` and compare `oldest_timestamp` with the moment. Messages
older than retention are already gone; a replay from before `oldest_timestamp`
starts at the oldest message that remains, which is not what the user asked
for. Say so before previewing, not after.

### 3. Check the group is stopped

Call `describe_consumer_group`. A running consumer keeps its position in memory
and overwrites any commit with its next one, so a replay applied to a `Stable`
group appears to work and changes nothing. Ask the user to stop the consumers
and call again until the state is `Empty`.

`allow_active_members` exists only for a restart that happens immediately
afterwards; it does not make the commit stick.

### 4. Preview the replay

Call `commit_offset` with **`confirm` omitted** and one item:

```json
{"items": [{"topic": "orders", "group": "payments", "timestamp": "2026-10-02T07:00:00Z"}]}
```

Omitting `partition` moves every partition of the topic, each to its own first
message at or after the time. The result reports, per partition and in total,
`replayed_messages` — how many messages will be processed a second time.

Read the per-partition notes. A partition whose `note` says no message exists at
or after the time moves to its end: nothing is replayed there. If every
partition says so, the moment is probably wrong (a date typo, a wrong zone).

### 5. Get explicit consent, naming the duplicates

Tell the user how many messages will be processed again, and ask whether the
consumer is safe to run twice on the same message. **A replay produces
duplicates by design.** A consumer that charges a card, sends an email, or
increments a counter does it again for every replayed message unless it is
idempotent.

Never infer consent from "the fix is deployed". The user agrees to the replay
count, not to the goal.

### 6. Apply, restart, verify

Call again with `confirm: true`. Each partition's `resulting_offset` is re-read
from the broker. Have the user restart the consumers, then call `consumer_lag`:
expect lag equal to the replay count, `status: draining`.

## Notes

- Several topics or groups are one call with several items. A whole-topic item
  and a single-partition item for the same group and topic are refused in one
  batch, because which one wins would depend on order.
- An exact offset needs `partition`: the same number in another partition is an
  unrelated message.
- Moving **forward** with the same tool skips messages instead. That is a
  different decision, covered by
  [skip-poison-message.md](skip-poison-message.md).
- A replay into a topic another system also consumes affects only the group
  moved. To replay into a *different* consumer without disturbing the original,
  have the user start it under a new group id, then move that group.
