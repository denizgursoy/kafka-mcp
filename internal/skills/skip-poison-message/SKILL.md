---
name: skip-poison-message
description: Use when a Kafka consumer is stuck on a message it cannot process — a poison message, a malformed payload, or a consumer that keeps failing on the same offset and never advances. Covers confirming the consumer is genuinely blocked, preserving the message before it becomes unreachable, and moving the consumer group past it. Use for requests like "the consumer is stuck", "skip this bad message", "our consumer keeps crashing on the same record", or "unblock the payments consumer".
---

# Skip a poison message

Move a consumer group past a message it cannot process, after preserving the
message so the failure can still be understood.

Use this when the user says "the consumer is stuck", "it keeps crashing on the
same message", or "skip this record and move on".

## Tools

| Tool                   | Use it for                                          |
| ---------------------- | --------------------------------------------------- |
| `server_config`        | Checking whether this server may change anything     |
| `consumer_lag`         | Confirming the consumer is genuinely stuck           |
| `list_consumer_groups` | The group's state and member count                   |
| `get_message`          | Reading the message that is blocking the consumer    |
| `copy_message`         | Preserving it in a dead letter topic                 |
| `search_messages`      | Preserving several messages to a file                |
| `commit_offset`        | Moving the group past the message                    |

## Steps

### 0. Check the server may change anything

Call `server_config` **first**. If `read_only` is true, stop here.

Tell the user the message can be diagnosed but not skipped, and that skipping
needs a server configured without `read_only`. Do not walk them through
finding and preserving the message for a fix that cannot happen: that wastes
their time during an incident and may leave a copy in a dead letter topic for
a skip that never occurs.

Diagnosis is still worth doing, and steps 1 and 2 still work. What stops is the
promise of a fix.

### 1. Confirm the consumer is actually stuck

Call `consumer_lag`. A poison message looks like this:

- **`status: stalled`** with members present — the group has consumers, and
  they are consuming nothing. This is the signature.
- Lag that does not shrink between two calls a few seconds apart.

It is **not** a poison message when:

- **`status: no_active_consumers`** — nothing is running. Start the consumer.
- **`status: draining`** — it is working, just slowly. Skipping loses data for
  no reason.
- **`status: growing`** — consumers cannot keep up. That is a capacity problem;
  see the scale-partitions skill.

Say so plainly when the diagnosis does not fit. Skipping a message that was
going to be processed anyway destroys data for nothing.

### 2. Read the blocking message

The consumer is stuck on the message at its committed offset, which
`consumer_lag` reports per partition. Read it with `get_message`, and set
`context` to a few messages so the surrounding sequence is visible.

Show the user what it is. Often the message itself explains the failure — a
truncated payload, an unexpected type, a schema that changed.

### 3. Offer preservation, and let the user choose

Once the offset moves past it, the message stays in Kafka only until retention
removes it, and nothing points at it any more. Preserve it first.

Present the three options with what each is good for, and **wait for the user
to choose**:

- **`copy_message` to a dead letter topic** — durable, stays in Kafka, and the
  copy carries headers recording where it came from, when, and by which
  principal. Best when the message will be reprocessed later, or when other
  systems need to see it. The destination topic must already exist.
- **`search_messages` with `output_file`** — writes the messages as JSON lines
  on the server. Best for several messages at once, or for inspecting them
  outside Kafka. Nothing needs to exist first.
- **Show it in the conversation** — best for a single small message the user
  will handle themselves. It is lost when the session ends.

Do not choose for them. Where the data goes is their decision.

### 4. If they decline all three

Say plainly that the skipped messages will be unrecoverable once retention
removes them, and ask them to confirm **that specifically**, separately from
confirming the skip.

This is a second confirmation on purpose. Losing the evidence and skipping the
message are two different decisions, and an operator under pressure should make
each one knowingly.

### 5. Stop the consumers

`commit_offset` refuses while the group has active members, and the reason
matters: a running consumer keeps its position in memory and only reads the
committed offset when it joins a group. A commit made while it is running is
overwritten by its next commit, leaving the group exactly where it was.

So the fix would appear to work and change nothing. Have the user stop the
consumers, then confirm with `list_consumer_groups` that the group is `Empty`.

`allow_active_members` exists for the case where the consumers are restarted
immediately afterwards. It does not make the commit stick; it only stops the
tool refusing.

### 6. Preview the skip

Call `commit_offset` **without** `confirm`. The response gives the current
offset, the target, and how many messages will be skipped.

To skip the message at offset 42, the target is **43**: the offset is the one
the group reads next.

Check the skipped count. If it is larger than expected, the target is wrong —
say so rather than confirming.

### 7. Get explicit consent

Tell the user exactly how many messages will never be processed, and that this
cannot be undone by Kafka: the only way back is to commit the earlier offset
again, which replays everything after it.

Never infer consent from "unblock the consumer". Unblocking is the goal;
skipping specific messages is the method, and the user has to agree to it.

### 8. Commit, restart, verify

Call `commit_offset` with `confirm: true`. The tool re-reads the offset and
reports where the group now stands.

Have the user restart the consumers, then call `consumer_lag` again. Expect
`status: draining` and lag falling. If it is `stalled` again at a new offset,
there is another poison message, and this whole sequence repeats.

## Notes

- A repeatedly poisoned topic is usually a producer or schema problem. After
  the second or third skip, say so: skipping is a way to restore service, not
  a fix.
- `commit_offset` moves one partition. A consumer stuck on several partitions
  needs one call per partition, each previewed separately.
- Moving the offset backward is the same tool and replays messages instead of
  skipping them, which produces duplicates rather than losing data.
