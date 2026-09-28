# Delete a topic

Remove a topic and everything in it, deliberately, after the user has seen what
that destroys.

Use this when the user says "delete this topic", "remove the old orders topic",
"clean up these test topics", or asks to tidy a cluster after an experiment.

This is the most destructive thing the server can do. **Kafka has no undo.**
Recreating the topic afterwards gives an empty topic with the same name, not the
data back. Treat every step below as load-bearing.

## Tools

| Tool                   | Use it for                                            |
| ---------------------- | ------------------------------------------------------ |
| `server_config`        | Checking this endpoint may delete at all               |
| `list_topics`          | Finding the exact names, and what the siblings are     |
| `describe_topic`       | How much data and how many partitions are at stake     |
| `list_consumer_groups` | Who reads the topic today                              |
| `search_messages`      | Preserving anything worth keeping, with `output_file`  |
| `copy_message`         | Preserving individual messages elsewhere first         |
| `delete_topic`         | Previewing and deleting                                |

## Steps

### 0. Check the endpoint may delete

Call `server_config` **first** and look for `delete_topic` in its `tools` list.

A read-only endpoint does not expose it at all, so there is no preview to fall
back on. Say the topic can be inspected but not deleted, and that deleting needs
a server configured without `read_only`. An endpoint may also withhold
`delete_topic` on its own through its `tools` configuration while staying
writable — a sensible thing for a deployment to do, and the tool list is what
settles it.

### 1. Establish exactly which topics, by exact name

Call `list_topics` and confirm each name character for character. A topic name
is not a pattern here: `orders` and `orders-v2` are different topics, and there
is no recovery from deleting the wrong one.

If the user described the topics rather than naming them — "the old ones", "the
test topics" — list the candidates and make them choose explicitly. Never expand
a description into a deletion list yourself.

### 2. Find out what is inside, and who reads it

Preview with `delete_topic` and **`confirm` omitted**. Nothing is deleted. Each
item's result reports:

- **`message_count`** — how many messages would be destroyed. This is the number
  the user is really deciding about.
- **`partitions`** — the shape, so a mistaken deletion can at least be
  recreated.
- **`consumer_groups`** — the groups with committed offsets for this topic. Each
  one breaks when it disappears.

A non-empty `consumer_groups` deserves its own sentence to the user. Something
is reading this topic today, and someone owns it.

### 3. Offer preservation before destroying anything

If `message_count` is not zero, ask what should happen to the data **before**
asking whether to delete:

- **`search_messages` with `output_file`** — writes every message to a file on
  the server as JSON lines. The practical option for a whole topic.
- **`copy_message` to another topic** — for a handful of messages worth keeping
  inside Kafka, possibly on another cluster.
- **Nothing** — legitimate for a scratch topic, but say plainly that the
  messages are gone for good.

Do not choose for them, and do not skip this because the topic looks unimportant.

### 4. Get two separate acknowledgements

`delete_topic` needs two things, and they answer different questions:

- **`confirm: true`** at the top level — the caller meant to delete.
- **`acknowledge_data_loss: true`** on any item whose topic still holds
  messages — the caller knows what is inside.

Ask for them separately, in those terms. Tell the user the message count and the
consumer groups in the same breath as asking. Never infer either from "clean
this up": deleting a topic is a method, and the user has to agree to the method,
not just the goal.

### 5. Delete, and report what happened

Call again with `confirm: true` and the acknowledgements set. Each result
reports `deleted: true` and the count that was destroyed.

For several topics, put them in one `items` array. One `confirm` covers the
batch, but **each** topic holding data needs its own `acknowledge_data_loss`.
The batch is not atomic: if a later item fails, the topics already deleted stay
deleted. Report per-item errors exactly as they come back and never imply a
partial batch was rolled back.

### 6. Say what is now broken

Deletion does not end at the broker. Tell the user what still needs doing:

- The consumer groups named in step 2 are now reading a topic that does not
  exist. Whoever owns them has to be told.
- Producers still pointing at the topic will fail, or silently recreate it if
  the cluster has auto-creation enabled.
- Metadata caches are a few seconds old, so `list_topics` may still show the
  topic briefly. That is the cache, not a failed deletion.

## Notes

- **Internal topics are refused outright**, at any level of acknowledgement.
  `__consumer_offsets` and its siblings hold cluster state rather than anyone's
  data, and deleting one breaks every consumer on the cluster at once.
- **Naming the same topic twice in one batch is refused** before anything is
  deleted, because it means the caller has lost track of what they are removing.
- `message_count` is an offset span, so it can overcount where retention or
  compaction has already removed records. It never undercounts, which is the
  safe direction for a warning.
- The count is re-read at deletion time rather than trusted from the preview, so
  a topic that gained messages since the caller looked is still caught by the
  acknowledgement.
- If the broker refuses with an authorization error, the fix is a Kafka ACL:
  deleting needs `DELETE` on the topic for the principal this server connects
  as. That is a request to whoever administers the cluster, not something to
  work around.
- A topic deleted to "fix" a problem usually does not fix it. A poison message
  ([skip-poison-message.md](skip-poison-message.md)) and a partition count
  ([scale-partitions.md](scale-partitions.md)) both have their own remedies that
  keep the data.
