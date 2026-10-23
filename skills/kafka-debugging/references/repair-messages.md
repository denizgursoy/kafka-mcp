# Repair messages

Rewrite many messages that were produced in the wrong shape — a document
wrapped in a field it should not be in, a field with the wrong name, a key that
should have been an id from the body — and write the corrected versions back.

Use this when the user says "these messages came in wrapped in payload", "the
real data should have been the payload itself", "fix every message where X",
or "rewrite these and send them again". For one or a handful of messages built
by hand, [produce-message](produce-message.md) is enough.

Kafka cannot edit a message. A repair always means **new** messages, appended
after everything already in the topic, with the broken originals left where
they are until retention removes them. Everything below follows from that.

## Tools

| Tool                   | Use it for                                                |
| ---------------------- | --------------------------------------------------------- |
| `server_config`        | Whether this endpoint may write, and to which clusters     |
| `list_clusters`        | Whether a `destination_cluster` is writable from here     |
| `sample_messages`      | Seeing the broken shape and the topic's `format`          |
| `search_messages`      | Counting the broken messages and finding the range        |
| `describe_topic`       | Partition counts, and whether the destination is compacted |
| `list_consumer_groups` | Who reads the destination, before writing into it         |
| `transform_messages`   | Previewing, downloading and writing the repair            |
| `produce_message`      | Sending a downloaded repair with `upload`                 |
| `get_message`          | Reading a message the transform failed on                 |
| `create_topic`         | Creating a separate destination topic                     |

## Steps

### 0. Check the endpoint may write

Call `server_config` **first**. `transform_messages` stays registered on a
read-only endpoint, because the destination is chosen per call, and previewing
or saving to a file writes nothing to Kafka. Only `confirm` needs a writable
destination. Find out now whether the user's destination can be written from
this session, not after they have approved the repair.

### 1. See the broken shape

Call `sample_messages` on the topic, or `get_message` on an address the user
gave. Confirm with the user what is wrong and what correct looks like, on a real
message rather than a description: "the value is `{"payload":{...}}` and should
be the inner object".

Read `format` and `schema_id`. A schema-encoded value (Avro, Protobuf, JSON
Schema) is decoded for the transform and re-encoded with **the same schema id**,
so the corrected document must still fit that schema. If the repair needs a
different schema, the user must say which; pass it as `value_schema`.

### 2. Find exactly which messages are broken

Write the predicate that selects only the broken messages and test it with
`search_messages` `count_only` before transforming anything:

```json
{"topic": "orders", "script": "return value.payload !== undefined", "count_only": true}
```

Messages that are already correct must not match: transforming them would
corrupt them. If the bad messages came from one incident, narrow the range with
`from_timestamp`/`to_timestamp` too. Report the count and the per-partition
breakdown to the user. This is the number they will approve.

### 3. Decide where the repaired messages go

There are two choices, and the user makes this decision:

- **Back into the same topic.** Consumers process the repaired messages as new
  ones. The broken originals are still there, behind them. If a consumer is
  stuck on a broken message, moving it past the broken range is a separate step
  ([skip-poison-message](skip-poison-message.md)); a repair alone does not
  unblock it. Consumers that already processed or skipped the originals will
  also process the repairs, so check they tolerate seeing an entity again.
- **Into another topic**, such as `orders-fixed`, from which a consumer is
  pointed at the result. Nothing that reads the original topic is affected. The
  topic must exist first ([create-topic](create-topic.md)) with at least as
  many partitions if `keep_partition` is used.

Either way, call `list_consumer_groups` for the destination and name the
consumers that will process the repaired messages.

**Keys decide ordering.** The transform keeps the original key unless it
returns one, so a repaired message lands on its key's partition, but it lands
**after** every message already there. Say so when order between an entity's
messages matters. If the source producer picked partitions explicitly rather
than by key, set `keep_partition` so each repair goes to the partition number
it came from.

### 4. Write the transform

The transform sees the same variables as the search script and returns an
object with any of `value`, `key` and `headers`:

| Repair | Transform |
| ------ | --------- |
| Unwrap a nested document | `return { value: value.payload }` |
| Rename a field | `const v = { ...value, orderId: value.order_id }; delete v.order_id; return { value: v }` |
| Key from the body | `return { key: value.id }` |
| Leave some out | `if (value.test) return null; return { value: value.payload }` |

| Take a message out of a DLQ | `return { value, headers: { ...headers, 'error-reason': null } }` |
| Fix a field by rule | `return { value: { ...value, status: value.status.toUpperCase() } }` |

The transform is ordinary JavaScript run once per message, so `if`, loops,
`map`/`filter`, regular expressions, `JSON.parse` of a string field and date
arithmetic all work. There is no `console`, `require` or network.

A field the transform leaves out is kept exactly. Returning `headers` replaces
all of them, so spread the originals in to keep them, and set one to `null` to
remove it. An unknown field is refused rather than ignored.

**Large integers.** An integer beyond 2^53 — a 64-bit id — arrives as a
`BigInt`, so passing it through writes it back exactly. Compare it with a
BigInt literal such as `value.id === 1234567890123456789n`, or with `String()`.
Arithmetic with ordinary numbers throws, and `Number()` rounds it; a message
whose output holds a rounded integer fails in the preview with a reason saying
so, rather than being written with the wrong id. The `script` predicate sees
numbers the way `search_messages` does, so the count from step 2 still matches.

### 5. Preview

Call `transform_messages` with the `script` from step 2 and **without**
`confirm`. Nothing is written to Kafka. Check, and show the user:

- `matched` equals the count from step 2. A different number means the range or
  the predicate changed.
- `complete` is `true`. Otherwise `max_messages` or the timeout stopped the
  preview, and the counts cover only part of the range. Raise the limit before
  approving.
- the `examples`, before and after, look right.
- `failed` and `errors`. Each error group names a reason and example
  addresses. Read one with `get_message`; usually the predicate let through a
  message the transform cannot handle. Fix the predicate or the transform and
  preview again. **Do not confirm with failures the user has not accepted**:
  a failed message is not written, so it stays broken.

### 6. Optionally save and inspect first

When the user wants to look at the whole repaired set before it reaches Kafka,
or the repair needs a sign-off from someone else, call it with
`download: {"filename": "orders-fixed.jsonl"}` instead of `confirm`, and run the
returned command on the user's machine. The file lands there, never on the
server, and each line is a `produce_message` item, so it can be checked and
then sent later from that machine:

```json
{"name": "produce_message", "arguments": {"upload": {"filename": "orders-fixed.jsonl"}}}
```

Run the returned `curl -T` command where the file is. Without `confirm` the
upload validates every line and reports the bad ones by line number; ask for a
new link with `confirm: true` to send the valid lines in file order.
Downloading works on a read-only endpoint, so the repair can be prepared there
and sent from a session that may write.

### 7. Write

Call `transform_messages` again with the same arguments and `confirm: true`,
after the user has approved the count, the destination, the consumers and the
examples. State plainly that the written messages cannot be removed.

One `confirm` sends every selected message; it does not need splitting. The
confirmed call reads the topic again rather than reusing the preview, so to
send exactly what was previewed, pass each partition's `scanned_ranges` `end`
from the preview as `to_offset` (one call per partition when they differ).

Check `written` against `transformed`. Writing is **not** atomic: a run stopped
by a broker refusal (`stopped_reason: write_failed`) or by the timeout keeps
what it already wrote. `scanned_ranges` says how far each partition got: its
`end` is the first message not written, so continue with `partitions: [n]` and
`from_offset` set to that `end`. Never run the whole range again, which would
write the repaired messages twice. A broker refuses a produce batch as a whole,
so one oversized message can fail the messages sent with it; they are counted
in `failed` and lie after `end`, so a resumed run retries them.

### 8. Verify

Read a repaired message with `get_message`, or search the destination for the
`kafka-mcp-transformed-from-offset` header, and confirm the value is right and
the provenance headers point at the original. Then check the consumer is
processing the repairs with `consumer_lag`.

## Notes

- Every repaired message carries `kafka-mcp-transformed-from-cluster`,
  `-from-topic`, `-from-partition` and `-from-offset`, with `-at`, `-by-tool`,
  `-by-principal` and, when known, `-by-client`. Transforming a repaired message
  again replaces these rather than stacking a second set.
- Writing back into the source topic is safe from loops: the scan ends at the
  end offset the call started with, so it never reads the messages it wrote.
- A null `value` would be a tombstone, which deletes the key on a compacted
  topic, so the transform cannot return one. Return `null` for the whole
  message to skip it.
- Moving a schema-encoded value to a cluster with a different registry needs
  `value_schema`, because the original schema id may not exist there.
- An authorization error on write is a Kafka ACL to request: writing needs
  `write` on the destination topic for the principal this server connects as.
