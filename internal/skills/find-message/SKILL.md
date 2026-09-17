---
name: find-message
description: Use when locating Kafka messages by their content — finding a message by order id, correlation id, customer id or message key, or selecting messages by a field condition such as event type or amount. Covers searching a topic, reading the scan report before answering, and handling large result sets. Use for requests like "find the message for order 12345", "which message had this correlation id", or "show me the failed payments over 500".
---

# Find a message

Locate Kafka messages by their content, when the user knows something about a
message but not where it is.

Use this when the user says things like "find the message for order 12345",
"which message had this correlation id", "show me the failed payments over 500",
or "did we ever receive an event for this customer".

## Tools

| Tool               | Use it for                                             |
| ------------------ | ------------------------------------------------------ |
| `list_topics`      | Finding the topic when the user does not name one      |
| `sample_messages`  | Learning how messages are shaped before searching      |
| `describe_topic`   | Sizing a topic and choosing a range                    |
| `search_messages`  | Scanning a bounded range for matching messages         |
| `get_message`      | Reading one located message in full, with its context  |

## Steps

### 1. Establish the topic

If the user named a topic, use it. Otherwise call `list_topics`, with `search`
if their wording suggests a name, and ask them to choose when several are
plausible. Do not guess.

### 2. Learn the shape first

Call `sample_messages` **before** searching. This is the step that decides how
to search, and skipping it usually produces a slow, imprecise query.

Read three things from the result:

- **`key_in_value`** — if it names a field such as `payload.orderId`, then the
  key *is* that identifier. This is the single most useful fact in the whole
  investigation.
- **`key_stats`** — are keys present, and unique?
- **`value_formats`** and **`json_fields`** — is the value JSON, and which
  paths and types can a filter use?

`sampled_ranges` shows which offsets the sample came from. The sample is of the
newest messages, so a topic whose format changed over time may hold older
messages of a different shape. Bear that in mind if a search of older data
finds nothing.

### 3. Write the narrowest script

Filtering is a JavaScript expression. In scope are `value` (the parsed JSON
document, or the raw text when the message is not JSON), `key`, `headers`,
`partition`, `offset` and `timestamp`. Return true to keep a message.

**If the identifier is the key**, compare it exactly:

```js
return key === 'order-123'
```

This matters more than it looks. Searching the body for a bare id like `123`
also matches `"amount": 1123` and `"ts": "...T01:23"`. Those false positives
fill up `max_matches` and the message actually wanted is never reached — a
confident wrong answer, not merely a slow one.

**If the user describes a condition**, read the fields directly:

```js
return value.eventType === 'NEW' && value.payload.amount >= 500
```

Useful things the script can do that a fixed filter grammar cannot:

```js
return value.payload.items.some(function (i) { return i.qty > 100 })
return value.payload.amount > value.payload.refunded
return timestamp.getUTCHours() < 6
return /ORD-\d{4}/.test(value.payload.orderId)
```

Two traps worth respecting:

- A missing field is `undefined`, a field set to null is `null`. Use
  `=== null` for "present and empty" and `=== undefined` for "absent". They
  are different states and conflating them hides schema drift.
- On a topic that is not JSON, `value` is a **string**, so use string methods:
  `return value.indexOf('ERROR') >= 0`.

Check `script_errors` in the result. A high count means the script is throwing
on messages it did not expect, which is a broken filter rather than an absence
of matches.

### 4. Narrow the range

Call `describe_topic` to see the size. Turn what the user knows into
constraints, because each one removes messages that must otherwise be read:

- a time ("this morning", "last Tuesday") → `from_timestamp` / `to_timestamp`
- a known offset neighbourhood → `from_offset` / `to_offset`
- a known partition → `partitions`

`describe_topic` also returns the topic's configuration, and two entries decide
whether the message can still exist at all:

- **`retention.ms`** — messages older than this are gone. If the user asks
  about something older, say so instead of scanning: no search can find it.
  Retention is enforced per log segment, so messages may survive somewhat
  longer than the setting suggests.
- **`cleanup.policy`** — `compact` means Kafka keeps only the most recent
  message per key. Earlier values of a key are gone even inside the retention
  window, so "not found" for a superseded message is expected, not a failure.

**Never infer a partition from a key.** Producers may set the partition
explicitly when producing, so the key does not determine it. Guessing wrong
means reporting "not found" for a message that exists.

### 5. Check the scan report before answering

Never report "not found" from an empty match list alone. Check `complete`:

- `complete: true` — the whole range was read. No match means it is not there.
- `complete: false` — the scan stopped early. Say so, give `stopped_reason` and
  `scanned_ranges`, then narrow the search or raise `max_messages_scanned`.

Treating an incomplete scan as proof of absence is the main way this
investigation goes wrong.

### 6. Consider parallelism for large scans

`parallelism` splits each partition's offsets between that many readers, so
even a single-partition topic is scanned concurrently. It is worth setting for
`count_only`, `output_file` or a full-range search.

It is usually **not** worth it for a narrow newest-first lookup: a sequential
scan reads the newest chunk and stops, while parallel readers have already read
the older ranges that sequential scanning would never have touched.

### 7. Handle large result sets deliberately

If the query may match many messages, do not fetch bodies first. Call
`search_messages` with `count_only: true` to learn how many there are, then
**ask the user how they want them**:

- the newest few (`direction: "newest_first"`, small `max_matches`)
- the oldest few (`direction: "oldest_first"`)
- a narrower filter to cut the number down
- everything written to a file (`output_file: "matches.jsonl"`), which returns
  a path instead of flooding the conversation

Never print thousands of messages into the chat.

### 8. Show the message

Search results truncate values. Once a match is located, call `get_message`
with its topic, partition and offset for the full message, and set `context` to
show surrounding messages when the user is debugging ordering or a stuck
consumer.

Always report partition and offset alongside the content: that is what
identifies the message for any follow-up.

## Notes

- Values that are not valid UTF-8 are returned base64 encoded, with `encoding`
  set to `base64`. Say so rather than showing base64 as if it were text.
- `message_count` from `describe_topic` counts offsets, so it can overcount
  where retention or compaction has removed records.
