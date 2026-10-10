# Schema error

Work out what is wrong when a schema change breaks producers or consumers, and
check a schema change before it ships.

Use this when the user says "the producer fails with 409 incompatible schema",
"is this schema change safe", "consumers crash with a deserialization error
since the deploy", "which version of the schema are producers using", or
"decode_error on the messages".

## Tools

| Tool              | Use it for                                                      |
| ----------------- | --------------------------------------------------------------- |
| `get_schema`      | The subject's versions, its compatibility level, and testing a candidate |
| `sample_messages` | Which schemas recent messages were written with                 |
| `search_messages` | When a schema first appeared, and how many messages carry it    |
| `get_message`     | One message decoded, with its `schema_id` and any `decode_error` |
| `server_config`   | Whether this cluster has a `schema_registry` at all             |

## Steps

### 1. Establish the subject

The subject for a topic's values is usually `<topic>-value`, and for keys
`<topic>-key`. If `get_schema` reports the subject does not exist, the
registry may use a different naming strategy. Ask rather than guess.

If `server_config` shows no `schema_registry`, schema-encoded messages appear
as base64 with a `decode_error`, and none of the steps below can run here.

### 2. For "is this change safe": test it

Call `get_schema` with the subject and `check_schema: {schema: "<candidate>"}`.
Add `type` for Protobuf or JSON Schema. The candidate is checked against the
latest version under the subject's `compatibility` level and is **not
registered**.

- **`check.compatible: true`** — the registry will accept it. Say which level
  it was judged under: `BACKWARD` means new consumers can read old data, not
  that old consumers can read new data.
- **`check.compatible: false`** — quote `check.messages`. They name the field
  and the rule broken (for example a type changed, or a field removed without a
  default), which is exactly what the developer has to fix.

The level matters as much as the answer:

| `compatibility` | What it guarantees | Deploy order |
| --------------- | ------------------ | ------------ |
| `BACKWARD` | New schema reads data written with the previous one | Consumers first |
| `FORWARD` | Previous schema reads data written with the new one | Producers first |
| `FULL` | Both | Either |
| `*_TRANSITIVE` | The same, against every version, not only the last | As above |
| `NONE` | Nothing; any change is accepted | Coordinate by hand |

### 3. For "producers get 409": the same test, with the schema they send

A 409 from the registry is the registration being refused. Run step 2 with
the schema the producer is trying to register. The messages say why. The fix
is a compatible change, or a deliberate change of the subject's level by
whoever owns the registry. This server does not change levels.

### 4. For "consumers break since the deploy": find the new schema

Call `sample_messages` on the topic. `schemas` lists the `schema_id`s recent
messages carry, with counts. A new id beside the old one means producers
changed schema.

Then find when it started and how widespread it is:

- `search_messages` with `script: "return schema_id === <new id>"`,
  `direction: "oldest_first"` and `max_matches: 1` returns the first message
  written with it. Its timestamp is when the producer changed.
- `search_messages` with `group_by: "return schema_id"` over the incident
  window counts messages per schema.
- `get_schema` with `id: <new id>` shows the schema text and, in `used_by`,
  which subject version it is.

Compare it with the version the consumer was built against. A consumer
deployed with an older generated class fails on a field it does not know only
when the change was not forward compatible. Step 2 against the old version
explains which.

### 5. For `decode_error`: say why decoding failed

`decode_error` on a message means this server could not decode it either. It
usually means a `schema_id` the registry does not have, which happens when data
was copied from another cluster without `translate_schema`. It can also mean
bytes that only look like the registry framing. Report the reason as given
rather than showing the base64 as if it were the content.

## Notes

- `get_schema` reads only. Registering, deleting or changing a subject's level
  is the registry owner's job.
- Avro compatibility is judged on the schema, not on the data: a "compatible"
  schema can still break a consumer that relies on a field's meaning.
- To skip messages already written with a bad schema, see
  [skip-poison-message](skip-poison-message.md). Fix the producer first, or new
  bad messages keep arriving.
