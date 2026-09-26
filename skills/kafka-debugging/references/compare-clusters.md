# Compare two clusters

Find out how two environments differ: which topics one has and the other does
not, and which topics exist on both but are not the same topic in practice.

Use this when the user says "what does preprod have that prod does not", "are
these two environments the same", "which topics are missing", "why does this
work in preprod but not in prod", or asks to create the topics one environment
is missing.

## Tools

| Tool               | Use it for                                              |
| ------------------ | -------------------------------------------------------- |
| `list_clusters`    | Which cluster names are valid, and which accept writes   |
| `compare_clusters` | The difference itself: missing topics and config drift   |
| `describe_topic`   | The full configuration of one topic, when drift needs detail |
| `server_config`    | Checking this endpoint may create anything, before promising it |
| `create_topic`     | Creating the topics that are missing                     |

## Steps

### 1. Establish which two clusters, and which direction

Call `list_clusters` to see the names this server serves. The comparison runs
against the cluster **this endpoint** serves, so the endpoint decides one side
and `cluster` decides the other.

Direction is not a detail. `only_here` and `only_there` mean opposite things,
and a report read backwards leads to creating topics in the wrong environment.
The response names both sides; use those names when reporting, rather than
"here" and "there".

### 2. Narrow the comparison

Whole-cluster comparisons are long and mostly uninteresting. Use `search` to
limit the report to what the user is actually asking about — one team's prefix,
one product area.

Internal topics are excluded unless `include_internal` is set. They exist on
every cluster and are never a difference worth acting on.

### 3. Read the three answers separately

The response splits the difference three ways, and they call for different
responses:

- **`only_there`** — topics the other cluster has and this one lacks. These are
  the candidates for creation, and each carries its partition count,
  replication factor and explicitly-set configs.
- **`only_here`** — topics this cluster has and the other lacks. Often the more
  interesting half: a topic in preproduction that never reached production is
  usually either unfinished work or something abandoned.
- **`differing`** — topics both clusters hold that do not match. `differences`
  names what disagrees, and `here`/`there` give both values.

`in_both` is a count rather than a list, because two environments commonly share
hundreds of identical topics.

### 4. Treat drift as the more important finding

A missing topic is obvious. Drift is the thing that wastes an afternoon:

- **A different partition count** changes consumer parallelism and which
  partition a key lands on. This is the usual reason a bug reproduces in one
  environment and not the other, and why an ordering problem appears only in
  production.
- **A different `retention.ms`** means the two environments hold data for
  different lengths of time. A message still present in preproduction may be
  long gone in production.
- **A different `cleanup.policy`** is the sharpest one. `compact` keeps only
  the latest value per key, so one side silently discards history the other
  keeps.

Only configs a topic sets for itself are compared. Two clusters may carry
different broker defaults, and comparing inherited values would report every
topic as different. If the user suspects a default differs, call
`describe_topic` on both sides and compare `is_default` entries directly.

### 5. Do not assume a difference is a mistake

This is the step most worth slowing down for. A topic missing from production
is frequently **deliberate**: an experiment that was never promoted, a
deprecated feature, a topic belonging to a team that does not deploy there.

Present the difference and ask which of them should exist. Never create topics
in production merely because preproduction has them. Say plainly that the tool
reports difference, not correctness.

### 6. Create what the user chose, if anything

`compare_clusters` changes nothing. Creating is `create_topic`, on an endpoint
bound to the cluster that needs them.

Check `server_config` first: a read-only endpoint does not expose `create_topic`
at all, so there is nothing to fall back on. The endpoint that needs the topics
is the one that must be writable — comparing from a read-only session is fine,
creating from it is not.

Pass the chosen entries from `only_there` as `create_topic` items, keeping the
partition count and configs the report carried, so the new topic matches the one
it was copied from rather than the broker's defaults. Preview the batch first,
and note that a partition count can never be reduced afterwards; see
[create-topic.md](create-topic.md).

Check the broker counts in the report before copying a replication factor. A
`replication_factor` of 3 cannot be created on a one-broker cluster, and
`compare_clusters` warns when the two sides differ in size.

## Notes

- The comparison reads metadata only. It never reads messages, and it reports no
  broker addresses or credentials.
- Comparing a cluster with itself is allowed and reports no differences, which
  is a cheap way to confirm the tool is pointed where you think it is.
- The replication factor reported is that of the topic's first partition. Kafka
  allows partitions of one topic to differ after a reassignment, so treat it as
  the value `create_topic` would reproduce rather than a property of every
  partition.
- A topic that cannot be read on either side fails that item rather than being
  reported as absent. "Missing" and "unreadable" are different answers, and
  conflating them would invite creating a topic that already exists.
- An unreachable cluster fails its own item; other items in the batch still
  return.
- **A freshly created topic can still be reported as missing.** Topic listings
  come from the Kafka client's metadata cache, which is a few seconds old, so a
  comparison run immediately after `create_topic` may not see what was just
  created. Wait a moment and compare again rather than creating it twice — the
  second `create_topic` would be refused, but the report would have already
  misled the user.
