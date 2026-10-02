# Cluster health

Find out whether the cluster itself is the problem: brokers down, partitions
without a leader, replicas out of sync.

Use this when the user says "producers fail with NOT_ENOUGH_REPLICAS", "we get
LEADER_NOT_AVAILABLE", "one partition can't be read", "a broker went down, is
anything affected", or "is the cluster healthy".

## Tools

| Tool             | Use it for                                                  |
| ---------------- | ----------------------------------------------------------- |
| `server_config`  | Which cluster this endpoint reaches                         |
| `cluster_health` | Brokers, controller, and every unhealthy partition          |
| `describe_topic` | `min.insync.replicas` and the rest of one topic's config    |
| `consumer_lag`   | Whether consumers on an affected topic have stopped moving  |

## Steps

### 1. Confirm which cluster

Call `server_config` when there is any doubt. A healthy report about the wrong
cluster is worse than none.

### 2. Check the cluster

Call `cluster_health`. Narrow it with `search` when the user named a topic; leave
it whole when they asked about the cluster. Set `include_internal` when
**every** consumer group is failing at once: a broken `__consumer_offsets` does
that, and it is skipped by default.

Read the top first:

- **`controller` is -1** — the cluster has no controller. Nothing can create
  topics, elect leaders or reassign partitions. This outranks everything else.
- **`brokers`** — compare with how many the user expects. A missing broker is
  the usual root cause of everything below.
- **`leaders` per broker** — a broker leading zero partitions while others lead
  many has usually restarted and not been given leadership back. Traffic is
  concentrated on the others.

### 3. Read each problem by its issue

| Issue | What the user sees | What it means |
| ----- | ------------------ | ------------- |
| `offline` | `LEADER_NOT_AVAILABLE`, reads and writes hang or fail | No leader. Nothing can use the partition until a replica comes back |
| `under_min_isr` | Producers with `acks=all` fail with `NOT_ENOUGH_REPLICAS` | Fewer in-sync replicas than `min_insync_replicas`. Reads still work |
| `under_replicated` | Usually nothing yet | A replica is down or behind. One more failure may take the partition offline |
| `error` | Depends on the error | The broker returned an error for that partition; quote it |

Group the problems by the replica that is missing from `isr`. When every problem
shares one broker id, that broker is the cause, and saying so is the answer.

### 4. Say what was not checked

If `min_isr_unknown` lists topics, the broker did not report
`min.insync.replicas` for them, so `under_min_isr` was not judged there. Say so
rather than reporting those topics as fine; Redpanda in particular does not
report it.

If `healthy` is true and the user still sees errors, the cluster is not the
problem. Say that plainly and look at the client: its configuration,
authorization ([authorization-error.md](authorization-error.md)), or a hung
transaction (`open_transactions`, see
[skip-poison-message.md](skip-poison-message.md)).

## Notes

- This tool reads metadata only and changes nothing. Recovering a broker,
  electing leaders or reassigning replicas is cluster administration outside
  this server.
- Lowering a topic's `min.insync.replicas` to make producers succeed again
  trades durability for availability. It is possible with `alter_topic_config`,
  but it is the user's decision to make, with that trade said out loud, and it
  should be reverted once the broker is back.
- Metadata is a snapshot. A partition recovering from a broker restart moves
  through `offline` and `under_replicated` within seconds; call again before
  concluding a problem is persistent.
