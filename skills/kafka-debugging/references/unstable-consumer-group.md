# Unstable consumer group

Find out why a consumer group keeps rebalancing, and which consumer is causing
it.

Use this when the user says "the consumer keeps rebalancing", "lag spikes every
few minutes", "members keep getting kicked out", "the group is always in
PreparingRebalance", or "consumption stops and starts".

## Tools

| Tool                      | Use it for                                              |
| ------------------------- | ------------------------------------------------------- |
| `describe_consumer_group` | Watching the group with `sample_seconds`: states, members joining and leaving |
| `consumer_lag`            | Whether the group falls behind while it rebalances      |

## Steps

### 1. Confirm it is a group problem

Call `consumer_lag` on the topic with the group. A group that rebalances often
shows lag that climbs and drops, a consume rate that is `sample_inconclusive`
in one call and healthy in the next, and a state other than `Stable` some of
the time. A group that is simply `stalled` with the same members throughout is
not this problem: see [skip-poison-message](skip-poison-message.md).

### 2. Watch the group

Call `describe_consumer_group` with the group and `sample_seconds`. Use 30 to
start, and up to 60 for a group that rebalances every few minutes. The call
blocks that long, so tell the user you are watching.

Read `observation`:

- **`states`** — every state the group passed through, in order. `Stable` alone
  means nothing changed during the window. `Stable, PreparingRebalance,
  CompletingRebalance, Stable` is one rebalance. Several of those cycles in one
  window is a storm.
- **`left`** — members that disappeared, with `client_id` and `host`. This is
  the most useful field. The same `client_id` or `host` leaving repeatedly
  points at one consumer.
- **`joined`** — members that appeared. A member that restarts gets a new
  `member_id`, so it shows in both `left` and `joined` with the same host.
- **`unstable`** — true when anything changed.

If nothing changed, say so and widen the window or call again later. A clean
window does not prove the group is healthy, only that it was stable for those
seconds.

### 3. Name the likely cause from the pattern

The tool shows what happened. Why it happened lives in the consumer's
configuration and logs, so give the user the pattern and the place to look:

| Pattern | Usual cause | Where to look |
| ------- | ----------- | ------------- |
| One host leaves and rejoins over and over | That instance crashes or is restarted (OOM, liveness probe) | That pod's restarts and logs |
| Members leave after processing a slow batch | Processing takes longer than `max.poll.interval.ms`, so the consumer is evicted | Batch processing time, `max.poll.records`, `max.poll.interval.ms` |
| Members leave with no restart | Heartbeats stop: `session.timeout.ms` too short for GC pauses or network blips | GC logs, `session.timeout.ms`, `heartbeat.interval.ms` |
| Every rolling deploy causes a storm | Each restarting pod triggers a full rebalance | Static membership (`group.instance.id`), cooperative assignor |
| Members join and leave in rapid succession | An autoscaler adding and removing replicas | The autoscaler's scale events |

`instance_id` on a member means static membership is in use. A restart within
`session.timeout.ms` then rejoins without a rebalance, which is why rolling
deploys stay quiet with it. If the group has no `instance_id` and deploys cause
the storms, suggesting it is the fix.

### 4. Do not fix capacity during a storm

A rebalancing group consumes in bursts, so its lag and rates look like a
capacity problem. Adding partitions or consumers during a storm adds members to
an unstable group and usually makes it worse. Settle the group first, then
measure again with `consumer_lag`.

## Notes

- Classic group metadata carries no rebalance count or generation id. Churn is
  detected from member ids and states seen once a second, so a rebalance that
  completes between two reads can be missed. Repeated watching catches a storm
  even when one window misses a cycle.
- Members of a KIP-848 (new protocol) group rebalance incrementally, so `states`
  may stay `Stable` while `joined` and `left` still show churn.
- This server cannot change consumer configuration or restart consumers. The
  fix is always in the consuming application.
