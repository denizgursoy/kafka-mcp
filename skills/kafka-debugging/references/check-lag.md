# Check lag

Report whether a topic's consumers are behind, how fast messages move, and when
the backlog will clear.

Use this when the user asks "is there lag on orders", "how far behind is the
payments consumer", "how fast are we processing", or "when will it catch up".

## Tools

| Tool                   | Use it for                                          |
| ---------------------- | --------------------------------------------------- |
| `list_topics`          | Finding the topic when the user does not name one   |
| `list_consumer_groups` | Finding who consumes the topic, and their state     |
| `describe_consumer_group` | Which member owns a lagging partition            |
| `delete_consumer_group` | Removing an abandoned group whose lag nobody drains |
| `consumer_lag`         | Lag, produce and consume rates, and the estimate    |
| `describe_topic`       | The topic's configuration and size                  |

## Steps

### 1. Establish the topic

If the user named a topic, use it. Otherwise call `list_topics`. Do not guess
between several plausible names: ask.

### 2. Find the consumers

Call `list_consumer_groups` with the topic. Read the **state** of each group
before anything else, because it decides how the numbers should be read:

- **Stable** — members are consuming, so lag should be moving
- **Empty** — no members. The group may still report lag, because committed
  offsets outlive the consumers that made them. Nothing will drain it.
- **PreparingRebalance** — members are joining or leaving, so a rate sampled
  now is unreliable. If it keeps coming back, see
  [unstable-consumer-group](unstable-consumer-group.md)
- **Dead** — the group is gone

If no group consumes the topic, say so. A topic nobody consumes has no lag, and
that is a different answer from "the consumers are keeping up".

### 3. Measure

Call `consumer_lag` with one `items` entry per topic, and the group on the item
if the user named one. Comparing several topic/group pairs is the same call with
more entries. Up to 4 sampling windows run at a time, so the call takes about
`sample_seconds` for every 4 entries. Read each item's own result.

The call **blocks for `sample_seconds`** (default 5), because Kafka stores no
history of consumption: the only way to learn the consume rate is to read the
committed offset, wait, and read it again. Tell the user you are sampling if
they are waiting on the answer. Use `skip_consume_rate: true` when they only
want the lag, and `sample_seconds: 30` when traffic is bursty and a five second
window would be noisy.

### 4. Report the lag

Give the total, then the per-partition breakdown when lag is uneven, since one
badly lagging partition is a different problem from a uniformly slow consumer.
Uneven lag has its own guide: [partition-skew](partition-skew.md).

**Check `offset_expired` first.** It means the committed offset is below the
partition's `start_offset`: retention already deleted the group's position. The
lag number is then not a backlog. The next time the consumer fetches, it resets
by its `auto.offset.reset` (to the start or the end), and the messages between
the commit and the start are gone. The group carries a warning saying so. Report
it ahead of everything else, because the fix is a decision about where the
group should resume ([replay-messages](replay-messages.md)), not more
throughput.

A partition carrying an `error` has **unknown** lag, not zero. Never fold it
into a total as though it were caught up.

### 5. Report the rates honestly

The two rates are not measured the same way, and presenting them as equivalent
misleads the user:

- **`produce_rate`** is historical fact, measured from message timestamps over
  the last second, minute and hour. Compare the windows: an hourly average far
  below the last minute means traffic is ramping up, and a current estimate
  will age badly.
- **`consume_rate`** is a short sample extrapolated to per-minute and per-hour
  figures. Say it is a sample. If `sample_inconclusive` is true, nothing moved
  during the window: report that as "no progress observed", not as a measured
  throughput of zero.

If `window_truncated` is set, the topic is younger than the window and the rate
covers the topic's whole life instead. Mention it rather than quoting an hourly
rate for a topic that is minutes old.

### 6. Answer "when will it clear" from the status, not by dividing

**Never compute `lag ÷ consume_rate` yourself.** The backlog drains at the
consume rate *minus* the produce rate, because producers keep adding to it. The
tool has already done this. Read `status`:

| Status | What to say |
|---|---|
| `caught_up` | There is no lag. |
| `draining` | Quote `eta_human` and `eta_at`, and note it assumes the current rates hold. |
| `growing` | **The lag will never clear at these rates.** Give `growing_by_per_minute` and say consumers cannot keep up. |
| `stalled` | Members are present but consuming nothing. Point at the consumer, not at Kafka. |
| `no_active_consumers` | The group has no members. The lag is real but nothing is draining it. |
| `not_measured` | The rate was not sampled, so no estimate exists. Offer to measure. |

An estimate is only ever offered when the lag is genuinely shrinking. When it
is not, naming the reason is the useful answer: a completion time that will
never arrive is worse than none.

### 7. When the backlog is large, check retention

If the lag is big or growing, call `consumer_lag` again with
`measure_backlog_age: true`. For each partition it reads the next unconsumed
message and reports `committed_timestamp` and `lag_seconds`. Each group gets
`oldest_unconsumed_at`, and the topic gets `retention_ms`. `retention_risk` is
true once the oldest unconsumed message has used half of the retention. Then
those messages will be **deleted before they are ever consumed** unless the
group speeds up. That is data loss, and it is worth raising unprompted, with
the age and the retention side by side.

## Notes

- Lag is counted in messages. `measure_backlog_age` turns it into time. A lag of
  10,000 tiny messages and 10,000 large ones are very different amounts of work.
- Rates are per topic, so a consumer group reading several topics may be busy
  elsewhere.
- A group can be caught up on one partition and far behind on another; the
  total alone can hide that. `describe_consumer_group` names the member and host
  owning the lagging partition, which is usually one slow instance rather than a
  slow application.
- A `no_active_consumers` group whose consumers were decommissioned keeps
  reporting lag forever, and keeps firing whatever alert watches it. If the user
  confirms nothing will use that group id again, `delete_consumer_group` removes
  it; see [purge-messages.md](purge-messages.md). Never delete a group just
  because it is `Empty`: an `Empty` group is often a consumer that is only
  stopped for a deploy.
