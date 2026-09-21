---
name: kafka-debugging
description: Use when debugging a Kafka cluster through the kafka-mcp server — finding a message by order id, correlation id, key or a field condition; measuring consumer lag, throughput and when a backlog will clear; unblocking a consumer stuck on a poison message; or adding partitions to a topic. Use for requests like "find the message for order 12345", "which message had this correlation id", "is there lag on orders", "how far behind is this consumer group", "how fast are we consuming", "the consumer is stuck", "skip this bad message", "add partitions to this topic", or "when will the backlog clear". Routes to the guide for the scenario, so read this before calling the tools.
---

# Kafka debugging

Four scenarios, one per guide. Read the guide for the scenario before calling
any tool: each one exists because the obvious sequence of calls gets the answer
wrong in a specific way.

## Pick the guide

| The user is asking | Guide |
| ------------------ | ----- |
| Where a message is, by id, key or a field condition | [find-message.md](find-message.md) |
| Whether consumers are behind, how fast, when it clears | [check-lag.md](check-lag.md) |
| Why a consumer is stuck, and how to get it moving | [skip-poison-message.md](skip-poison-message.md) |
| Whether to add partitions, and doing it safely | [scale-partitions.md](scale-partitions.md) |

## When "the consumer is behind" is ambiguous

Three of these guides answer the same opening complaint, and picking between
them by wording is guesswork. Call `consumer_lag` first and let `status` decide:

| `status` | What it means | Where to go |
| -------- | ------------- | ----------- |
| `caught_up` | There is no lag | Answer and stop |
| `draining` | Working, just slower than the user hoped | [check-lag.md](check-lag.md) for the ETA |
| `growing` | Consumers cannot keep up at all | [scale-partitions.md](scale-partitions.md) |
| `stalled` | Members present, consuming nothing | [skip-poison-message.md](skip-poison-message.md) |
| `no_active_consumers` | Nobody is running | Say so: starting a consumer is the fix |

Adding partitions to a `stalled` group, or skipping a message from a `growing`
one, is the common way this goes wrong. One destroys data for a capacity
problem; the other adds capacity to a consumer that is not consuming.

## Rules that apply to every guide

**One endpoint is one cluster.** The server serves each cluster on its own path
under `/mcp/`, and every tool is bound to the cluster of the endpoint it was
called on. No tool takes a cluster parameter, except `copy_message`, which
chooses a destination, and `list_clusters`, which reports the roster.

**Check `server_config` before promising a change.** A read-only endpoint does
not expose `add_partitions` or `commit_offset` at all, so there is no refusal
to discover and no preview to fall back on. Find out at the start, not after
the user has already stopped their consumers.

**Never guess a topic.** If the user did not name one, call `list_topics` and
ask when several are plausible. Answering confidently about the wrong topic is
worse than one more question.

**Report partition and offset with any message.** That triple is what
identifies a message for every follow-up call; content alone does not.

**Say what was not measured.** An incomplete scan, an unsampled rate, a
partition returning an error — each is a different answer from "nothing there",
and reporting it as absence is how these investigations produce confident wrong
conclusions.
