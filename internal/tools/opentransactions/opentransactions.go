// Package opentransactions implements the open_transactions MCP tool.
package opentransactions

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/denizgursoy/kafka-mcp/internal/domain/batch"
)

// Item is one topic to check.
type Item struct {
	Topic string `json:"topic" jsonschema:"Topic to check for open transactions. Matched exactly and case-sensitively."`
}

// Input is the argument set accepted by the open_transactions tool.
type Input struct {
	Items []Item `json:"items" jsonschema:"The topics to check, 1 to 100 of them. Checking one topic is an array of length one. Results follow this order and a missing topic is reported against its own item."`
}

// Producer is one transactional producer holding a partition back.
type Producer struct {
	ProducerID             int64  `json:"producer_id"`
	ProducerEpoch          int16  `json:"producer_epoch"`
	TransactionStartOffset int64  `json:"transaction_start_offset"`
	LastProducedAt         string `json:"last_produced_at,omitempty"`

	// The fields below come from the transaction coordinator, and are only
	// present when the producer's transactional id could be found.
	TransactionalID string `json:"transactional_id,omitempty"`
	State           string `json:"state,omitempty"`
	StartedAt       string `json:"started_at,omitempty"`
	OpenFor         string `json:"open_for,omitempty"`
	TimeoutMillis   *int32 `json:"timeout_ms,omitempty"`
}

// Partition is one partition whose stable offset trails its end.
type Partition struct {
	Partition          int32      `json:"partition"`
	LastStableOffset   int64      `json:"last_stable_offset"`
	HighWatermark      int64      `json:"high_watermark"`
	UnreadableMessages int64      `json:"unreadable_messages"`
	Producers          []Producer `json:"producers"`
	Error              string     `json:"error,omitempty"`
}

// Output is the result of one item.
type Output struct {
	Topic      string      `json:"topic"`
	Blocked    bool        `json:"blocked"`
	Partitions []Partition `json:"partitions"`
	Warnings   []string    `json:"warnings"`
}

type BatchOutput = batch.Output[Output]

const description = `
Find open transactions holding back read_committed consumers on 1 to 100 topics
in one call through items. A consumer with isolation.level=read_committed
cannot read past the first message of a transaction that has not been committed
or aborted, so a transactional producer that hangs or dies mid-transaction
stalls every such consumer on that partition. In consumer_lag this looks
exactly like a poison message: members present, nothing consumed.

For each topic, blocked says whether any partition's last stable offset trails
its high watermark. Each such partition reports both offsets, how many messages
read_committed consumers cannot see, and every producer with an open
transaction there: producer id and epoch, the offset the transaction started
at, and where the coordinator knows it, the transactional id (which names the
application), state, when it started, how long it has been open and its
timeout, after which the broker aborts it.

The fix is in the producer, not the consumer: restart or fence the producing
application, or wait for the timeout. Skipping offsets does not help. Results
follow items order, each carrying index with result or error. Needs DESCRIBE on
the topic and on transactional ids.
`

// Register adds the open_transactions tool to the MCP server.
func Register(server *mcp.Server, admin *kadm.Client) {
	mcp.AddTool(
		server,
		&mcp.Tool{
			Name:        "open_transactions",
			Description: description,
		},
		func(
			ctx context.Context,
			req *mcp.CallToolRequest,
			input Input,
		) (*mcp.CallToolResult, BatchOutput, error) {

			out, err := Run(ctx, admin, input)
			if err != nil {
				return nil, BatchOutput{}, fmt.Errorf("open transactions: %w", err)
			}

			return nil, out, nil
		},
	)
}

// Run checks every requested topic with bounded concurrency.
func Run(ctx context.Context, admin *kadm.Client, input Input) (BatchOutput, error) {
	return batch.Run(ctx, input.Items, batch.MaxItems, func(ctx context.Context, item Item) (Output, error) {
		return check(ctx, admin, item.Topic)
	})
}

func check(ctx context.Context, admin *kadm.Client, topic string) (Output, error) {
	if topic == "" {
		return Output{}, fmt.Errorf("topic is required")
	}

	details, err := admin.ListTopics(ctx, topic)
	if err != nil {
		return Output{}, fmt.Errorf("list topic %q: %w", topic, err)
	}

	detail, ok := details[topic]
	if !ok || errors.Is(detail.Err, kerr.UnknownTopicOrPartition) {
		return Output{}, fmt.Errorf("topic %q does not exist", topic)
	}

	if detail.Err != nil {
		return Output{}, fmt.Errorf("topic %q: %w", topic, detail.Err)
	}

	// The last stable offset is what read_committed consumers can reach; the
	// high watermark is what exists. A gap between them is an open
	// transaction, so this pair is the whole detection.
	stable, err := admin.ListCommittedOffsets(ctx, topic)
	if err != nil {
		return Output{}, fmt.Errorf("list last stable offsets for %q: %w", topic, err)
	}

	ends, err := admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return Output{}, fmt.Errorf("list end offsets for %q: %w", topic, err)
	}

	out := Output{Topic: topic, Partitions: []Partition{}, Warnings: []string{}}

	blocked := kadm.TopicsSet{}

	for _, id := range detail.Partitions.Numbers() {
		lso, okStable := stable.Lookup(topic, id)
		hw, okEnd := ends.Lookup(topic, id)

		if !okStable || !okEnd || lso.Err != nil || hw.Err != nil {
			out.Partitions = append(out.Partitions, Partition{
				Partition: id, LastStableOffset: -1, HighWatermark: -1, Producers: []Producer{},
				Error: "offsets unavailable, so this partition could not be checked",
			})

			continue
		}

		if lso.Offset >= hw.Offset {
			continue
		}

		out.Partitions = append(out.Partitions, Partition{
			Partition:          id,
			LastStableOffset:   lso.Offset,
			HighWatermark:      hw.Offset,
			UnreadableMessages: hw.Offset - lso.Offset,
			Producers:          []Producer{},
		})

		blocked.Add(topic, id)
	}

	sort.Slice(out.Partitions, func(i, j int) bool { return out.Partitions[i].Partition < out.Partitions[j].Partition })

	for _, partition := range out.Partitions {
		if partition.Error == "" {
			out.Blocked = true
		}
	}

	if !out.Blocked {
		return out, nil
	}

	if err := producers(ctx, admin, topic, blocked, &out); err != nil {
		// The offsets already prove the topic is blocked; failing to name the
		// producer narrows the answer rather than invalidating it.
		out.Warnings = append(out.Warnings, fmt.Sprintf(
			"the producers holding these transactions could not be described: %v", err))
	}

	return out, nil
}

// producers fills in who holds each blocked partition, joining the partition
// leader's view (producer id, start offset) with the coordinator's
// (transactional id, state, age).
func producers(ctx context.Context, admin *kadm.Client, topic string, blocked kadm.TopicsSet, out *Output) error {
	described, err := admin.DescribeProducers(ctx, blocked)
	if err != nil {
		return fmt.Errorf("describe producers: %w", err)
	}

	transactions, err := transactionsByProducer(ctx, admin)
	if err != nil {
		out.Warnings = append(out.Warnings, fmt.Sprintf(
			"transactional ids could not be listed, so producers are reported by id only: %v", err))
	}

	now := time.Now()

	for index := range out.Partitions {
		partition := &out.Partitions[index]

		if partition.Error != "" {
			continue
		}

		detail, ok := described[topic].Partitions[partition.Partition]
		if !ok {
			continue
		}

		if detail.Err != nil {
			partition.Error = fmt.Sprintf("producers could not be described: %v", detail.Err)

			continue
		}

		for _, active := range detail.ActiveProducers.Sorted() {
			// Idempotent producers without a transaction are listed too, with
			// no start offset. They hold nothing back.
			if active.CurrentTxnStartOffset < 0 {
				continue
			}

			producer := Producer{
				ProducerID:             active.ProducerID,
				ProducerEpoch:          active.ProducerEpoch,
				TransactionStartOffset: active.CurrentTxnStartOffset,
			}

			if active.LastTimestamp > 0 {
				producer.LastProducedAt = time.UnixMilli(active.LastTimestamp).UTC().Format(time.RFC3339)
			}

			if transaction, ok := transactions[active.ProducerID]; ok {
				producer.TransactionalID = transaction.TxnID
				producer.State = transaction.State

				if transaction.StartTimestamp > 0 {
					started := time.UnixMilli(transaction.StartTimestamp)
					producer.StartedAt = started.UTC().Format(time.RFC3339)
					producer.OpenFor = now.Sub(started).Round(time.Second).String()
				}

				if transaction.TimeoutMillis > 0 {
					timeout := transaction.TimeoutMillis
					producer.TimeoutMillis = &timeout
				}
			}

			partition.Producers = append(partition.Producers, producer)
		}
	}

	return nil
}

// transactionsByProducer describes every transaction that is not finished,
// keyed by producer id, which is the only field the partition leader and the
// coordinator have in common.
func transactionsByProducer(ctx context.Context, admin *kadm.Client) (map[int64]kadm.DescribedTransaction, error) {
	listed, err := admin.ListTransactions(ctx, nil, []string{"Ongoing", "PrepareCommit", "PrepareAbort"})
	if err != nil {
		return nil, err
	}

	if len(listed) == 0 {
		return map[int64]kadm.DescribedTransaction{}, nil
	}

	ids := make([]string, 0, len(listed))
	for id := range listed {
		ids = append(ids, id)
	}

	sort.Strings(ids)

	described, err := admin.DescribeTransactions(ctx, ids...)
	if err != nil {
		return nil, err
	}

	out := make(map[int64]kadm.DescribedTransaction, len(described))
	for _, transaction := range described {
		if transaction.Err == nil {
			out[transaction.ProducerID] = transaction
		}
	}

	return out, nil
}
