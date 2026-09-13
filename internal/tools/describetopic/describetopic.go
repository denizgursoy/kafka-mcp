// Package describetopic implements the describe_topic MCP tool.
package describetopic

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/denizgursoy/kafka-mcp/internal/domain/records"
)

// Input is the argument set accepted by the describe_topic tool.
type Input struct {
	Topic string `json:"topic" jsonschema:"Name of the topic to describe. Matched exactly and case-sensitively."`
}

// Partition describes one partition's offset range and size.
type Partition struct {
	Partition    int32 `json:"partition"`
	StartOffset  int64 `json:"start_offset"`
	EndOffset    int64 `json:"end_offset"`
	MessageCount int64 `json:"message_count"`
}

// Output is the result returned by the describe_topic tool.
type Output struct {
	Topic           string      `json:"topic"`
	PartitionCount  int         `json:"partition_count"`
	MessageCount    int64       `json:"message_count"`
	Partitions      []Partition `json:"partitions"`
	OldestTimestamp *time.Time  `json:"oldest_timestamp,omitempty"`
	NewestTimestamp *time.Time  `json:"newest_timestamp,omitempty"`
}

const description = `
Describe one Kafka topic: how many partitions it has, the offset range of each
partition, how many messages it holds, and the timestamps of its oldest and
newest messages.

Use this before searching a topic. It tells you how much data a search would
have to scan and which partition and offset or time range to narrow it to.

"message_count" per partition is end_offset minus start_offset. It counts
offsets rather than surviving records, so it can overcount if records were
deleted by retention or compaction. A partition whose start and end offsets are
equal is empty. The timestamps are omitted for an empty topic.

Fails if the topic does not exist.
`

// Register adds the describe_topic tool to the MCP server.
func Register(server *mcp.Server, admin *kadm.Client, reader *records.Reader) {
	mcp.AddTool(
		server,
		&mcp.Tool{
			Name:        "describe_topic",
			Description: description,
		},
		func(
			ctx context.Context,
			req *mcp.CallToolRequest,
			input Input,
		) (*mcp.CallToolResult, Output, error) {

			out, err := Run(ctx, admin, reader, input)
			if err != nil {
				return nil, Output{}, fmt.Errorf("describe topic: %w", err)
			}

			return nil, out, nil
		},
	)
}

// Run reports the partition layout, size and time span of a topic.
func Run(
	ctx context.Context,
	admin *kadm.Client,
	reader *records.Reader,
	input Input,
) (Output, error) {

	if input.Topic == "" {
		return Output{}, fmt.Errorf("topic is required")
	}

	details, err := admin.ListTopics(ctx, input.Topic)
	if err != nil {
		return Output{}, fmt.Errorf("list topic %q: %w", input.Topic, err)
	}

	detail, ok := details[input.Topic]
	if !ok {
		return Output{}, fmt.Errorf("topic %q does not exist", input.Topic)
	}

	if detail.Err != nil {
		return Output{}, fmt.Errorf("topic %q: %w", input.Topic, detail.Err)
	}

	starts, err := admin.ListStartOffsets(ctx, input.Topic)
	if err != nil {
		return Output{}, fmt.Errorf("list start offsets for %q: %w", input.Topic, err)
	}

	if err := starts.Error(); err != nil {
		return Output{}, fmt.Errorf("list start offsets for %q: %w", input.Topic, err)
	}

	ends, err := admin.ListEndOffsets(ctx, input.Topic)
	if err != nil {
		return Output{}, fmt.Errorf("list end offsets for %q: %w", input.Topic, err)
	}

	if err := ends.Error(); err != nil {
		return Output{}, fmt.Errorf("list end offsets for %q: %w", input.Topic, err)
	}

	out := Output{
		Topic:          input.Topic,
		PartitionCount: len(detail.Partitions),
		Partitions:     make([]Partition, 0, len(detail.Partitions)),
	}

	for id := range detail.Partitions {
		start, hasStart := starts.Lookup(input.Topic, id)
		end, hasEnd := ends.Lookup(input.Topic, id)

		if !hasStart || !hasEnd {
			continue
		}

		count := end.Offset - start.Offset
		if count < 0 {
			count = 0
		}

		out.Partitions = append(out.Partitions, Partition{
			Partition:    id,
			StartOffset:  start.Offset,
			EndOffset:    end.Offset,
			MessageCount: count,
		})

		out.MessageCount += count
	}

	// kadm returns maps, and Go map iteration order is random, so sort to keep
	// the report stable across calls.
	sort.Slice(out.Partitions, func(i, j int) bool {
		return out.Partitions[i].Partition < out.Partitions[j].Partition
	})

	if out.MessageCount == 0 {
		return out, nil
	}

	oldest, newest, err := timestampRange(ctx, admin, reader, input.Topic, out.Partitions)
	if err != nil {
		return Output{}, err
	}

	out.OldestTimestamp = oldest
	out.NewestTimestamp = newest

	return out, nil
}

// timestampRange reports the oldest and newest record timestamps in a topic.
//
// The oldest comes from the offset listing after the epoch, which carries the
// timestamp of the record it points at. The newest is read from the last
// record of each partition: ListMaxTimestampOffsets was verified to report the
// *first* record's timestamp on Redpanda, so it cannot be trusted here.
func timestampRange(
	ctx context.Context,
	admin *kadm.Client,
	reader *records.Reader,
	topic string,
	partitions []Partition,
) (*time.Time, *time.Time, error) {

	afterEpoch, err := admin.ListOffsetsAfterMilli(ctx, 0, topic)
	if err != nil {
		return nil, nil, fmt.Errorf("list oldest timestamp for %q: %w", topic, err)
	}

	var oldest, newest *time.Time

	// A partition with no records reports a timestamp of -1, which must not be
	// mistaken for a real point in time.
	afterEpoch.Each(func(offset kadm.ListedOffset) {
		if offset.Err != nil || offset.Timestamp < 0 {
			return
		}

		at := time.UnixMilli(offset.Timestamp).UTC()

		if oldest == nil || at.Before(*oldest) {
			oldest = &at
		}
	})

	ends := make(map[int32]int64, len(partitions))

	for _, partition := range partitions {
		if partition.MessageCount > 0 {
			ends[partition.Partition] = partition.EndOffset
		}
	}

	last, err := reader.Last(ctx, topic, ends)
	if err != nil {
		return nil, nil, fmt.Errorf("read newest record of %q: %w", topic, err)
	}

	for _, record := range last {
		at := record.Timestamp.UTC()

		if newest == nil || at.After(*newest) {
			newest = &at
		}
	}

	return oldest, newest, nil
}
