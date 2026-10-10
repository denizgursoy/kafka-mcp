// Package deleterecords implements the delete_records MCP tool.
package deleterecords

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
	"github.com/denizgursoy/kafka-mcp/internal/domain/kafkaclient"
)

const toolName = "delete_records"

// Item is one partition to truncate.
type Item struct {
	Topic               string     `json:"topic" jsonschema:"Topic to delete records from. Matched exactly and case-sensitively."`
	Partition           int32      `json:"partition,omitempty" jsonschema:"Partition to delete records from. Ignored with all_partitions."`
	AllPartitions       bool       `json:"all_partitions,omitempty" jsonschema:"Optional. With before_timestamp, cut every partition of the topic at that moment; each partition gets its own resolved offset, reported in partitions. Partitions with nothing older are left alone."`
	BeforeOffset        int64      `json:"before_offset,omitempty" jsonschema:"Every message with an offset lower than this is deleted; the message at before_offset is kept and becomes the first readable one. Must be greater than the partition's current start offset and at most its end offset. Use the end offset to empty the partition. Give before_offset or before_timestamp, not both."`
	BeforeTimestamp     *time.Time `json:"before_timestamp,omitempty" jsonschema:"Optional RFC3339 time. Every message written before it is deleted: the cut is resolved per partition to the first offset at or after this time, and the preview shows the offset it resolved to. Give before_offset or before_timestamp, not both."`
	AcknowledgeDataLoss bool       `json:"acknowledge_data_loss,omitempty" jsonschema:"Optional. Required to apply. Deleted records cannot be restored by Kafka, so confirm alone is not enough."`
}

// Input is the argument set accepted by the delete_records tool.
type Input struct {
	Items   []Item `json:"items" jsonschema:"The partitions to truncate, 1 to 100 of them. Truncating one partition is an array of length one. Two items naming the same topic and partition are refused before anything is deleted."`
	Confirm bool   `json:"confirm,omitempty" jsonschema:"Optional. When false or omitted, nothing is deleted and the response shows how many messages each item would remove and which consumer groups have not read them yet. Must be true to delete. One confirm covers the whole batch."`
}

// AffectedGroup is a consumer group that would lose messages it has not read.
type AffectedGroup struct {
	Group           string `json:"group"`
	CommittedOffset int64  `json:"committed_offset"`
	UnprocessedLost int64  `json:"unprocessed_lost"`
}

// Cut is one partition's part of an all_partitions item.
type Cut struct {
	Partition            int32           `json:"partition"`
	StartOffset          int64           `json:"start_offset"`
	EndOffset            int64           `json:"end_offset"`
	BeforeOffset         int64           `json:"before_offset"`
	MessagesDeleted      int64           `json:"messages_deleted"`
	AffectedGroups       []AffectedGroup `json:"affected_groups"`
	ResultingStartOffset int64           `json:"resulting_start_offset,omitempty"`
}

// Output is the result of one item.
type Output struct {
	Topic                string          `json:"topic"`
	Partition            int32           `json:"partition"`
	StartOffset          int64           `json:"start_offset"`
	EndOffset            int64           `json:"end_offset"`
	BeforeOffset         int64           `json:"before_offset"`
	MessagesDeleted      int64           `json:"messages_deleted"`
	AffectedGroups       []AffectedGroup `json:"affected_groups"`
	ResultingStartOffset int64           `json:"resulting_start_offset,omitempty"`

	// AllPartitions marks an all_partitions item. Partition is then -1 and
	// the per-partition offsets live in Partitions; MessagesDeleted is their
	// total.
	AllPartitions bool  `json:"all_partitions,omitempty"`
	Partitions    []Cut `json:"partitions,omitempty"`

	WouldDelete bool     `json:"would_delete"`
	Deleted     bool     `json:"deleted"`
	Warnings    []string `json:"warnings"`
	Note        string   `json:"note,omitempty"`
}

type BatchOutput = batch.Output[Output]

const description = `
Delete the oldest messages of 1 to 100 partitions in one call through items,
without deleting the topic. Each item removes every message of one partition
with an offset lower than before_offset; the message at before_offset becomes
the first readable one. Alternatively give before_timestamp to cut at a moment,
resolved per partition, and all_partitions to apply that moment to every
partition of the topic in one item. Use it to purge test data, or a run of bad messages at
the head of a partition, while keeping the topic, its configuration and its
consumer groups. Truncating one partition is an items array of length one.

No message is deleted unless confirm is true and the item sets
acknowledge_data_loss. The preview reports the partition's start and end
offsets, how many messages would be removed, and every consumer group whose
committed offset is below the cut, with how many messages it would lose without
ever processing them. Such a group resumes from the new start offset.

Kafka cannot restore deleted records. One confirm covers the whole batch, and
deletion is not atomic: partitions truncated before a later item failed stay
truncated. Results follow items order, each carrying index with result or
error. Compacted topics are not supported by every broker. Requires Kafka
DELETE permission on the topic.
`

// Register adds the delete_records tool to the MCP server.
func Register(server *mcp.Server, kafka *kafkaclient.Client) {
	mcp.AddTool(
		server,
		&mcp.Tool{
			Name:        toolName,
			Description: description,
		},
		func(
			ctx context.Context,
			req *mcp.CallToolRequest,
			input Input,
		) (*mcp.CallToolResult, BatchOutput, error) {

			out, err := Run(ctx, kafka, input)
			if err != nil {
				return nil, BatchOutput{}, fmt.Errorf("delete records: %w", err)
			}

			return nil, out, nil
		},
	)
}

// Run previews every item before deleting from any valid one.
func Run(ctx context.Context, kafka *kafkaclient.Client, input Input) (BatchOutput, error) {
	items, confirm := input.Items, input.Confirm

	// Deleting is this tool's only purpose, so a read-only endpoint is refused
	// outright rather than offered a preview of a change it can never apply.
	if err := kafka.RequireWritable(toolName); err != nil {
		return BatchOutput{}, err
	}

	if err := batch.Validate(len(items), batch.MaxItems); err != nil {
		return BatchOutput{}, err
	}

	type target struct {
		topic     string
		partition int32
	}

	seen := make(map[target]struct{}, len(items))
	whole := make(map[string]struct{})
	for _, item := range items {
		if item.AllPartitions {
			whole[item.Topic] = struct{}{}
		}
	}
	for _, item := range items {
		if _, ok := whole[item.Topic]; ok && !item.AllPartitions {
			return BatchOutput{}, fmt.Errorf(
				"topic %q has an all_partitions item and another item: two cuts on one partition leave the result depending on order",
				item.Topic)
		}
		key := target{item.Topic, item.Partition}
		if item.AllPartitions {
			key.partition = -1
		}
		if _, ok := seen[key]; ok {
			return BatchOutput{}, fmt.Errorf(
				"duplicate target topic %q partition %d: two cuts on one partition leave the result depending on order",
				item.Topic, item.Partition)
		}
		seen[key] = struct{}{}
	}

	out, err := batch.Run(ctx, items, batch.MaxItems, func(ctx context.Context, item Item) (Output, error) {
		return preview(ctx, kafka.Admin(), item)
	})
	if err != nil || !confirm {
		return out, err
	}

	out.Succeeded, out.Failed = 0, 0

	for index, item := range items {
		result := out.Results[index]

		if result.Error != "" {
			out.Failed++

			continue
		}

		if !item.AcknowledgeDataLoss {
			out.Results[index].Result = nil
			out.Results[index].Error = fmt.Sprintf(
				"refusing to delete %d message(s) from %q (partition %d, or every partition with all_partitions): Kafka cannot restore them. Set acknowledge_data_loss true on this item if that is intended",
				result.Result.MessagesDeleted, item.Topic, item.Partition)
			out.Failed++

			continue
		}

		deleted := *result.Result

		if item.AllPartitions {
			if err := removeAll(ctx, kafka.Admin(), &deleted); err != nil {
				out.Results[index].Result = nil
				out.Results[index].Error = err.Error()
				out.Failed++

				continue
			}
		} else {
			start, err := remove(ctx, kafka.Admin(), item.Topic, item.Partition, deleted.BeforeOffset)
			if err != nil {
				out.Results[index].Result = nil
				out.Results[index].Error = err.Error()
				out.Failed++

				continue
			}
			deleted.ResultingStartOffset = start
			deleted.Note = fmt.Sprintf(
				"%q partition %d now starts at offset %d. The deleted messages cannot be restored",
				item.Topic, item.Partition, start)
		}

		deleted.Deleted = true
		deleted.WouldDelete = false

		out.Results[index].Result = &deleted
		out.Succeeded++
		out.Applied++
	}

	return out, nil
}

func preview(ctx context.Context, admin *kadm.Client, item Item) (Output, error) {
	if item.Topic == "" {
		return Output{}, fmt.Errorf("topic is required")
	}

	if item.Partition < 0 {
		return Output{}, fmt.Errorf("partition must not be negative, got %d", item.Partition)
	}

	if item.BeforeTimestamp != nil && item.BeforeOffset != 0 {
		return Output{}, fmt.Errorf("give before_offset or before_timestamp, not both: they are two different cuts")
	}

	if item.AllPartitions {
		if item.BeforeTimestamp == nil {
			return Output{}, fmt.Errorf(
				"all_partitions needs before_timestamp: an offset in one partition means nothing in another")
		}

		return previewAll(ctx, admin, item)
	}

	start, end, err := bounds(ctx, admin, item.Topic, item.Partition)
	if err != nil {
		return Output{}, err
	}

	if item.BeforeTimestamp != nil {
		cuts, err := resolveTimestamp(ctx, admin, item.Topic, *item.BeforeTimestamp)
		if err != nil {
			return Output{}, err
		}
		item.BeforeOffset = cuts[item.Partition]
		if item.BeforeOffset <= start {
			return Output{}, fmt.Errorf(
				"%q partition %d holds nothing written before %s: its first message, at offset %d, is newer, so this deletes nothing",
				item.Topic, item.Partition, item.BeforeTimestamp.UTC().Format(time.RFC3339), start)
		}
	}

	if item.BeforeOffset > end {
		return Output{}, fmt.Errorf(
			"before_offset %d is past the end of %q partition %d, which ends at %d: there are no messages there to delete. Use %d to empty the partition",
			item.BeforeOffset, item.Topic, item.Partition, end, end)
	}

	if item.BeforeOffset <= start {
		return Output{}, fmt.Errorf(
			"before_offset %d is not after the start of %q partition %d, which is %d: every message before it is already gone, so this deletes nothing",
			item.BeforeOffset, item.Topic, item.Partition, start)
	}

	affected, err := affectedGroups(ctx, admin, item.Topic, item.Partition, item.BeforeOffset)
	if err != nil {
		return Output{}, err
	}

	out := Output{
		Topic:           item.Topic,
		Partition:       item.Partition,
		StartOffset:     start,
		EndOffset:       end,
		BeforeOffset:    item.BeforeOffset,
		MessagesDeleted: item.BeforeOffset - start,
		AffectedGroups:  affected,
		WouldDelete:     true,
		Warnings:        []string{},
	}

	out.Warnings = append(out.Warnings, fmt.Sprintf(
		"%d message(s) at offsets %d to %d would be deleted, and Kafka cannot restore them",
		out.MessagesDeleted, start, item.BeforeOffset-1))

	for _, group := range affected {
		out.Warnings = append(out.Warnings, fmt.Sprintf(
			"group %q is committed at %d and would lose %d message(s) it has not processed; it resumes from %d",
			group.Group, group.CommittedOffset, group.UnprocessedLost, item.BeforeOffset))
	}

	out.Note = "nothing was deleted. Call again with confirm true and acknowledge_data_loss true on this item to delete."

	return out, nil
}

// previewAll resolves a timestamp cut for every partition of the topic.
func previewAll(ctx context.Context, admin *kadm.Client, item Item) (Output, error) {
	details, err := admin.ListTopics(ctx, item.Topic)
	if err != nil {
		return Output{}, fmt.Errorf("list topic %q: %w", item.Topic, err)
	}

	detail, ok := details[item.Topic]
	if !ok || errors.Is(detail.Err, kerr.UnknownTopicOrPartition) {
		return Output{}, fmt.Errorf("topic %q does not exist", item.Topic)
	}

	cuts, err := resolveTimestamp(ctx, admin, item.Topic, *item.BeforeTimestamp)
	if err != nil {
		return Output{}, err
	}

	out := Output{
		Topic:          item.Topic,
		Partition:      -1,
		AllPartitions:  true,
		AffectedGroups: []AffectedGroup{},
		Partitions:     []Cut{},
		WouldDelete:    true,
		Warnings:       []string{},
	}

	for _, partition := range detail.Partitions.Numbers() {
		start, end, err := bounds(ctx, admin, item.Topic, partition)
		if err != nil {
			return Output{}, err
		}

		before := cuts[partition]
		if before <= start {
			continue
		}

		affected, err := affectedGroups(ctx, admin, item.Topic, partition, before)
		if err != nil {
			return Output{}, err
		}

		out.Partitions = append(out.Partitions, Cut{
			Partition:       partition,
			StartOffset:     start,
			EndOffset:       end,
			BeforeOffset:    before,
			MessagesDeleted: before - start,
			AffectedGroups:  affected,
		})
		out.MessagesDeleted += before - start

		for _, group := range affected {
			out.Warnings = append(out.Warnings, fmt.Sprintf(
				"group %q is committed at %d on partition %d and would lose %d message(s) it has not processed",
				group.Group, group.CommittedOffset, partition, group.UnprocessedLost))
		}
	}

	if len(out.Partitions) == 0 {
		return Output{}, fmt.Errorf(
			"no partition of %q holds anything written before %s, so this deletes nothing",
			item.Topic, item.BeforeTimestamp.UTC().Format(time.RFC3339))
	}

	sort.Slice(out.Partitions, func(i, j int) bool { return out.Partitions[i].Partition < out.Partitions[j].Partition })

	out.Warnings = append([]string{fmt.Sprintf(
		"%d message(s) across %d partition(s) would be deleted, and Kafka cannot restore them",
		out.MessagesDeleted, len(out.Partitions))}, out.Warnings...)
	out.Note = "nothing was deleted. Call again with confirm true and acknowledge_data_loss true on this item to delete."

	return out, nil
}

// resolveTimestamp returns, per partition, the first offset written at or
// after at, which is the cut that deletes everything older. A partition with
// nothing at or after at resolves to its end offset.
func resolveTimestamp(ctx context.Context, admin *kadm.Client, topic string, at time.Time) (map[int32]int64, error) {
	listed, err := admin.ListOffsetsAfterMilli(ctx, at.UnixMilli(), topic)
	if err != nil {
		return nil, fmt.Errorf("resolve %s in %q: %w", at.UTC().Format(time.RFC3339), topic, err)
	}

	cuts := make(map[int32]int64)
	var failed error
	listed.Each(func(offset kadm.ListedOffset) {
		if offset.Err != nil {
			failed = fmt.Errorf("resolve %s in %q partition %d: %w",
				at.UTC().Format(time.RFC3339), topic, offset.Partition, offset.Err)
			return
		}
		cuts[offset.Partition] = offset.Offset
	})

	return cuts, failed
}

// removeAll applies every cut of an all_partitions item in one request.
func removeAll(ctx context.Context, admin *kadm.Client, out *Output) error {
	for i := range out.Partitions {
		cut := &out.Partitions[i]
		start, err := remove(ctx, admin, out.Topic, cut.Partition, cut.BeforeOffset)
		if err != nil {
			return fmt.Errorf("%w (partitions before %d in this item were already truncated and stay so)", err, cut.Partition)
		}
		cut.ResultingStartOffset = start
	}

	out.Note = fmt.Sprintf("%d partition(s) of %q were truncated at their resolved cuts. The deleted messages cannot be restored",
		len(out.Partitions), out.Topic)

	return nil
}

func bounds(ctx context.Context, admin *kadm.Client, topic string, partition int32) (int64, int64, error) {
	details, err := admin.ListTopics(ctx, topic)
	if err != nil {
		return 0, 0, fmt.Errorf("list topic %q: %w", topic, err)
	}

	detail, ok := details[topic]
	if !ok || errors.Is(detail.Err, kerr.UnknownTopicOrPartition) {
		return 0, 0, fmt.Errorf("topic %q does not exist", topic)
	}

	if detail.Err != nil {
		return 0, 0, fmt.Errorf("topic %q: %w", topic, detail.Err)
	}

	if detail.IsInternal {
		return 0, 0, fmt.Errorf(
			"refusing to delete records from %q: it is an internal Kafka topic holding cluster state", topic)
	}

	if _, ok := detail.Partitions[partition]; !ok {
		return 0, 0, fmt.Errorf("topic %q has no partition %d", topic, partition)
	}

	starts, err := admin.ListStartOffsets(ctx, topic)
	if err != nil {
		return 0, 0, fmt.Errorf("list start offsets for %q: %w", topic, err)
	}

	ends, err := admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return 0, 0, fmt.Errorf("list end offsets for %q: %w", topic, err)
	}

	start, ok := starts.Lookup(topic, partition)
	if !ok || start.Err != nil {
		return 0, 0, fmt.Errorf("could not read the start offset of %q partition %d", topic, partition)
	}

	end, ok := ends.Lookup(topic, partition)
	if !ok || end.Err != nil {
		return 0, 0, fmt.Errorf("could not read the end offset of %q partition %d", topic, partition)
	}

	return start.Offset, end.Offset, nil
}

// affectedGroups lists the groups whose committed offset on the partition is
// below the cut, so they would lose messages they never processed.
func affectedGroups(ctx context.Context, admin *kadm.Client, topic string, partition int32, before int64) ([]AffectedGroup, error) {
	listed, err := admin.ListGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("list groups: %w", err)
	}

	affected := []AffectedGroup{}

	names := listed.Groups()
	if len(names) == 0 {
		return affected, nil
	}

	commits := admin.FetchManyOffsets(ctx, names...)

	for _, name := range names {
		response, ok := commits[name]
		if !ok || response.Err != nil {
			continue
		}

		offset, ok := response.Fetched.Lookup(topic, partition)
		if !ok || offset.Err != nil || offset.At < 0 || offset.At >= before {
			continue
		}

		affected = append(affected, AffectedGroup{
			Group:           name,
			CommittedOffset: offset.At,
			UnprocessedLost: before - offset.At,
		})
	}

	sort.Slice(affected, func(i, j int) bool { return affected[i].Group < affected[j].Group })

	return affected, nil
}

func remove(ctx context.Context, admin *kadm.Client, topic string, partition int32, before int64) (int64, error) {
	offsets := make(kadm.Offsets)
	offsets.AddOffset(topic, partition, before, -1)

	responses, err := admin.DeleteRecords(ctx, offsets)
	if err != nil {
		return 0, fmt.Errorf("delete records from %q partition %d: %w", topic, partition, err)
	}

	response, ok := responses.Lookup(topic, partition)
	if !ok {
		return 0, fmt.Errorf(
			"delete records from %q partition %d: the broker returned no response for it", topic, partition)
	}

	// Authorization failures arrive in the response rather than as a returned
	// error, so they would otherwise pass for success.
	if response.Err != nil {
		if errors.Is(response.Err, kerr.TopicAuthorizationFailed) {
			return 0, fmt.Errorf(
				"not authorized to delete records from %q: the broker refused this request. It needs DELETE permission on the topic for the principal this server connects as: %w",
				topic, response.Err)
		}

		return 0, fmt.Errorf("delete records from %q partition %d: %w", topic, partition, response.Err)
	}

	return response.LowWatermark, nil
}
