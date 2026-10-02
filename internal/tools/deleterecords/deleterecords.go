// Package deleterecords implements the delete_records MCP tool.
package deleterecords

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/denizgursoy/kafka-mcp/internal/domain/batch"
	"github.com/denizgursoy/kafka-mcp/internal/domain/kafkaclient"
)

const toolName = "delete_records"

// Item is one partition to truncate.
type Item struct {
	Topic               string `json:"topic" jsonschema:"Topic to delete records from. Matched exactly and case-sensitively."`
	Partition           int32  `json:"partition" jsonschema:"Partition to delete records from."`
	BeforeOffset        int64  `json:"before_offset" jsonschema:"Every message with an offset lower than this is deleted; the message at before_offset is kept and becomes the first readable one. Must be greater than the partition's current start offset and at most its end offset. Use the end offset to empty the partition."`
	AcknowledgeDataLoss bool   `json:"acknowledge_data_loss,omitempty" jsonschema:"Optional. Required to apply. Deleted records cannot be restored by Kafka, so confirm alone is not enough."`
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
the first readable one. Use it to purge test data, or a run of bad messages at
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
	for _, item := range items {
		key := target{item.Topic, item.Partition}
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
				"refusing to delete %d message(s) from %q partition %d: Kafka cannot restore them. Set acknowledge_data_loss true on this item if that is intended",
				result.Result.MessagesDeleted, item.Topic, item.Partition)
			out.Failed++

			continue
		}

		start, err := remove(ctx, kafka.Admin(), item)
		if err != nil {
			out.Results[index].Result = nil
			out.Results[index].Error = err.Error()
			out.Failed++

			continue
		}

		deleted := *result.Result
		deleted.Deleted = true
		deleted.WouldDelete = false
		deleted.ResultingStartOffset = start
		deleted.Note = fmt.Sprintf(
			"%q partition %d now starts at offset %d. The deleted messages cannot be restored",
			item.Topic, item.Partition, start)

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

	start, end, err := bounds(ctx, admin, item.Topic, item.Partition)
	if err != nil {
		return Output{}, err
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

	affected, err := affectedGroups(ctx, admin, item)
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
func affectedGroups(ctx context.Context, admin *kadm.Client, item Item) ([]AffectedGroup, error) {
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

		offset, ok := response.Fetched.Lookup(item.Topic, item.Partition)
		if !ok || offset.Err != nil || offset.At < 0 || offset.At >= item.BeforeOffset {
			continue
		}

		affected = append(affected, AffectedGroup{
			Group:           name,
			CommittedOffset: offset.At,
			UnprocessedLost: item.BeforeOffset - offset.At,
		})
	}

	sort.Slice(affected, func(i, j int) bool { return affected[i].Group < affected[j].Group })

	return affected, nil
}

func remove(ctx context.Context, admin *kadm.Client, item Item) (int64, error) {
	offsets := make(kadm.Offsets)
	offsets.AddOffset(item.Topic, item.Partition, item.BeforeOffset, -1)

	responses, err := admin.DeleteRecords(ctx, offsets)
	if err != nil {
		return 0, fmt.Errorf("delete records from %q partition %d: %w", item.Topic, item.Partition, err)
	}

	response, ok := responses.Lookup(item.Topic, item.Partition)
	if !ok {
		return 0, fmt.Errorf(
			"delete records from %q partition %d: the broker returned no response for it", item.Topic, item.Partition)
	}

	// Authorization failures arrive in the response rather than as a returned
	// error, so they would otherwise pass for success.
	if response.Err != nil {
		if errors.Is(response.Err, kerr.TopicAuthorizationFailed) {
			return 0, fmt.Errorf(
				"not authorized to delete records from %q: the broker refused this request. It needs DELETE permission on the topic for the principal this server connects as: %w",
				item.Topic, response.Err)
		}

		return 0, fmt.Errorf("delete records from %q partition %d: %w", item.Topic, item.Partition, response.Err)
	}

	return response.LowWatermark, nil
}
