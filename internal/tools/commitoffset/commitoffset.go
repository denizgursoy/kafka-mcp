// Package commitoffset implements the commit_offset MCP tool.
package commitoffset

import (
	"context"
	"errors"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/denizgursoy/kafka-mcp/internal/domain/batch"
	"github.com/denizgursoy/kafka-mcp/internal/domain/kafkaclient"
)

// Item is one consumer offset move in a batch request.
type Item struct {
	Topic     string `json:"topic" jsonschema:"Topic whose offset is being moved. Matched exactly and case-sensitively."`
	Group     string `json:"group" jsonschema:"Consumer group to move. It must already exist."`
	Partition int32  `json:"partition" jsonschema:"Partition to move."`
	// Offset is absolute rather than relative so there is no ambiguity about
	// what a caller meant, and repeating the same call cannot drift.
	Offset             int64 `json:"offset" jsonschema:"The offset the group will read from next, exactly as Kafka stores it. To skip the message at offset 42, commit 43. Must be within the partition's start and end offsets."`
	AllowActiveMembers bool  `json:"allow_active_members,omitempty" jsonschema:"Optional. Required when the group has active members. A running consumer keeps its position in memory and will usually overwrite this commit, so the change is likely to have no effect unless the consumers are restarted straight afterwards."`
}

// Input is the argument set accepted by the commit_offset tool.
type Input struct {
	Items   []Item `json:"items" jsonschema:"The offsets to move, 1 to 100 of them. Moving one offset is an array of length one. Two items naming the same group, topic and partition are refused before anything changes."`
	Confirm bool   `json:"confirm,omitempty" jsonschema:"Optional. When false or omitted, nothing is changed and the response describes what would happen for every item. Must be true to actually move the offsets. One confirm covers the whole batch."`
}

// Output is the result returned by the commit_offset tool.
type Output struct {
	Topic            string `json:"topic"`
	Group            string `json:"group"`
	Partition        int32  `json:"partition"`
	State            string `json:"state"`
	Members          int    `json:"members"`
	CurrentOffset    int64  `json:"current_offset"`
	RequestedOffset  int64  `json:"requested_offset"`
	ResultingOffset  int64  `json:"resulting_offset,omitempty"`
	StartOffset      int64  `json:"partition_start_offset"`
	EndOffset        int64  `json:"partition_end_offset"`
	SkippedMessages  int64  `json:"skipped_messages,omitempty"`
	ReplayedMessages int64  `json:"replayed_messages,omitempty"`
	Applied          bool   `json:"applied"`

	Warnings []string `json:"warnings,omitempty"`
	Note     string   `json:"note,omitempty"`
}

type BatchOutput = batch.Output[Output]

const description = `
Move 1 to 100 committed offsets in one call through items. The response previews
how many messages each move would skip or replay; offset is the next message the
group will read. Moving one offset is an items array of length one.

No change is made unless confirm is true; one confirm covers the whole batch,
and applying is not atomic because Kafka cannot roll back commits that succeeded
before a later item failed. Results follow items order, each carrying index with
result or error.

Active groups are refused unless allow_active_members is set on the item,
because running consumers may overwrite the commit. Requires Kafka offset-commit
permission.
`

// Register adds the commit_offset tool to the MCP server.
func Register(server *mcp.Server, kafka *kafkaclient.Client) {
	mcp.AddTool(
		server,
		&mcp.Tool{
			Name:        "commit_offset",
			Description: description,
		},
		func(
			ctx context.Context,
			req *mcp.CallToolRequest,
			input Input,
		) (*mcp.CallToolResult, BatchOutput, error) {

			out, err := Run(ctx, kafka, input)
			if err != nil {
				return nil, BatchOutput{}, fmt.Errorf("commit offsets: %w", err)
			}

			return nil, out, nil
		},
	)
}

// Run previews every item before applying any valid item. Applying is not
// atomic: Kafka cannot roll back offset commits that succeeded before another
// item failed.
func Run(ctx context.Context, kafka *kafkaclient.Client, input Input) (BatchOutput, error) {
	items, confirm := input.Items, input.Confirm

	if err := batch.Validate(len(items), batch.MaxItems); err != nil {
		return BatchOutput{}, err
	}
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		key := fmt.Sprintf("%s\x00%s\x00%d", item.Group, item.Topic, item.Partition)
		if _, ok := seen[key]; ok {
			return BatchOutput{}, fmt.Errorf("duplicate offset target for group %q topic %q partition %d", item.Group, item.Topic, item.Partition)
		}
		seen[key] = struct{}{}
	}

	out, err := batch.Run(ctx, items, batch.MaxItems, func(ctx context.Context, item Item) (Output, error) {
		return move(ctx, kafka, item, false)
	})
	if err != nil || !confirm {
		return out, err
	}

	out.Succeeded, out.Failed = 0, 0
	for index, item := range items {
		if out.Results[index].Error != "" {
			out.Failed++
			continue
		}
		value, applyErr := move(ctx, kafka, item, true)
		if applyErr != nil {
			out.Results[index].Result = nil
			out.Results[index].Error = applyErr.Error()
			out.Failed++
			continue
		}
		out.Results[index].Result = &value
		out.Succeeded++
		if value.Applied {
			out.Applied++
		}
	}
	return out, nil
}

// move previews or applies one change to a group's committed offset.
func move(
	ctx context.Context,
	kafka *kafkaclient.Client,
	input Item,
	confirm bool,
) (Output, error) {

	if input.Topic == "" {
		return Output{}, fmt.Errorf("topic is required")
	}

	if input.Group == "" {
		return Output{}, fmt.Errorf("group is required")
	}

	// Kafka gives some negative offsets a special meaning, so a negative
	// value here would not be rejected by the broker but would move the group
	// somewhere the caller did not ask for.
	if input.Offset < 0 {
		return Output{}, fmt.Errorf(
			"offset must not be negative, got %d: it is the offset the group reads next",
			input.Offset)
	}

	admin := kafka.Admin()

	start, end, err := partitionBounds(ctx, admin, input.Topic, input.Partition)
	if err != nil {
		return Output{}, err
	}

	// An offset past the end leaves the group looking caught up while
	// consuming nothing, which is a silent failure rather than a visible one.
	if input.Offset < start || input.Offset > end {
		return Output{}, fmt.Errorf(
			"offset %d is outside partition %d of %q, which holds offsets %d to %d",
			input.Offset, input.Partition, input.Topic, start, end)
	}

	described, err := admin.DescribeGroups(ctx, input.Group)
	if err != nil {
		return Output{}, fmt.Errorf("describe group %q: %w", input.Group, err)
	}

	group, ok := described[input.Group]
	if !ok || group.State == "Dead" {
		return Output{}, fmt.Errorf("consumer group %q does not exist", input.Group)
	}

	if group.Err != nil {
		return Output{}, fmt.Errorf("consumer group %q: %w", input.Group, group.Err)
	}

	current, err := committedOffset(ctx, admin, input)
	if err != nil {
		return Output{}, err
	}

	out := Output{
		Topic:           input.Topic,
		Group:           input.Group,
		Partition:       input.Partition,
		State:           group.State,
		Members:         len(group.Members),
		CurrentOffset:   current,
		RequestedOffset: input.Offset,
		StartOffset:     start,
		EndOffset:       end,
		Warnings:        []string{},
	}

	switch {
	case input.Offset > current:
		out.SkippedMessages = input.Offset - current
		out.Warnings = append(out.Warnings, fmt.Sprintf(
			"%d message(s) between offsets %d and %d will never be processed by group %q",
			out.SkippedMessages, current, input.Offset, input.Group))

	case input.Offset < current:
		out.ReplayedMessages = current - input.Offset
		out.Warnings = append(out.Warnings, fmt.Sprintf(
			"%d message(s) between offsets %d and %d will be processed again, so expect duplicates",
			out.ReplayedMessages, input.Offset, current))
	}

	if len(group.Members) > 0 {
		out.Warnings = append(out.Warnings, fmt.Sprintf(
			"group %q has %d active member(s): a running consumer keeps its position in memory and will usually overwrite this commit",
			input.Group, len(group.Members)))
	}

	if input.Offset == current {
		out.Note = fmt.Sprintf(
			"group %q is already committed at offset %d, so there is nothing to do",
			input.Group, current)

		return out, nil
	}

	if !confirm {
		out.Note = "nothing was changed. Call again with confirm true to move the offset."

		return out, nil
	}

	if len(group.Members) > 0 && !input.AllowActiveMembers {
		return Output{}, fmt.Errorf(
			"refusing to commit for group %q: it has %d active member(s), and a running consumer keeps its position in memory and will overwrite this commit. Stop the consumers first, or set allow_active_members true if you will restart them immediately afterwards",
			input.Group, len(group.Members))
	}

	// The read-only check sits immediately before the only call that changes
	// anything, so every earlier refusal is reported on its own terms. This
	// protection is enforced here rather than left to Kafka ACLs, because many
	// clusters have none.
	if err := kafka.RequireWritable("commit_offset"); err != nil {
		return Output{}, err
	}

	if err := commit(ctx, admin, input); err != nil {
		return Output{}, err
	}

	// Re-read rather than assume: the resulting offset is what the broker says
	// it is, not what was asked for.
	resulting, err := committedOffset(ctx, admin, input)
	if err != nil {
		return Output{}, err
	}

	out.Applied = true
	out.ResultingOffset = resulting
	out.Note = fmt.Sprintf(
		"group %q now reads partition %d of %q from offset %d",
		input.Group, input.Partition, input.Topic, resulting)

	if len(group.Members) > 0 {
		out.Warnings = append(out.Warnings,
			"the group still has active members, so this commit may be overwritten: restart the consumers to be sure they pick it up")
	}

	return out, nil
}

func commit(ctx context.Context, admin *kadm.Client, input Item) error {
	offsets := make(kadm.Offsets)

	// A leader epoch of -1 means "unknown", which is what a caller moving an
	// offset by hand always has.
	offsets.AddOffset(input.Topic, input.Partition, input.Offset, -1)

	responses, err := admin.CommitOffsets(ctx, input.Group, offsets)
	if err != nil {
		return fmt.Errorf("commit offset for group %q: %w", input.Group, err)
	}

	response, ok := responses.Lookup(input.Topic, input.Partition)
	if !ok {
		return fmt.Errorf(
			"commit offset for group %q: the broker returned no response for %s partition %d",
			input.Group, input.Topic, input.Partition)
	}

	// Authorization failures arrive in the response rather than as a returned
	// error, so they would otherwise pass for success.
	if response.Err != nil {
		if errors.Is(response.Err, kerr.GroupAuthorizationFailed) ||
			errors.Is(response.Err, kerr.TopicAuthorizationFailed) {

			return fmt.Errorf(
				"not authorized to commit offsets for group %q: the broker refused this request. Committing needs offset-commit permission on the group for the principal this server connects as: %w",
				input.Group, response.Err)
		}

		return fmt.Errorf("commit offset for group %q: %w", input.Group, response.Err)
	}

	return nil
}

// committedOffset reports where the group currently reads from, or the
// partition start when it has never committed.
func committedOffset(ctx context.Context, admin *kadm.Client, input Item) (int64, error) {
	offsets, err := admin.FetchOffsets(ctx, input.Group)
	if err != nil {
		return 0, fmt.Errorf("fetch offsets for group %q: %w", input.Group, err)
	}

	offset, ok := offsets.Lookup(input.Topic, input.Partition)
	if !ok {
		return 0, nil
	}

	if offset.Err != nil {
		return 0, fmt.Errorf(
			"fetch offset for group %q on %s partition %d: %w",
			input.Group, input.Topic, input.Partition, offset.Err)
	}

	return offset.At, nil
}

// partitionBounds returns the offsets a partition actually holds, so a commit
// cannot land outside them.
func partitionBounds(
	ctx context.Context,
	admin *kadm.Client,
	topic string,
	partition int32,
) (int64, int64, error) {

	details, err := admin.ListTopics(ctx, topic)
	if err != nil {
		return 0, 0, fmt.Errorf("list topic %q: %w", topic, err)
	}

	detail, ok := details[topic]
	if !ok {
		return 0, 0, fmt.Errorf("topic %q does not exist", topic)
	}

	if detail.Err != nil {
		return 0, 0, fmt.Errorf("topic %q: %w", topic, detail.Err)
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
