// Package commitoffset implements the commit_offset MCP tool.
package commitoffset

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

// Item is one consumer offset move in a batch request.
type Item struct {
	Topic string `json:"topic" jsonschema:"Topic whose offset is being moved. Matched exactly and case-sensitively."`
	Group string `json:"group" jsonschema:"Consumer group to move. It must already exist."`
	// Partition is a pointer because partition 0 is a real partition, and
	// omitting it has its own meaning: every partition of the topic.
	Partition *int32 `json:"partition,omitempty" jsonschema:"Optional partition to move. Required with offset. With timestamp or position, omit it to move every partition of the topic."`
	// Offset is absolute rather than relative so there is no ambiguity about
	// what a caller meant, and repeating the same call cannot drift.
	Offset             *int64 `json:"offset,omitempty" jsonschema:"The offset the group will read from next, exactly as Kafka stores it. To skip the message at offset 42, commit 43. Must be within the partition's start and end offsets. Give exactly one of offset, timestamp or position."`
	Timestamp          string `json:"timestamp,omitempty" jsonschema:"RFC3339 time to move to, such as 2026-10-01T09:00:00Z. Each partition moves to its first message at or after this time, or to its end when it has none. Use this to replay everything since a moment. Give exactly one of offset, timestamp or position."`
	Position           string `json:"position,omitempty" jsonschema:"earliest (the first offset each partition still holds, replaying everything) or latest (the end, skipping every unread message). Case-sensitive. Give exactly one of offset, timestamp or position."`
	AllowActiveMembers bool   `json:"allow_active_members,omitempty" jsonschema:"Optional. Required when the group has active members. A running consumer keeps its position in memory and will usually overwrite this commit, so the change is likely to have no effect unless the consumers are restarted straight afterwards."`
}

// Positions accepted by Item.Position.
const (
	PositionEarliest = "earliest"
	PositionLatest   = "latest"
)

// Input is the argument set accepted by the commit_offset tool.
type Input struct {
	Items   []Item `json:"items" jsonschema:"The moves to make, 1 to 100 of them. Moving one offset is an array of length one. Two items that would move the same group, topic and partition, including a whole-topic item and a partition item, are refused before anything changes."`
	Confirm bool   `json:"confirm,omitempty" jsonschema:"Optional. When false or omitted, nothing is changed and the response describes what would happen for every item. Must be true to actually move the offsets. One confirm covers the whole batch."`
}

// PartitionMove is what one item does to one partition.
type PartitionMove struct {
	Partition        int32  `json:"partition"`
	CurrentOffset    int64  `json:"current_offset"`
	TargetOffset     int64  `json:"target_offset"`
	ResultingOffset  *int64 `json:"resulting_offset,omitempty"`
	StartOffset      int64  `json:"partition_start_offset"`
	EndOffset        int64  `json:"partition_end_offset"`
	SkippedMessages  int64  `json:"skipped_messages,omitempty"`
	ReplayedMessages int64  `json:"replayed_messages,omitempty"`
	Note             string `json:"note,omitempty"`
}

// Output is the result of one item.
type Output struct {
	Topic   string `json:"topic"`
	Group   string `json:"group"`
	State   string `json:"state"`
	Members int    `json:"members"`

	// Partitions holds one entry per partition the item moves, sorted.
	Partitions []PartitionMove `json:"partitions"`

	// SkippedMessages and ReplayedMessages total every partition, so the
	// number a caller agrees to is a single figure.
	SkippedMessages  int64 `json:"skipped_messages,omitempty"`
	ReplayedMessages int64 `json:"replayed_messages,omitempty"`
	Applied          bool  `json:"applied"`

	Warnings []string `json:"warnings"`
	Note     string   `json:"note,omitempty"`
}

type BatchOutput = batch.Output[Output]

const description = `
Move 1 to 100 consumer group positions in one call through items. Each item
names a group and topic and exactly one target: offset (one partition, the next
offset the group reads), timestamp (RFC3339; each partition moves to its first
message at or after that time, which is how messages are replayed since a
moment), or position (earliest or latest). With timestamp or position, omit
partition to move every partition of the topic. Moving one offset is an items
array of length one.

The response previews, per partition and in total, how many messages each move
would skip or replay. No change is made unless confirm is true; one confirm
covers the whole batch, and applying is not atomic because Kafka cannot roll
back commits that succeeded before a later item failed. Results follow items
order, each carrying index with result or error.

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
	if err := refuseOverlaps(items); err != nil {
		return BatchOutput{}, err
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

// refuseOverlaps rejects a batch in which two items would move the same
// partition of the same group, because which commit wins would depend on the
// order they happen to be applied in.
func refuseOverlaps(items []Item) error {
	type target struct{ group, topic string }

	wholeTopic := make(map[target]bool, len(items))
	partitions := make(map[target]map[int32]bool, len(items))

	for _, item := range items {
		key := target{group: item.Group, topic: item.Topic}

		if item.Partition == nil {
			if wholeTopic[key] || len(partitions[key]) > 0 {
				return fmt.Errorf(
					"two items move group %q on topic %q, and one of them covers every partition: combine them into one item",
					item.Group, item.Topic)
			}

			wholeTopic[key] = true

			continue
		}

		if wholeTopic[key] {
			return fmt.Errorf(
				"two items move group %q on topic %q, and one of them covers every partition: combine them into one item",
				item.Group, item.Topic)
		}

		if partitions[key] == nil {
			partitions[key] = map[int32]bool{}
		}

		if partitions[key][*item.Partition] {
			return fmt.Errorf("duplicate offset target for group %q topic %q partition %d",
				item.Group, item.Topic, *item.Partition)
		}

		partitions[key][*item.Partition] = true
	}

	return nil
}

// validate checks the item's shape before anything is read from the broker.
func validate(input Item) (time.Time, error) {
	if input.Topic == "" {
		return time.Time{}, fmt.Errorf("topic is required")
	}

	if input.Group == "" {
		return time.Time{}, fmt.Errorf("group is required")
	}

	targets := 0
	if input.Offset != nil {
		targets++
	}
	if input.Timestamp != "" {
		targets++
	}
	if input.Position != "" {
		targets++
	}

	if targets != 1 {
		return time.Time{}, fmt.Errorf(
			"give exactly one of offset, timestamp or position: each names a different place to move to, got %d", targets)
	}

	if input.Partition != nil && *input.Partition < 0 {
		return time.Time{}, fmt.Errorf("partition must not be negative, got %d", *input.Partition)
	}

	if input.Offset != nil {
		if input.Partition == nil {
			return time.Time{}, fmt.Errorf(
				"offset needs a partition: an exact offset names one place in one partition, and the same number in another partition is an unrelated message")
		}

		// Kafka gives some negative offsets a special meaning, so a negative
		// value here would not be rejected by the broker but would move the
		// group somewhere the caller did not ask for.
		if *input.Offset < 0 {
			return time.Time{}, fmt.Errorf(
				"offset must not be negative, got %d: it is the offset the group reads next",
				*input.Offset)
		}
	}

	if input.Position != "" && input.Position != PositionEarliest && input.Position != PositionLatest {
		return time.Time{}, fmt.Errorf("position must be %q or %q, got %q",
			PositionEarliest, PositionLatest, input.Position)
	}

	if input.Timestamp != "" {
		at, err := time.Parse(time.RFC3339, input.Timestamp)
		if err != nil {
			return time.Time{}, fmt.Errorf("timestamp must be RFC3339, such as 2026-10-01T09:00:00Z: %w", err)
		}

		return at, nil
	}

	return time.Time{}, nil
}

// move previews or applies one item's change to a group's committed offsets.
func move(
	ctx context.Context,
	kafka *kafkaclient.Client,
	input Item,
	confirm bool,
) (Output, error) {

	at, err := validate(input)
	if err != nil {
		return Output{}, err
	}

	admin := kafka.Admin()

	bounds, err := partitionBounds(ctx, admin, input.Topic, input.Partition)
	if err != nil {
		return Output{}, err
	}

	targets, notes, err := resolveTargets(ctx, admin, input, at, bounds)
	if err != nil {
		return Output{}, err
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

	current, err := committedOffsets(ctx, admin, input.Group, input.Topic, bounds)
	if err != nil {
		return Output{}, err
	}

	out := Output{
		Topic:      input.Topic,
		Group:      input.Group,
		State:      group.State,
		Members:    len(group.Members),
		Partitions: make([]PartitionMove, 0, len(bounds)),
		Warnings:   []string{},
	}

	changes := 0

	for _, partition := range sortedPartitions(bounds) {
		move := PartitionMove{
			Partition:     partition,
			CurrentOffset: current[partition],
			TargetOffset:  targets[partition],
			StartOffset:   bounds[partition].start,
			EndOffset:     bounds[partition].end,
			Note:          notes[partition],
		}

		switch {
		case move.TargetOffset > move.CurrentOffset:
			move.SkippedMessages = move.TargetOffset - move.CurrentOffset
		case move.TargetOffset < move.CurrentOffset:
			move.ReplayedMessages = move.CurrentOffset - move.TargetOffset
		}

		if move.TargetOffset != move.CurrentOffset {
			changes++
		}

		out.SkippedMessages += move.SkippedMessages
		out.ReplayedMessages += move.ReplayedMessages
		out.Partitions = append(out.Partitions, move)
	}

	if out.SkippedMessages > 0 {
		out.Warnings = append(out.Warnings, fmt.Sprintf(
			"%d message(s) will never be processed by group %q", out.SkippedMessages, input.Group))
	}

	if out.ReplayedMessages > 0 {
		out.Warnings = append(out.Warnings, fmt.Sprintf(
			"%d message(s) will be processed again by group %q, so expect duplicates",
			out.ReplayedMessages, input.Group))
	}

	if len(group.Members) > 0 {
		out.Warnings = append(out.Warnings, fmt.Sprintf(
			"group %q has %d active member(s): a running consumer keeps its position in memory and will usually overwrite this commit",
			input.Group, len(group.Members)))
	}

	if changes == 0 {
		out.Note = fmt.Sprintf("group %q is already where this item would move it, so there is nothing to do", input.Group)

		return out, nil
	}

	if !confirm {
		out.Note = "nothing was changed. Call again with confirm true to move the offsets."

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

	if err := commit(ctx, admin, input.Group, input.Topic, targets); err != nil {
		return Output{}, err
	}

	// Re-read rather than assume: the resulting offset is what the broker says
	// it is, not what was asked for.
	resulting, err := committedOffsets(ctx, admin, input.Group, input.Topic, bounds)
	if err != nil {
		return Output{}, err
	}

	for index := range out.Partitions {
		offset := resulting[out.Partitions[index].Partition]
		out.Partitions[index].ResultingOffset = &offset
	}

	out.Applied = true
	out.Note = fmt.Sprintf("group %q moved on %d partition(s) of %q", input.Group, changes, input.Topic)

	if len(group.Members) > 0 {
		out.Warnings = append(out.Warnings,
			"the group still has active members, so this commit may be overwritten: restart the consumers to be sure they pick it up")
	}

	return out, nil
}

// resolveTargets turns the item's offset, timestamp or position into one
// target offset per partition, with a note where the target needs explaining.
func resolveTargets(
	ctx context.Context,
	admin *kadm.Client,
	input Item,
	at time.Time,
	bounds map[int32]bounds,
) (map[int32]int64, map[int32]string, error) {

	targets := make(map[int32]int64, len(bounds))
	notes := make(map[int32]string, len(bounds))

	switch {
	case input.Offset != nil:
		partition := *input.Partition
		limits := bounds[partition]

		// An offset past the end leaves the group looking caught up while
		// consuming nothing, which is a silent failure rather than a visible
		// one.
		if *input.Offset < limits.start || *input.Offset > limits.end {
			return nil, nil, fmt.Errorf(
				"offset %d is outside partition %d of %q, which holds offsets %d to %d",
				*input.Offset, partition, input.Topic, limits.start, limits.end)
		}

		targets[partition] = *input.Offset

	case input.Position == PositionEarliest:
		for partition, limits := range bounds {
			targets[partition] = limits.start
		}

	case input.Position == PositionLatest:
		for partition, limits := range bounds {
			targets[partition] = limits.end
		}

	default:
		listed, err := admin.ListOffsetsAfterMilli(ctx, at.UnixMilli(), input.Topic)
		if err != nil {
			return nil, nil, fmt.Errorf("list offsets after %s for %q: %w", input.Timestamp, input.Topic, err)
		}

		for partition, limits := range bounds {
			offset, ok := listed.Lookup(input.Topic, partition)
			if !ok || offset.Err != nil {
				return nil, nil, fmt.Errorf(
					"could not resolve %s to an offset on %q partition %d", input.Timestamp, input.Topic, partition)
			}

			// Kafka answers "no message at or after this time" with the end
			// offset, which is indistinguishable from a real match unless it
			// is called out: a mistyped date would otherwise look like a
			// replay of nothing.
			target := offset.Offset
			if target < 0 || target >= limits.end {
				target = limits.end
				notes[partition] = fmt.Sprintf(
					"no message at or after %s, so the target is the end of the partition", input.Timestamp)
			}

			targets[partition] = target
		}
	}

	return targets, notes, nil
}

func commit(ctx context.Context, admin *kadm.Client, group string, topic string, targets map[int32]int64) error {
	offsets := make(kadm.Offsets)

	// A leader epoch of -1 means "unknown", which is what a caller moving an
	// offset by hand always has.
	for partition, offset := range targets {
		offsets.AddOffset(topic, partition, offset, -1)
	}

	responses, err := admin.CommitOffsets(ctx, group, offsets)
	if err != nil {
		return fmt.Errorf("commit offsets for group %q: %w", group, err)
	}

	for _, partition := range sortedPartitions(targets) {
		response, ok := responses.Lookup(topic, partition)
		if !ok {
			return fmt.Errorf(
				"commit offset for group %q: the broker returned no response for %s partition %d",
				group, topic, partition)
		}

		// Authorization failures arrive in the response rather than as a
		// returned error, so they would otherwise pass for success.
		if response.Err != nil {
			if errors.Is(response.Err, kerr.GroupAuthorizationFailed) ||
				errors.Is(response.Err, kerr.TopicAuthorizationFailed) {

				return fmt.Errorf(
					"not authorized to commit offsets for group %q: the broker refused this request. Committing needs offset-commit permission on the group for the principal this server connects as: %w",
					group, response.Err)
			}

			return fmt.Errorf("commit offset for group %q partition %d: %w", group, partition, response.Err)
		}
	}

	return nil
}

// committedOffsets reports where the group currently reads each partition
// from, or the partition start where it has never committed.
func committedOffsets(
	ctx context.Context,
	admin *kadm.Client,
	group string,
	topic string,
	bounds map[int32]bounds,
) (map[int32]int64, error) {

	offsets, err := admin.FetchOffsets(ctx, group)
	if err != nil {
		return nil, fmt.Errorf("fetch offsets for group %q: %w", group, err)
	}

	current := make(map[int32]int64, len(bounds))

	for partition, limits := range bounds {
		offset, ok := offsets.Lookup(topic, partition)
		if !ok || offset.At < 0 {
			current[partition] = limits.start

			continue
		}

		if offset.Err != nil {
			return nil, fmt.Errorf(
				"fetch offset for group %q on %s partition %d: %w", group, topic, partition, offset.Err)
		}

		current[partition] = offset.At
	}

	return current, nil
}

type bounds struct{ start, end int64 }

// partitionBounds returns the offsets each partition actually holds, for one
// partition or for all of them when partition is nil.
func partitionBounds(
	ctx context.Context,
	admin *kadm.Client,
	topic string,
	partition *int32,
) (map[int32]bounds, error) {

	details, err := admin.ListTopics(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("list topic %q: %w", topic, err)
	}

	detail, ok := details[topic]
	if !ok {
		return nil, fmt.Errorf("topic %q does not exist", topic)
	}

	if detail.Err != nil {
		return nil, fmt.Errorf("topic %q: %w", topic, detail.Err)
	}

	wanted := make([]int32, 0, len(detail.Partitions))

	if partition != nil {
		if _, ok := detail.Partitions[*partition]; !ok {
			return nil, fmt.Errorf("topic %q has no partition %d", topic, *partition)
		}

		wanted = append(wanted, *partition)
	} else {
		for id := range detail.Partitions {
			wanted = append(wanted, id)
		}
	}

	starts, err := admin.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("list start offsets for %q: %w", topic, err)
	}

	ends, err := admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("list end offsets for %q: %w", topic, err)
	}

	out := make(map[int32]bounds, len(wanted))

	for _, id := range wanted {
		start, ok := starts.Lookup(topic, id)
		if !ok || start.Err != nil {
			return nil, fmt.Errorf("could not read the start offset of %q partition %d", topic, id)
		}

		end, ok := ends.Lookup(topic, id)
		if !ok || end.Err != nil {
			return nil, fmt.Errorf("could not read the end offset of %q partition %d", topic, id)
		}

		out[id] = bounds{start: start.Offset, end: end.Offset}
	}

	return out, nil
}

// sortedPartitions returns a map's partitions in order, because Go map order is
// random and the report has to be stable.
func sortedPartitions[V any](byPartition map[int32]V) []int32 {
	partitions := make([]int32, 0, len(byPartition))
	for partition := range byPartition {
		partitions = append(partitions, partition)
	}

	sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })

	return partitions
}
