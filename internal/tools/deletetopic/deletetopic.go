// Package deletetopic implements the delete_topic MCP tool.
package deletetopic

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

const toolName = "delete_topic"

// Item is one topic to delete.
type Item struct {
	Topic string `json:"topic" jsonschema:"Name of the topic to delete. Matched exactly and case-sensitively. The topic must exist: a name that does not is refused rather than reported as deleted."`
	// AcknowledgeDataLoss is separate from confirm on purpose. confirm says the
	// caller meant to delete; this says they know what is inside.
	AcknowledgeDataLoss bool `json:"acknowledge_data_loss,omitempty" jsonschema:"Optional. Required when the topic still holds messages. Deleting a topic destroys every message in it, and Kafka has no undo: the only recovery is a backup taken beforehand. Preview first to see message_count, then set this to confirm that losing them is intended."`
}

// Input is the argument set accepted by the delete_topic tool.
type Input struct {
	Items   []Item `json:"items" jsonschema:"The topics to delete, 1 to 100 of them. Deleting one topic is an array of length one. Naming the same topic twice is refused before anything is deleted."`
	Confirm bool   `json:"confirm,omitempty" jsonschema:"Optional. When false or omitted, nothing is deleted and the response describes what each deletion would destroy. Must be true to actually delete. One confirm covers the whole batch."`
}

// Output is the result returned for one topic.
type Output struct {
	Topic string `json:"topic"`
	// Partitions and MessageCount describe what was there, so a caller who
	// deletes by mistake knows the shape they have to recreate and how much
	// they lost.
	Partitions   int   `json:"partitions"`
	MessageCount int64 `json:"message_count"`
	// ConsumerGroups names the groups that had committed offsets for this
	// topic. They break when it disappears.
	ConsumerGroups []string `json:"consumer_groups,omitempty"`
	Deleted        bool     `json:"deleted"`
	WouldDelete    bool     `json:"would_delete"`
	Warnings       []string `json:"warnings,omitempty"`
	Note           string   `json:"note,omitempty"`
}

type BatchOutput = batch.Output[Output]

const description = `
Delete 1 to 100 Kafka topics in one call through items. Deleting one topic is an
items array of length one.

This is the most destructive tool here. Deleting a topic destroys every message
in it, and Kafka has no undo: recreating the topic does not bring the data back.
Any consumer group reading it breaks.

Nothing is deleted unless confirm is true; otherwise the response reports, per
topic, how many messages would be destroyed, how many partitions it had, and
which consumer groups had committed offsets for it. A topic that still holds
messages also requires acknowledge_data_loss on its item, so destroying data is
never a single unconsidered call.

One confirm covers the whole batch, and deletion is not atomic: topics deleted
before a later item failed stay deleted. Results follow items order, each
carrying index with result or error. Internal topics are refused. Requires Kafka
DELETE permission.
`

// Register adds the delete_topic tool to the MCP server.
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
				return nil, BatchOutput{}, fmt.Errorf("delete topics: %w", err)
			}

			return nil, out, nil
		},
	)
}

// Run previews every topic before deleting any valid item. Deletion is
// non-atomic because Kafka cannot restore a topic to roll a successful item
// back.
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

	seen := make(map[string]struct{}, len(items))

	for _, item := range items {
		if _, ok := seen[item.Topic]; ok {
			return BatchOutput{}, fmt.Errorf(
				"duplicate topic %q in items: naming one topic twice in a deletion means the caller has lost track of what they are removing",
				item.Topic)
		}

		seen[item.Topic] = struct{}{}
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

		// The acknowledgement is checked against what the preview just read, so
		// a topic that gained messages since the caller looked is still caught.
		if result.Result.MessageCount > 0 && !item.AcknowledgeDataLoss {
			out.Results[index].Result = nil
			out.Results[index].Error = fmt.Sprintf(
				"refusing to delete %q: it holds %d message(s), which deleting destroys irrecoverably. Set acknowledge_data_loss true on this item if that is intended",
				item.Topic, result.Result.MessageCount)
			out.Failed++

			continue
		}

		if err := remove(ctx, kafka.Admin(), item.Topic); err != nil {
			out.Results[index].Result = nil
			out.Results[index].Error = err.Error()
			out.Failed++

			continue
		}

		deleted := *result.Result
		deleted.Deleted = true
		deleted.WouldDelete = false
		deleted.Note = fmt.Sprintf(
			"deleted %q and its %d message(s). Kafka cannot restore it; recreating the topic does not bring the data back",
			item.Topic, deleted.MessageCount)

		out.Results[index].Result = &deleted
		out.Succeeded++
		out.Applied++
	}

	return out, nil
}

// preview reports what deleting one topic would destroy.
func preview(ctx context.Context, admin *kadm.Client, input Item) (Output, error) {
	if input.Topic == "" {
		return Output{}, fmt.Errorf("topic is required")
	}

	detail, err := describe(ctx, admin, input.Topic)
	if err != nil {
		return Output{}, err
	}

	// An internal topic is Kafka's own state rather than a caller's data.
	// Deleting __consumer_offsets breaks every consumer on the cluster at once,
	// which is not a debugging operation at any level of acknowledgement.
	if detail.IsInternal {
		return Output{}, fmt.Errorf(
			"refusing to delete %q: it is an internal Kafka topic holding cluster state, and removing it breaks consumers across the whole cluster",
			input.Topic)
	}

	count, err := messageCount(ctx, admin, input.Topic)
	if err != nil {
		return Output{}, err
	}

	groups, err := committedGroups(ctx, admin, input.Topic)
	if err != nil {
		return Output{}, err
	}

	out := Output{
		Topic:          input.Topic,
		Partitions:     len(detail.Partitions),
		MessageCount:   count,
		ConsumerGroups: groups,
		WouldDelete:    true,
		Warnings:       make([]string, 0, 2),
	}

	if count > 0 {
		out.Warnings = append(out.Warnings, fmt.Sprintf(
			"%d message(s) would be destroyed, and Kafka cannot restore them: the only recovery is a backup taken beforehand",
			count))
	}

	if len(groups) > 0 {
		out.Warnings = append(out.Warnings, fmt.Sprintf(
			"%d consumer group(s) have committed offsets for this topic and will break when it disappears: %v",
			len(groups), groups))
	}

	out.Note = "nothing was deleted. Call again with confirm true to delete this topic."

	if count > 0 {
		out.Note = "nothing was deleted. This topic holds messages, so deleting it needs confirm true and acknowledge_data_loss true on this item."
	}

	return out, nil
}

func describe(ctx context.Context, admin *kadm.Client, topic string) (kadm.TopicDetail, error) {
	listed, err := admin.ListTopics(ctx, topic)
	if err != nil {
		return kadm.TopicDetail{}, fmt.Errorf("list topic %q: %w", topic, err)
	}

	detail, ok := listed[topic]

	// The broker reports an absent topic either by omitting it or by returning
	// it with UNKNOWN_TOPIC_OR_PARTITION, and both mean the same thing here.
	if !ok || errors.Is(detail.Err, kerr.UnknownTopicOrPartition) {
		return kadm.TopicDetail{}, fmt.Errorf(
			"topic %q does not exist: nothing was deleted, and a name that is not on this cluster is more likely a typo than a topic already gone",
			topic)
	}

	if detail.Err != nil {
		return kadm.TopicDetail{}, fmt.Errorf("topic %q: %w", topic, detail.Err)
	}

	return detail, nil
}

// messageCount reports how many messages the topic currently holds.
//
// It is the offset span, so it overcounts where retention or compaction has
// removed records. That is the safe direction for a warning: it never claims a
// topic is emptier than it is.
func messageCount(ctx context.Context, admin *kadm.Client, topic string) (int64, error) {
	starts, err := admin.ListStartOffsets(ctx, topic)
	if err != nil {
		return 0, fmt.Errorf("list start offsets for %q: %w", topic, err)
	}

	ends, err := admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return 0, fmt.Errorf("list end offsets for %q: %w", topic, err)
	}

	var count int64

	ends.Each(func(end kadm.ListedOffset) {
		if end.Err != nil {
			return
		}

		start, ok := starts.Lookup(end.Topic, end.Partition)
		if !ok || start.Err != nil {
			return
		}

		if span := end.Offset - start.Offset; span > 0 {
			count += span
		}
	})

	return count, nil
}

// committedGroups lists the consumer groups holding offsets for the topic.
func committedGroups(ctx context.Context, admin *kadm.Client, topic string) ([]string, error) {
	listed, err := admin.ListGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("list groups: %w", err)
	}

	names := listed.Groups()
	if len(names) == 0 {
		return []string{}, nil
	}

	commits := admin.FetchManyOffsets(ctx, names...)

	affected := make([]string, 0)

	for _, name := range names {
		response, ok := commits[name]
		if !ok || response.Err != nil {
			continue
		}

		found := false

		response.Fetched.Each(func(offset kadm.OffsetResponse) {
			if offset.Topic == topic {
				found = true
			}
		})

		if found {
			affected = append(affected, name)
		}
	}

	// Go map iteration decides the order above, so sort for a stable report.
	sort.Strings(affected)

	return affected, nil
}

func remove(ctx context.Context, admin *kadm.Client, topic string) error {
	responses, err := admin.DeleteTopics(ctx, topic)
	if err != nil {
		return fmt.Errorf("delete topic %q: %w", topic, err)
	}

	response, err := responses.On(topic, nil)
	if err != nil {
		return fmt.Errorf("delete topic %q: %w", topic, err)
	}

	if response.Err != nil {
		if errors.Is(response.Err, kerr.TopicAuthorizationFailed) {
			return fmt.Errorf(
				"not authorized to delete %q: the broker refused this request. Deleting a topic requires DELETE permission on it for the principal this server connects as: %w",
				topic, response.Err)
		}

		if response.ErrMessage != "" {
			return fmt.Errorf("delete topic %q: %w: %s", topic, response.Err, response.ErrMessage)
		}

		return fmt.Errorf("delete topic %q: %w", topic, response.Err)
	}

	return nil
}
