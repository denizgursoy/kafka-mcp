// Package addpartitions implements the add_partitions MCP tool.
package addpartitions

import (
	"context"
	"errors"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/denizgursoy/kafka-mcp/internal/domain/batch"
	"github.com/denizgursoy/kafka-mcp/internal/domain/kafkaclient"
	"github.com/denizgursoy/kafka-mcp/internal/domain/records"
)

// Item is one topic partition target in a batch request.
type Item struct {
	Topic                  string `json:"topic" jsonschema:"Topic to change."`
	Partitions             int    `json:"partitions" jsonschema:"Final total partition count."`
	AcknowledgeKeyOrdering bool   `json:"acknowledge_key_ordering,omitempty" jsonschema:"Required when sampled messages carry keys."`
	SampleSize             int    `json:"sample_size,omitempty" jsonschema:"Optional recent messages to inspect for keys. Defaults to 20."`
}

// Input is the argument set accepted by the add_partitions tool.
type Input struct {
	Topic string `json:"topic,omitempty" jsonschema:"Topic to change for a single operation. Omit when items is used."`
	// Partitions is the final count rather than a number to add, so calling
	// twice with the same value does not add twice.
	Partitions             int    `json:"partitions" jsonschema:"The total number of partitions the topic should end up with. This is the final count, not the number to add, so repeating the same call is safe. Must be greater than the current count: Kafka cannot remove partitions."`
	Confirm                bool   `json:"confirm,omitempty" jsonschema:"Optional. When false or omitted, nothing is changed and the response describes what would happen. Must be true to actually add partitions."`
	AcknowledgeKeyOrdering bool   `json:"acknowledge_key_ordering,omitempty" jsonschema:"Optional. Required when the topic holds keyed messages. Adding partitions changes which partition a key maps to, so existing keys lose their ordering guarantee. Set this to true to confirm that is acceptable."`
	SampleSize             int    `json:"sample_size,omitempty" jsonschema:"Optional number of recent messages to inspect when checking whether the topic is keyed. Defaults to 20."`
	Items                  []Item `json:"items,omitempty" jsonschema:"Optional batch of 1 to 100 topic targets. Do not combine with single-operation fields. confirm applies to the whole batch; successful changes cannot be rolled back."`
}

// Output is the result returned by the add_partitions tool.
type Output struct {
	Topic               string   `json:"topic"`
	CurrentPartitions   int      `json:"current_partitions"`
	RequestedPartitions int      `json:"requested_partitions"`
	ResultingPartitions int      `json:"resulting_partitions,omitempty"`
	Applied             bool     `json:"applied"`
	WouldApply          bool     `json:"would_apply"`
	KeyedMessages       bool     `json:"keyed_messages"`
	SampledMessages     int      `json:"sampled_messages"`
	ConsumerGroups      []string `json:"consumer_groups,omitempty"`
	Warnings            []string `json:"warnings,omitempty"`
	Note                string   `json:"note,omitempty"`
}

type BatchOutput = batch.Output[Output]

type Response struct {
	*Output
	*BatchOutput
}

const defaultSampleSize = 20

const description = `
Increase one topic's partition count, or up to 100 topics through items, and report the current count, affected
consumer groups, sampled key usage and warnings. Kafka cannot remove
partitions, and changing the count can break ordering for keyed messages.

No change is made unless confirm is true. Keyed samples also require
acknowledge_key_ordering. Requires Kafka ALTER permission.
`

// Register adds the add_partitions tool to the MCP server.
func Register(server *mcp.Server, kafka *kafkaclient.Client, reader *records.Reader) {
	mcp.AddTool(
		server,
		&mcp.Tool{
			Name:         "add_partitions",
			Description:  description,
			OutputSchema: batch.OutputSchema[Output](),
		},
		func(
			ctx context.Context,
			req *mcp.CallToolRequest,
			input Input,
		) (*mcp.CallToolResult, Response, error) {

			if input.Items != nil {
				if input.Topic != "" || input.Partitions != 0 || input.AcknowledgeKeyOrdering || input.SampleSize != 0 {
					return nil, Response{}, fmt.Errorf("add partitions: items cannot be combined with single-operation fields")
				}
				out, err := RunBatch(ctx, kafka, reader, input.Items, input.Confirm)
				if err != nil {
					return nil, Response{}, fmt.Errorf("add partitions batch: %w", err)
				}
				return nil, Response{BatchOutput: &out}, nil
			}

			out, err := Run(ctx, kafka, reader, input)
			if err != nil {
				return nil, Response{}, fmt.Errorf("add partitions: %w", err)
			}

			return nil, Response{Output: &out}, nil
		},
	)
}

// RunBatch previews every topic before applying valid changes. Changes are
// non-atomic because Kafka cannot remove partitions to roll a successful item
// back.
func RunBatch(ctx context.Context, kafka *kafkaclient.Client, reader *records.Reader, items []Item, confirm bool) (BatchOutput, error) {
	if err := batch.Validate(len(items), batch.MaxItems); err != nil {
		return BatchOutput{}, err
	}
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		if _, ok := seen[item.Topic]; ok {
			return BatchOutput{}, fmt.Errorf("duplicate topic %q in items", item.Topic)
		}
		seen[item.Topic] = struct{}{}
	}

	out, err := batch.Run(ctx, items, batch.MaxItems, func(ctx context.Context, item Item) (Output, error) {
		return Run(ctx, kafka, reader, addInput(item, false))
	})
	if err != nil || !confirm {
		return out, err
	}

	out.Succeeded, out.Failed = 0, 0
	for index, item := range items {
		preview := out.Results[index]
		if preview.Error != "" {
			out.Failed++
			continue
		}
		if preview.Result.KeyedMessages && !item.AcknowledgeKeyOrdering {
			out.Results[index].Result = nil
			out.Results[index].Error = fmt.Sprintf("refusing to add partitions to %q: acknowledge_key_ordering is required", item.Topic)
			out.Failed++
			continue
		}
		value, applyErr := Run(ctx, kafka, reader, addInput(item, true))
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

func addInput(item Item, confirm bool) Input {
	return Input{Topic: item.Topic, Partitions: item.Partitions, Confirm: confirm,
		AcknowledgeKeyOrdering: item.AcknowledgeKeyOrdering, SampleSize: item.SampleSize}
}

// Run previews or applies a partition count change.
func Run(
	ctx context.Context,
	kafka *kafkaclient.Client,
	reader *records.Reader,
	input Input,
) (Output, error) {

	if input.Topic == "" {
		return Output{}, fmt.Errorf("topic is required")
	}

	// A zero target almost always means the parameter was omitted, and
	// treating it as "no partitions" would be the worst possible reading.
	if input.Partitions <= 0 {
		return Output{}, fmt.Errorf(
			"partitions must be a positive number, got %d: it is the total count the topic should end up with",
			input.Partitions)
	}

	admin := kafka.Admin()

	current, err := currentPartitions(ctx, admin, input.Topic)
	if err != nil {
		return Output{}, err
	}

	out := Output{
		Topic:               input.Topic,
		CurrentPartitions:   current,
		RequestedPartitions: input.Partitions,
		Warnings:            []string{},
	}

	// Checked before anything else, and in a dry run too, so a preview never
	// suggests an impossible change is worth confirming.
	if input.Partitions < current {
		return Output{}, fmt.Errorf(
			"cannot reduce partitions on %q from %d to %d: Kafka can only add partitions, never remove them. Reducing requires creating a new topic and migrating the data",
			input.Topic, current, input.Partitions)
	}

	if input.Partitions == current {
		out.Note = fmt.Sprintf(
			"topic %q already has %d partitions, so there is nothing to do",
			input.Topic, current)

		return out, nil
	}

	keyed, sampled, err := sampleKeys(ctx, admin, reader, input)
	if err != nil {
		return Output{}, err
	}

	out.KeyedMessages = keyed
	out.SampledMessages = sampled

	groups, err := affectedGroups(ctx, admin, input.Topic)
	if err != nil {
		return Output{}, err
	}

	out.ConsumerGroups = groups

	if keyed {
		out.Warnings = append(out.Warnings, fmt.Sprintf(
			"messages in %q carry keys: after this change a key will hash to a different partition, so existing keys lose their ordering guarantee",
			input.Topic))
	}

	if len(groups) > 0 {
		out.Warnings = append(out.Warnings, fmt.Sprintf(
			"%d consumer group(s) read this topic and will rebalance: %v", len(groups), groups))
	}

	out.Warnings = append(out.Warnings,
		"this cannot be undone: Kafka cannot reduce a topic's partition count")

	out.WouldApply = true

	if !input.Confirm {
		out.Note = "nothing was changed. Call again with confirm true to apply."

		return out, nil
	}

	if keyed && !input.AcknowledgeKeyOrdering {
		return Output{}, fmt.Errorf(
			"refusing to add partitions to %q: its messages are keyed, so this would break ordering for existing keys. Set acknowledge_key_ordering true to confirm that is acceptable",
			input.Topic)
	}

	// The read-only check sits here, immediately before the only call that
	// changes anything, so every earlier refusal is reported on its own terms.
	if err := kafka.RequireWritable("add_partitions"); err != nil {
		return Output{}, err
	}

	if err := apply(ctx, admin, input.Topic, input.Partitions); err != nil {
		return Output{}, err
	}

	// Re-read rather than assume: the resulting count is what the broker says
	// it is, not what was asked for.
	resulting, err := currentPartitions(ctx, admin, input.Topic)
	if err != nil {
		return Output{}, err
	}

	out.Applied = true
	out.ResultingPartitions = resulting
	out.Note = fmt.Sprintf("topic %q now has %d partitions", input.Topic, resulting)

	return out, nil
}

func apply(ctx context.Context, admin *kadm.Client, topic string, partitions int) error {
	responses, err := admin.UpdatePartitions(ctx, partitions, topic)
	if err != nil {
		return fmt.Errorf("add partitions to %q: %w", topic, err)
	}

	response, err := responses.On(topic, nil)
	if err != nil {
		return fmt.Errorf("add partitions to %q: %w", topic, err)
	}

	// Authorization failures arrive in the response rather than as a returned
	// error, so they would otherwise pass for success.
	if response.Err != nil {
		if isAuthError(response.Err) {
			return fmt.Errorf(
				"not authorized to add partitions to %q: the broker refused this request. Adding partitions requires ALTER permission on the topic for the principal this server connects as: %w",
				topic, response.Err)
		}

		if response.ErrMessage != "" {
			return fmt.Errorf(
				"add partitions to %q: %w: %s", topic, response.Err, response.ErrMessage)
		}

		return fmt.Errorf("add partitions to %q: %w", topic, response.Err)
	}

	return nil
}

func currentPartitions(ctx context.Context, admin *kadm.Client, topic string) (int, error) {
	details, err := admin.ListTopics(ctx, topic)
	if err != nil {
		return 0, fmt.Errorf("list topic %q: %w", topic, err)
	}

	detail, ok := details[topic]
	if !ok {
		return 0, fmt.Errorf("topic %q does not exist", topic)
	}

	if detail.Err != nil {
		return 0, fmt.Errorf("topic %q: %w", topic, detail.Err)
	}

	return len(detail.Partitions), nil
}

// sampleKeys reports whether recent messages carry keys, which is what decides
// if this change can break ordering.
func sampleKeys(
	ctx context.Context,
	admin *kadm.Client,
	reader *records.Reader,
	input Input,
) (bool, int, error) {

	size := input.SampleSize
	if size <= 0 {
		size = defaultSampleSize
	}

	starts, err := admin.ListStartOffsets(ctx, input.Topic)
	if err != nil {
		return false, 0, fmt.Errorf("list start offsets for %q: %w", input.Topic, err)
	}

	ends, err := admin.ListEndOffsets(ctx, input.Topic)
	if err != nil {
		return false, 0, fmt.Errorf("list end offsets for %q: %w", input.Topic, err)
	}

	ranges := make([]records.Range, 0)

	ends.Each(func(end kadm.ListedOffset) {
		if end.Err != nil {
			return
		}

		start, ok := starts.Lookup(input.Topic, end.Partition)
		if !ok || start.Err != nil || end.Offset <= start.Offset {
			return
		}

		from := end.Offset - int64(size)
		if from < start.Offset {
			from = start.Offset
		}

		ranges = append(ranges, records.Range{
			Partition: end.Partition,
			Start:     from,
			End:       end.Offset,
		})
	})

	if len(ranges) == 0 {
		return false, 0, nil
	}

	var (
		keyed   bool
		scanned int
	)

	err = reader.Scan(ctx, input.Topic, ranges, func(record *kgo.Record) bool {
		scanned++

		if len(record.Key) > 0 {
			keyed = true

			// One keyed message is enough to establish the risk.
			return false
		}

		return scanned < size
	})
	if err != nil {
		return false, 0, fmt.Errorf("sample %q for keys: %w", input.Topic, err)
	}

	return keyed, scanned, nil
}

// affectedGroups lists the consumer groups that will rebalance.
func affectedGroups(ctx context.Context, admin *kadm.Client, topic string) ([]string, error) {
	listed, err := admin.ListGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("list groups: %w", err)
	}

	names := listed.Groups()
	if len(names) == 0 {
		return nil, nil
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

	return affected, nil
}

// isAuthError reports whether the broker refused the request on permission
// grounds, which needs a different answer from an ordinary failure: an ACL,
// not a retry.
func isAuthError(err error) bool {
	return errors.Is(err, kerr.TopicAuthorizationFailed) ||
		errors.Is(err, kerr.ClusterAuthorizationFailed)
}
