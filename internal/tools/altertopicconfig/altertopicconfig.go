// Package altertopicconfig implements the alter_topic_config MCP tool.
package altertopicconfig

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/denizgursoy/kafka-mcp/internal/domain/batch"
	"github.com/denizgursoy/kafka-mcp/internal/domain/kafkaclient"
	"github.com/denizgursoy/kafka-mcp/internal/domain/topicconfig"
)

// Item is one topic whose configuration changes.
type Item struct {
	Topic  string            `json:"topic" jsonschema:"Topic to change. Matched exactly and case-sensitively."`
	Set    map[string]string `json:"set,omitempty" jsonschema:"Optional topic-level configs to set, such as retention.ms, cleanup.policy or max.message.bytes. Keys and values are passed to Kafka as given. Every key not named here keeps its current value."`
	Delete []string          `json:"delete,omitempty" jsonschema:"Optional config keys whose topic-level override is removed, so the topic inherits the cluster default again. A key may not appear in both set and delete."`
}

// Input is the argument set accepted by the alter_topic_config tool.
type Input struct {
	Items   []Item `json:"items" jsonschema:"The topics to change, 1 to 100 of them. Changing one topic is an array of length one. Duplicate topic names are refused before anything changes."`
	Confirm bool   `json:"confirm,omitempty" jsonschema:"Optional. When false or omitted, nothing is changed: the broker validates every item and the response shows each key's current and requested value. Must be true to apply. One confirm covers the whole batch."`
}

// Change is what happens to one key.
type Change struct {
	Key           string `json:"key"`
	Action        string `json:"action"`
	Current       string `json:"current"`
	CurrentSource string `json:"current_source"`
	// Requested is empty for a delete, whose new value is whatever the
	// cluster default turns out to be.
	Requested       string `json:"requested,omitempty"`
	Resulting       string `json:"resulting,omitempty"`
	ResultingSource string `json:"resulting_source,omitempty"`
	Sensitive       bool   `json:"sensitive,omitempty"`
}

// Output is the result of one item.
type Output struct {
	Topic   string   `json:"topic"`
	Changes []Change `json:"changes"`

	// MessagesPastRetention is set when retention.ms is shortened: how many
	// messages are already older than the new limit and become eligible for
	// deletion as soon as it applies.
	MessagesPastRetention int64 `json:"messages_past_retention,omitempty"`

	Applied  bool     `json:"applied"`
	Warnings []string `json:"warnings"`
	Note     string   `json:"note,omitempty"`
}

type BatchOutput = batch.Output[Output]

// Actions reported in Change.Action.
const (
	ActionSet    = "set"
	ActionDelete = "delete"
)

const description = `
Change topic-level configuration of 1 to 100 topics in one call through items.
Each item sets keys (set) and removes overrides so the cluster default applies
again (delete). Every key not named keeps its value: changes are incremental,
never a full replace. Changing one topic is an items array of length one.

No change is made unless confirm is true. The preview asks the broker to
validate every item and returns, per key, the current value and its source
(DYNAMIC_TOPIC_CONFIG means a deliberate topic setting; anything else is
inherited) next to the requested value. Shortening retention.ms reports how many
messages are already older than the new limit, because they become eligible for
deletion straight away. Changing cleanup.policy or retention.bytes is warned
about.

One confirm covers the whole batch, and applying is not atomic: topics changed
before a later item failed stay changed. Results follow items order, each
carrying index with result or error. Requires Kafka ALTER_CONFIGS permission.
`

// Register adds the alter_topic_config tool to the MCP server.
func Register(server *mcp.Server, kafka *kafkaclient.Client) {
	mcp.AddTool(
		server,
		&mcp.Tool{
			Name:        "alter_topic_config",
			Description: description,
		},
		func(
			ctx context.Context,
			req *mcp.CallToolRequest,
			input Input,
		) (*mcp.CallToolResult, BatchOutput, error) {

			out, err := Run(ctx, kafka, input)
			if err != nil {
				return nil, BatchOutput{}, fmt.Errorf("alter topic config: %w", err)
			}

			return nil, out, nil
		},
	)
}

// Run previews every item before applying any valid one. Applying is not
// atomic: Kafka cannot roll back a config change that succeeded before a later
// item failed.
func Run(ctx context.Context, kafka *kafkaclient.Client, input Input) (BatchOutput, error) {
	items, confirm := input.Items, input.Confirm

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
		return alter(ctx, kafka, item, false)
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
		value, applyErr := alter(ctx, kafka, item, true)
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

// alter previews or applies one topic's change.
func alter(ctx context.Context, kafka *kafkaclient.Client, input Item, confirm bool) (Output, error) {
	// Changing configuration is this tool's only purpose, so a read-only
	// endpoint is refused before anything else, preview included.
	if err := kafka.RequireWritable("alter_topic_config"); err != nil {
		return Output{}, err
	}

	operations, err := operations(input)
	if err != nil {
		return Output{}, err
	}

	admin := kafka.Admin()

	if err := exists(ctx, admin, input.Topic); err != nil {
		return Output{}, err
	}

	before, err := topicconfig.For(ctx, admin, input.Topic)
	if err != nil {
		return Output{}, err
	}

	current := byKey(before)

	out := Output{
		Topic:    input.Topic,
		Changes:  make([]Change, 0, len(operations)),
		Warnings: []string{},
	}

	for _, operation := range operations {
		entry := current[operation.Name]

		change := Change{
			Key:           operation.Name,
			Action:        ActionSet,
			Current:       entry.Value,
			CurrentSource: entry.Source,
			Sensitive:     entry.Sensitive,
		}

		if operation.Op == kadm.DeleteConfig {
			change.Action = ActionDelete
		} else {
			change.Requested = *operation.Value
		}

		out.Changes = append(out.Changes, change)
	}

	if err := warn(ctx, admin, input, current, &out); err != nil {
		return Output{}, err
	}

	if !confirm {
		// The broker answers the same request without applying it, so the
		// preview reports what the cluster would accept rather than a guess.
		responses, err := admin.ValidateAlterTopicConfigs(ctx, operations, input.Topic)
		if err := responseError(input.Topic, responses, err); err != nil {
			return Output{}, err
		}

		out.Note = "nothing was changed. The broker accepted this request, so calling again with confirm true would apply it."

		return out, nil
	}

	responses, err := admin.AlterTopicConfigs(ctx, operations, input.Topic)
	if err := responseError(input.Topic, responses, err); err != nil {
		return Output{}, err
	}

	// Re-read rather than assume: what the topic holds now is what the broker
	// reports, and for a delete the inherited value is only known this way.
	after, err := topicconfig.For(ctx, admin, input.Topic)
	if err != nil {
		return Output{}, fmt.Errorf("the change was applied but could not be read back: %w", err)
	}

	resulting := byKey(after)

	for index := range out.Changes {
		entry := resulting[out.Changes[index].Key]
		out.Changes[index].Resulting = entry.Value
		out.Changes[index].ResultingSource = entry.Source
	}

	out.Applied = true
	out.Note = fmt.Sprintf("%d config key(s) of %q changed", len(out.Changes), input.Topic)

	return out, nil
}

// operations turns an item into the incremental operations Kafka applies, in a
// stable order.
func operations(input Item) ([]kadm.AlterConfig, error) {
	if input.Topic == "" {
		return nil, fmt.Errorf("topic is required")
	}

	if len(input.Set) == 0 && len(input.Delete) == 0 {
		return nil, fmt.Errorf("give at least one key in set or delete: an item that changes nothing is a mistake in the request")
	}

	operations := make([]kadm.AlterConfig, 0, len(input.Set)+len(input.Delete))

	for key, value := range input.Set {
		if key == "" {
			return nil, fmt.Errorf("set contains an empty key")
		}

		operations = append(operations, kadm.AlterConfig{Op: kadm.SetConfig, Name: key, Value: kadm.StringPtr(value)})
	}

	deleted := make(map[string]bool, len(input.Delete))

	for _, key := range input.Delete {
		if key == "" {
			return nil, fmt.Errorf("delete contains an empty key")
		}

		if _, ok := input.Set[key]; ok {
			return nil, fmt.Errorf("key %q is in both set and delete, which has no single meaning", key)
		}

		if deleted[key] {
			continue
		}

		deleted[key] = true

		operations = append(operations, kadm.AlterConfig{Op: kadm.DeleteConfig, Name: key})
	}

	sort.Slice(operations, func(i, j int) bool { return operations[i].Name < operations[j].Name })

	return operations, nil
}

// exists refuses a missing topic with a plain answer, rather than the broker's
// error for describing configs of a resource it does not have.
func exists(ctx context.Context, admin *kadm.Client, topic string) error {
	details, err := admin.ListTopics(ctx, topic)
	if err != nil {
		return fmt.Errorf("list topic %q: %w", topic, err)
	}

	detail, ok := details[topic]
	if !ok || errors.Is(detail.Err, kerr.UnknownTopicOrPartition) {
		return fmt.Errorf("topic %q does not exist", topic)
	}

	if detail.Err != nil {
		return fmt.Errorf("topic %q: %w", topic, detail.Err)
	}

	return nil
}

// warn explains what a change does to data that is already in the topic.
func warn(
	ctx context.Context,
	admin *kadm.Client,
	input Item,
	current map[string]topicconfig.Entry,
	out *Output,
) error {

	if value, ok := input.Set["retention.ms"]; ok {
		retention, err := strconv.ParseInt(value, 10, 64)

		// A value that does not parse is left to the broker to refuse, so the
		// error the caller sees is Kafka's own.
		if err == nil && retention >= 0 {
			old, _ := strconv.ParseInt(current["retention.ms"].Value, 10, 64)

			if old < 0 || retention < old {
				expired, err := olderThan(ctx, admin, input.Topic, time.Duration(retention)*time.Millisecond)
				if err != nil {
					return err
				}

				out.MessagesPastRetention = expired

				if expired > 0 {
					out.Warnings = append(out.Warnings, fmt.Sprintf(
						"%d message(s) are already older than the new retention of %s and become eligible for deletion as soon as it applies; Kafka cannot bring them back",
						expired, time.Duration(retention)*time.Millisecond))
				}
			}
		}
	}

	if value, ok := input.Set["retention.bytes"]; ok {
		out.Warnings = append(out.Warnings, fmt.Sprintf(
			"retention.bytes limits each partition's size; setting it to %s may delete the oldest segments of any partition already larger than that",
			value))
	}

	if value, ok := input.Set["cleanup.policy"]; ok && value != current["cleanup.policy"].Value {
		out.Warnings = append(out.Warnings, fmt.Sprintf(
			"cleanup.policy changes from %q to %q. compact keeps only the latest message per key and removes messages without a key; delete removes messages by age and size regardless of key. Consumers that rely on either behaviour change with it",
			current["cleanup.policy"].Value, value))
	}

	return nil
}

// olderThan counts the messages with a timestamp older than age, which is
// what a retention of age would make eligible for deletion.
func olderThan(ctx context.Context, admin *kadm.Client, topic string, age time.Duration) (int64, error) {
	cutoff := time.Now().Add(-age).UnixMilli()

	after, err := admin.ListOffsetsAfterMilli(ctx, cutoff, topic)
	if err != nil {
		return 0, fmt.Errorf("list offsets after the new retention for %q: %w", topic, err)
	}

	starts, err := admin.ListStartOffsets(ctx, topic)
	if err != nil {
		return 0, fmt.Errorf("list start offsets for %q: %w", topic, err)
	}

	var total int64

	starts.Each(func(start kadm.ListedOffset) {
		if start.Err != nil {
			return
		}

		listed, ok := after.Lookup(topic, start.Partition)
		if !ok || listed.Err != nil || listed.Offset < start.Offset {
			return
		}

		total += listed.Offset - start.Offset
	})

	return total, nil
}

func byKey(entries []topicconfig.Entry) map[string]topicconfig.Entry {
	out := make(map[string]topicconfig.Entry, len(entries))
	for _, entry := range entries {
		out[entry.Key] = entry
	}

	return out
}

// responseError turns the broker's answer for one topic into an error a
// caller can act on. Refusals arrive in the response rather than as a returned
// error, so they would otherwise pass for success.
func responseError(topic string, responses kadm.AlterConfigsResponses, err error) error {
	if err != nil {
		return fmt.Errorf("alter configs of %q: %w", topic, err)
	}

	response, err := responses.On(topic, nil)
	if err != nil {
		return fmt.Errorf("alter configs of %q: %w", topic, err)
	}

	if response.Err == nil {
		return nil
	}

	if errors.Is(response.Err, kerr.TopicAuthorizationFailed) ||
		errors.Is(response.Err, kerr.ClusterAuthorizationFailed) {

		return fmt.Errorf(
			"not authorized to change the configuration of %q: the broker refused this request. It needs ALTER_CONFIGS permission on the topic for the principal this server connects as: %w",
			topic, response.Err)
	}

	if response.ErrMessage != "" {
		return fmt.Errorf("alter configs of %q: %w: %s", topic, response.Err, response.ErrMessage)
	}

	return fmt.Errorf("alter configs of %q: %w", topic, response.Err)
}
