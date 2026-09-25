// Package createtopic implements the create_topic MCP tool.
package createtopic

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/denizgursoy/kafka-mcp/internal/domain/batch"
	"github.com/denizgursoy/kafka-mcp/internal/domain/kafkaclient"
)

// Item is one topic to create.
type Item struct {
	Topic string `json:"topic" jsonschema:"Name of the topic to create. An existing topic is refused rather than modified."`

	// Partitions and ReplicationFactor are optional because a broker has
	// defaults for both, and a wrong guess is worse than the default: the
	// partition count can never be reduced afterwards.
	Partitions        int               `json:"partitions,omitempty" jsonschema:"Optional number of partitions. On Kafka 2.4 or newer, omit to use the broker default; older brokers require a value. This can only ever be increased later, never reduced, so prefer the smallest count that meets the expected consumer parallelism."`
	ReplicationFactor int               `json:"replication_factor,omitempty" jsonschema:"Optional number of replicas per partition. On Kafka 2.4 or newer, omit to use the broker default; older brokers require a value. It cannot exceed the number of brokers in the cluster."`
	Configs           map[string]string `json:"configs,omitempty" jsonschema:"Optional topic-level configuration, such as retention.ms, cleanup.policy or max.message.bytes. Keys and values are passed to Kafka as given. Anything omitted is inherited from the cluster defaults."`
}

// brokerDefault is what Kafka reads as "choose for me" for the partition count
// and the replication factor. It is what an omitted parameter becomes, so a
// caller who does not care about either does not have to invent a number.
const brokerDefault = -1

// How long to keep asking the cluster what a freshly created topic looks like.
// A topic appears in metadata only once its partitions have leaders, so the
// first read after creation can legitimately find nothing.
const (
	readBackAttempts = 15
	readBackDelay    = 200 * time.Millisecond
)

// Input is the argument set accepted by the create_topic tool.
type Input struct {
	Items   []Item `json:"items" jsonschema:"The topics to create, 1 to 100 of them. Creating one topic is an array of length one. Duplicate topic names are refused before anything is created."`
	Confirm bool   `json:"confirm,omitempty" jsonschema:"Optional. When false or omitted, nothing is created: every item is validated by the broker and the response describes what would happen. Must be true to actually create. One confirm covers the whole batch."`
}

// Output is the result returned by the create_topic tool.
type Output struct {
	Topic             string            `json:"topic"`
	Partitions        int               `json:"partitions,omitempty"`
	ReplicationFactor int               `json:"replication_factor,omitempty"`
	Configs           map[string]string `json:"configs,omitempty"`
	Created           bool              `json:"created"`
	WouldCreate       bool              `json:"would_create"`
	Warnings          []string          `json:"warnings,omitempty"`
	Note              string            `json:"note,omitempty"`
}

type BatchOutput = batch.Output[Output]

const description = `
Create 1 to 100 Kafka topics in one call through items, each with optional
partition count, replication factor and topic-level configs. Omitted values use
broker defaults. Existing topics are refused rather than modified. Creating one
topic is an items array of length one.

No topic is created unless confirm is true; otherwise the broker only validates
the requests. One confirm covers the whole batch, and creation is not atomic:
topics created before a later item failed stay, because Kafka cannot roll them
back. Results follow items order, each carrying index with result or error.

Partition counts cannot be reduced later. Requires Kafka CREATE permission.
`

// Register adds the create_topic tool to the MCP server.
func Register(server *mcp.Server, kafka *kafkaclient.Client) {
	mcp.AddTool(
		server,
		&mcp.Tool{
			Name:        "create_topic",
			Description: description,
		},
		func(
			ctx context.Context,
			req *mcp.CallToolRequest,
			input Input,
		) (*mcp.CallToolResult, BatchOutput, error) {

			out, err := Run(ctx, kafka, input)
			if err != nil {
				return nil, BatchOutput{}, fmt.Errorf("create topics: %w", err)
			}

			return nil, out, nil
		},
	)
}

// Run validates every topic before creating any valid item. The creation phase
// is non-atomic because Kafka cannot roll successful topics back.
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
		return create(ctx, kafka, item, false)
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
		value, applyErr := create(ctx, kafka, item, true)
		if applyErr != nil {
			out.Results[index].Result = nil
			out.Results[index].Error = applyErr.Error()
			out.Failed++
			continue
		}
		out.Results[index].Result = &value
		out.Succeeded++
		if value.Created {
			out.Applied++
		}
	}
	return out, nil
}

// create validates or performs one topic creation.
func create(
	ctx context.Context,
	kafka *kafkaclient.Client,
	input Item,
	confirm bool,
) (Output, error) {

	if input.Topic == "" {
		return Output{}, fmt.Errorf("topic is required")
	}

	topic := input.Topic

	// Creating is this tool's only purpose, so a read-only cluster is refused
	// before anything else, preview included: a preview of a capability this
	// endpoint does not have would describe a change that can never happen.
	if err := kafka.RequireWritable("create_topic"); err != nil {
		return Output{}, err
	}

	partitions, err := count(input.Partitions, "partitions", math.MaxInt32)
	if err != nil {
		return Output{}, err
	}

	replication, err := count(input.ReplicationFactor, "replication_factor", math.MaxInt16)
	if err != nil {
		return Output{}, err
	}

	admin := kafka.Admin()

	if err := replicationFits(ctx, admin, replication); err != nil {
		return Output{}, err
	}

	// kadm takes pointers so that a nil value can mean "delete this config",
	// which is why the values cannot be passed as a plain string map.
	configs := make(map[string]*string, len(input.Configs))
	for key, value := range input.Configs {
		configs[key] = kadm.StringPtr(value)
	}

	out := Output{
		Topic:    topic,
		Configs:  input.Configs,
		Warnings: warnings(input),
	}

	if !confirm {
		// ValidateOnly asks the broker the same question without creating
		// anything, so the preview reports the cluster's answer rather than a
		// guess made here about what the cluster would allow.
		responses, err := admin.ValidateCreateTopics(
			ctx, int32(partitions), int16(replication), configs, topic)
		if err != nil {
			return Output{}, fmt.Errorf("validate topic %q: %w", topic, err)
		}

		response, err := result(responses, topic)
		if err != nil {
			return Output{}, err
		}

		out.WouldCreate = true
		out.Partitions = reported(response.NumPartitions, input.Partitions)
		out.ReplicationFactor = reported(int32(response.ReplicationFactor), input.ReplicationFactor)
		out.Note = "nothing was created. The broker accepted this request, so calling again with confirm true would create the topic."

		return out, nil
	}

	responses, err := admin.CreateTopics(
		ctx, int32(partitions), int16(replication), configs, topic)
	if err != nil {
		return Output{}, fmt.Errorf("create topic %q: %w", topic, err)
	}

	response, err := result(responses, topic)
	if err != nil {
		return Output{}, err
	}

	// What the topic ended up with is what the cluster says it has, not what
	// was asked for, which is the only way an omitted count is reported as
	// the number the broker actually chose.
	created, replicas, err := settled(ctx, admin, topic, response)
	if err != nil {
		return Output{}, err
	}

	out.Created = true
	out.Partitions = created
	out.ReplicationFactor = replicas
	out.Note = fmt.Sprintf(
		"topic %q was created with %d partition(s) and replication factor %d",
		topic, created, replicas)

	return out, nil
}

// count turns an optional count into what Kafka expects, where -1 means the
// broker chooses.
func count(value int, field string, maximum int) (int, error) {
	// An omitted field arrives as zero, and a topic with no partitions is not
	// a thing Kafka can make, so zero can only mean "unspecified".
	if value == 0 || value == brokerDefault {
		return brokerDefault, nil
	}

	if value < 0 {
		return 0, fmt.Errorf(
			"%s must be a positive number, or omitted to use the broker default, got %d",
			field, value)
	}

	if value > maximum {
		return 0, fmt.Errorf(
			"%s must be at most %d, got %d",
			field, maximum, value)
	}

	return value, nil
}

// result pulls the topic's own response out and turns a refusal into an error
// a caller can act on.
func result(responses kadm.CreateTopicResponses, topic string) (kadm.CreateTopicResponse, error) {
	response, err := responses.On(topic, nil)
	if err != nil {
		return kadm.CreateTopicResponse{}, fmt.Errorf("create topic %q: %w", topic, err)
	}

	if response.Err == nil {
		return response, nil
	}

	// Authorization failures arrive in the response rather than as a returned
	// error, so they would otherwise pass for success.
	if errors.Is(response.Err, kerr.TopicAuthorizationFailed) ||
		errors.Is(response.Err, kerr.ClusterAuthorizationFailed) {

		return kadm.CreateTopicResponse{}, fmt.Errorf(
			"not authorized to create %q: the broker refused this request. Creating a topic requires CREATE permission on the topic or the cluster for the principal this server connects as: %w",
			topic, response.Err)
	}

	if errors.Is(response.Err, kerr.TopicAlreadyExists) {
		return kadm.CreateTopicResponse{}, fmt.Errorf(
			"topic %q already exists: create_topic never changes an existing topic. Use add_partitions to change its partition count",
			topic)
	}

	if response.ErrMessage != "" {
		return kadm.CreateTopicResponse{}, fmt.Errorf(
			"create topic %q: %w: %s", topic, response.Err, response.ErrMessage)
	}

	return kadm.CreateTopicResponse{}, fmt.Errorf("create topic %q: %w", topic, response.Err)
}

// replicationFits refuses a replication factor the cluster cannot satisfy.
//
// Brokers differ on whether a validate-only request catches this, and a
// preview that says an impossible request would work is worse than no preview
// at all, so the count is checked here rather than left to the cluster.
func replicationFits(ctx context.Context, admin *kadm.Client, replication int) error {
	if replication == brokerDefault {
		return nil
	}

	brokers, err := admin.ListBrokers(ctx)
	if err != nil {
		return fmt.Errorf("count the brokers in the cluster: %w", err)
	}

	if replication <= len(brokers) {
		return nil
	}

	return fmt.Errorf(
		"replication_factor %d exceeds the %d broker(s) in this cluster: every replica of a partition must live on a different broker, so this topic cannot be created. Use at most %d, or omit it for the broker default",
		replication, len(brokers), len(brokers))
}

// settled reports the partition count and replication factor the topic ended
// up with.
//
// The create response carries both from Kafka 2.4 onwards. Where it does not,
// the topic is read back instead, with retries: a topic is visible in metadata
// only once its partitions have leaders, which is not instant, and reporting a
// count of zero for a topic that was created would be a lie about the one
// number that cannot be changed downwards later.
func settled(
	ctx context.Context,
	admin *kadm.Client,
	topic string,
	response kadm.CreateTopicResponse,
) (int, int, error) {

	if response.NumPartitions > 0 && response.ReplicationFactor > 0 {
		return int(response.NumPartitions), int(response.ReplicationFactor), nil
	}

	var lastErr error

	for attempt := range readBackAttempts {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return 0, 0, ctx.Err()
			case <-time.After(readBackDelay):
			}
		}

		details, err := admin.ListTopics(ctx, topic)
		if err != nil {
			lastErr = err

			continue
		}

		detail, ok := details[topic]
		if !ok {
			lastErr = fmt.Errorf("the cluster does not list it yet")

			continue
		}

		if detail.Err != nil {
			lastErr = detail.Err

			continue
		}

		replicas := 0

		for _, partition := range detail.Partitions {
			if len(partition.Replicas) > replicas {
				replicas = len(partition.Replicas)
			}
		}

		return len(detail.Partitions), replicas, nil
	}

	return 0, 0, fmt.Errorf(
		"topic %q was created but cannot be read back, so its layout is unconfirmed: %w", topic, lastErr)
}

// reported prefers what the broker said over what was asked for, and falls
// back to the request when the broker answers an older protocol version that
// carries neither count.
func reported(fromBroker int32, requested int) int {
	if fromBroker > 0 {
		return int(fromBroker)
	}

	if requested > 0 {
		return requested
	}

	return 0
}

// warnings states what the caller cannot take back later.
func warnings(input Item) []string {
	warnings := make([]string, 0, 3)

	if input.Partitions > 0 {
		warnings = append(warnings,
			"the partition count can be increased later but never reduced, and increasing it moves keyed messages to different partitions")
	} else {
		warnings = append(warnings,
			"partitions was not given, so the broker's default partition count is used")
	}

	if input.ReplicationFactor <= 0 {
		warnings = append(warnings,
			"replication_factor was not given, so the broker's default replication factor is used")
	}

	if len(input.Configs) > 0 {
		keys := make([]string, 0, len(input.Configs))
		for key := range input.Configs {
			keys = append(keys, key)
		}

		// Go map order is random, and a warning that reorders itself between
		// two calls reads as a different warning.
		sort.Strings(keys)

		warnings = append(warnings, fmt.Sprintf(
			"topic configs %v are set at creation and can be changed later on the topic itself", keys))
	}

	return warnings
}
