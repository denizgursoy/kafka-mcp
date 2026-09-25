// Package producemessage implements the produce_message MCP tool.
package producemessage

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/denizgursoy/kafka-mcp/internal/domain/batch"
	"github.com/denizgursoy/kafka-mcp/internal/domain/kafkaclient"
	"github.com/denizgursoy/kafka-mcp/internal/domain/records"
)

// Provenance headers added to every produced message, so a record this server
// invented is never mistaken for one a real producer sent.
const (
	headerProducedAt       = "kafka-mcp-produced-at"
	headerProducedByTool   = "kafka-mcp-produced-by-tool"
	headerProducedByUser   = "kafka-mcp-produced-by-principal"
	headerProducedByClient = "kafka-mcp-produced-by-client"

	toolName = "produce_message"
)

// Encodings accepted for the key and value.
const (
	encodingUTF8   = "utf8"
	encodingBase64 = "base64"
)

// Item is one message to write.
type Item struct {
	Topic              string            `json:"topic" jsonschema:"Topic to write to. It must already exist."`
	Value              string            `json:"value" jsonschema:"The message body. Interpreted as text unless encoding is base64."`
	Key                string            `json:"key,omitempty" jsonschema:"Optional message key. The key decides which partition the message lands on when partition is omitted, so re-injecting a repaired message with its original key keeps that key's ordering."`
	Headers            map[string]string `json:"headers,omitempty" jsonschema:"Optional message headers, as a map of name to value. A header that collides with a provenance header is kept as given and the collision is reported."`
	Partition          *int32            `json:"partition,omitempty" jsonschema:"Optional exact partition to write to. Omit to let the key decide, which is almost always what you want: naming a partition puts a keyed message somewhere its key does not hash to, breaking that key's ordering."`
	Encoding           string            `json:"encoding,omitempty" jsonschema:"Optional encoding of key and value: utf8 (default) or base64. Use base64 to write a binary payload such as protobuf or Avro, which cannot survive being carried as text."`
	DestinationCluster string            `json:"destination_cluster,omitempty" jsonschema:"Optional cluster to write to. Defaults to the cluster this endpoint serves. Use list_clusters to see which names are valid. The destination must not be read-only; the endpoint you called may be."`
	MaxValueBytes      int               `json:"max_value_bytes,omitempty" jsonschema:"Optional maximum number of value bytes to show in the response. Defaults to 4096. This affects the preview only: the whole value is always written."`
}

// Input is the argument set accepted by the produce_message tool.
//
// Unlike copy_message, this writes content the caller supplies, so it can put a
// message into a topic that no producer ever sent. Provenance headers are
// therefore not optional: they are what keeps a fabricated message
// distinguishable from a genuine one.
type Input struct {
	Items   []Item `json:"items" jsonschema:"The messages to write, 1 to 20 of them. Writing one message is an array of length one. Two identical items mean two messages, which is allowed: appending the same payload twice is a real request."`
	Confirm bool   `json:"confirm,omitempty" jsonschema:"Optional. When false or omitted, nothing is written and the response shows the messages that would be produced. Must be true to actually write them. One confirm covers the whole batch."`
}

// Output is the result returned by the produce_message tool.
type Output struct {
	DestinationCluster string          `json:"destination_cluster"`
	Topic              string          `json:"topic"`
	Message            records.Message `json:"message"`
	Applied            bool            `json:"applied"`
	WrittenPartition   int32           `json:"written_partition"`
	WrittenOffset      int64           `json:"written_offset"`
	ProvenanceHeaders  []string        `json:"provenance_headers,omitempty"`
	Warnings           []string        `json:"warnings,omitempty"`
	Note               string          `json:"note,omitempty"`
}

type BatchOutput = batch.Output[Output]

const description = `
Write 1 to 20 new messages in one call through items, to existing topics on this
or another configured cluster. The caller supplies the key, value and headers,
so unlike copy_message this can write content no producer ever sent. Writing one
message is an items array of length one.

Every message carries provenance headers naming this tool, the time and the
principal, so a fabricated message stays distinguishable from a genuine one.

Nothing is written unless confirm is true; one confirm covers the whole batch,
and writing is not atomic because Kafka cannot retract a record produced before
a later item failed. Results follow items order, each carrying index with result
or error.

A produced message cannot be deleted: it stays until retention removes it, and
any consumer reading the topic will process it. The destination must be writable
and requires Kafka write permission; the endpoint you call may itself be
read-only.

Omit partition unless the exact partition matters. The key decides placement,
and naming a partition puts a keyed message where its key does not hash to,
which breaks ordering for that key.
`

// Register adds the produce_message tool to the MCP server.
func Register(
	server *mcp.Server,
	clusters *kafkaclient.Registry,
	own string,
) {
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

			client := ""

			if info := req.ClientInfo(); info != nil {
				client = info.Name
			}

			out, err := runBatch(ctx, clusters, own, input.Items, input.Confirm, client)
			if err != nil {
				return nil, BatchOutput{}, fmt.Errorf("produce messages: %w", err)
			}

			return nil, out, nil
		},
	)
}

// Run validates every message before writing any of them. The write phase is
// non-atomic because Kafka cannot retract a record produced before a later item
// failed.
func Run(
	ctx context.Context,
	clusters *kafkaclient.Registry,
	own string,
	input Input,
) (BatchOutput, error) {

	return runBatch(ctx, clusters, own, input.Items, input.Confirm, "")
}

func runBatch(
	ctx context.Context,
	clusters *kafkaclient.Registry,
	own string,
	items []Item,
	confirm bool,
	client string,
) (BatchOutput, error) {

	if err := batch.Validate(len(items), batch.MaxHeavyItems); err != nil {
		return BatchOutput{}, err
	}

	// Duplicate targets are not rejected here, unlike the batch write tools
	// that change one named thing. Two identical produce items are two
	// messages, which is a legitimate request: appending the same payload
	// twice is different from creating the same topic twice.

	out, err := batch.Run(ctx, items, batch.MaxHeavyItems, func(ctx context.Context, item Item) (Output, error) {
		return run(ctx, clusters, own, item, false, client)
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

		value, applyErr := run(ctx, clusters, own, item, true, client)
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

// run previews or performs one write.
func run(
	ctx context.Context,
	clusters *kafkaclient.Registry,
	own string,
	input Item,
	confirm bool,
	client string,
) (Output, error) {

	source := clusters.Endpoint(own)
	if source == nil {
		// Direct package tests and callers predating explicit endpoints pass a
		// cluster name. Keep that API while production uses endpoint names.
		source = clusters.Get(own)
	}

	if source == nil {
		return Output{}, fmt.Errorf("unknown endpoint or cluster %q", own)
	}

	destination := source
	destinationName := source.Config().Name

	if input.DestinationCluster != "" && input.DestinationCluster != destinationName {
		destination = clusters.Destination(input.DestinationCluster)
		if destination == nil {
			return Output{}, fmt.Errorf(
				"unknown destination_cluster %q: use list_clusters to see which clusters this server serves",
				input.DestinationCluster)
		}

		destinationName = input.DestinationCluster
	}

	// read_only protects the cluster being written to, so producing into a
	// writable cluster from a read-only endpoint is allowed: it changes
	// nothing on the protected one.
	//
	// Writing is the only thing this tool does, so it refuses outright rather
	// than offering a preview of a capability it does not have.
	if err := destination.RequireWritable(toolName); err != nil {
		return Output{}, err
	}

	if input.Topic == "" {
		return Output{}, fmt.Errorf("topic is required")
	}

	if input.Value == "" {
		return Output{}, fmt.Errorf(
			"value is required: an empty message and an absent one are different messages, and only one of them was intended")
	}

	key, value, err := decode(input)
	if err != nil {
		return Output{}, err
	}

	partitions, err := topicPartitions(ctx, destination, input.Topic)
	if err != nil {
		return Output{}, err
	}

	if input.Partition != nil {
		if *input.Partition < 0 || int(*input.Partition) >= partitions {
			return Output{}, fmt.Errorf(
				"topic %q has %d partition(s), numbered 0 to %d, so partition %d cannot be written to",
				input.Topic, partitions, partitions-1, *input.Partition)
		}
	}

	record := &kgo.Record{
		Topic: input.Topic,
		Key:   key,
		Value: value,
	}

	headers, added, collisions := withProvenance(input, destination, client)
	record.Headers = headers

	out := Output{
		DestinationCluster: destinationName,
		Topic:              input.Topic,
		Message:            records.Render(record, input.MaxValueBytes),
		ProvenanceHeaders:  added,
		Warnings:           []string{},
	}

	for _, key := range collisions {
		out.Warnings = append(out.Warnings, fmt.Sprintf(
			"the header %q was supplied by the caller, so it was kept and this message's provenance for it was not recorded",
			key))
	}

	if input.Partition != nil {
		record.Partition = *input.Partition

		out.Warnings = append(out.Warnings, fmt.Sprintf(
			"partition %d was chosen explicitly rather than by the key, so this message may not be on the partition its key hashes to",
			*input.Partition))
	}

	if !confirm {
		out.Message.Partition = 0
		out.Note = "nothing was written. Call again with confirm true to produce this message."

		return out, nil
	}

	written, err := produce(ctx, destination, record, input.Partition != nil)
	if err != nil {
		return Output{}, err
	}

	out.Applied = true
	out.WrittenPartition = written.Partition
	out.WrittenOffset = written.Offset
	out.Message.Partition = written.Partition
	out.Message.Offset = written.Offset
	out.Message.Timestamp = written.Timestamp.UTC()
	out.Note = fmt.Sprintf(
		"produced to cluster %s topic %s partition %d offset %d. A produced message cannot be deleted: it stays until retention removes it",
		destinationName, input.Topic, written.Partition, written.Offset)

	return out, nil
}

// decode turns the caller's key and value into bytes.
func decode(input Item) ([]byte, []byte, error) {
	encoding := input.Encoding
	if encoding == "" {
		encoding = encodingUTF8
	}

	switch encoding {
	case encodingUTF8:
		var key []byte
		if input.Key != "" {
			key = []byte(input.Key)
		}

		return key, []byte(input.Value), nil

	case encodingBase64:
		value, err := base64.StdEncoding.DecodeString(input.Value)
		if err != nil {
			return nil, nil, fmt.Errorf(
				"value is not valid base64: %w. Writing the literal text instead would put a payload in the topic that no consumer can read",
				err)
		}

		var key []byte

		if input.Key != "" {
			key, err = base64.StdEncoding.DecodeString(input.Key)
			if err != nil {
				return nil, nil, fmt.Errorf("key is not valid base64: %w", err)
			}
		}

		return key, value, nil
	}

	return nil, nil, fmt.Errorf(
		"unknown encoding %q: use %q or %q", input.Encoding, encodingUTF8, encodingBase64)
}

// withProvenance returns the headers the message will carry: the caller's own,
// plus a record of what produced it.
func withProvenance(
	input Item,
	destination *kafkaclient.Client,
	client string,
) ([]kgo.RecordHeader, []string, []string) {

	headers := make([]kgo.RecordHeader, 0, len(input.Headers)+4)
	supplied := make(map[string]bool, len(input.Headers))

	names := make([]string, 0, len(input.Headers))
	for name := range input.Headers {
		names = append(names, name)
	}

	// Go map order is random, and headers that change order between runs make
	// two identical requests produce different records.
	sort.Strings(names)

	for _, name := range names {
		supplied[name] = true
		headers = append(headers, kgo.RecordHeader{Key: name, Value: []byte(input.Headers[name])})
	}

	principal := "anonymous"

	if cfg := destination.Config(); cfg != nil {
		// The first configured identity is the one franz-go prefers, so it is
		// the principal a broker is most likely to have recorded for the write.
		if options := cfg.AuthenticationOptions(); len(options) > 0 && options[0].User != "" {
			principal = options[0].User
		}
	}

	provenance := []kgo.RecordHeader{
		{Key: headerProducedAt, Value: []byte(time.Now().UTC().Format(time.RFC3339))},
		{Key: headerProducedByTool, Value: []byte(toolName)},
		{Key: headerProducedByUser, Value: []byte(principal)},
	}

	if client != "" {
		provenance = append(provenance,
			kgo.RecordHeader{Key: headerProducedByClient, Value: []byte(client)})
	}

	var (
		added      []string
		collisions []string
	)

	for _, header := range provenance {
		// A header the caller supplied is data, and data is never overwritten
		// by bookkeeping.
		if supplied[header.Key] {
			collisions = append(collisions, header.Key)

			continue
		}

		headers = append(headers, header)
		added = append(added, header.Key)
	}

	return headers, added, collisions
}

func produce(
	ctx context.Context,
	kafka *kafkaclient.Client,
	record *kgo.Record,
	manualPartition bool,
) (*kgo.Record, error) {

	producer := kafka.Kafka()

	if manualPartition {
		// The shared client partitions by key and ignores Record.Partition, so
		// an explicitly chosen partition needs the producer that honours it.
		manual, err := kafka.ManualProducer()
		if err != nil {
			return nil, err
		}

		producer = manual
	}

	results := producer.ProduceSync(ctx, record)

	if err := results.FirstErr(); err != nil {
		if errors.Is(err, kerr.TopicAuthorizationFailed) {
			return nil, fmt.Errorf(
				"not authorized to write to %q: the broker refused this request. Producing needs write permission on the topic for the principal this server connects as: %w",
				record.Topic, err)
		}

		return nil, fmt.Errorf("write the message to %q: %w", record.Topic, err)
	}

	return record, nil
}

// topicPartitions returns how many partitions a topic has, and fails when the
// topic does not exist.
//
// Auto-creation is why this is checked rather than left to the produce call: a
// cluster with auto-creation on would turn a mistyped topic into a new one.
func topicPartitions(ctx context.Context, kafka *kafkaclient.Client, topic string) (int, error) {
	details, err := kafka.Admin().ListTopics(ctx, topic)
	if err != nil {
		return 0, fmt.Errorf("list topic %q: %w", topic, err)
	}

	detail, ok := details[topic]
	if !ok || errors.Is(detail.Err, kerr.UnknownTopicOrPartition) {
		// The broker reports an absent topic either by omitting it or by
		// returning it with UNKNOWN_TOPIC_OR_PARTITION, and both mean the same
		// thing to the caller.
		return 0, fmt.Errorf(
			"topic %q does not exist: create it first, so a mistyped name cannot scatter messages into a topic nobody meant to make",
			topic)
	}

	if detail.Err != nil {
		return 0, fmt.Errorf("topic %q: %w", topic, detail.Err)
	}

	return len(detail.Partitions), nil
}
