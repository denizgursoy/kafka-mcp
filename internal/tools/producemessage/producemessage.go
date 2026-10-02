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
	"github.com/denizgursoy/kafka-mcp/internal/domain/serde"
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
	Value              string            `json:"value" jsonschema:"The message body. Written as text unless encoding is base64. With value_schema, or on a topic with a configured format, give it as JSON and it is encoded to that schema."`
	Key                string            `json:"key,omitempty" jsonschema:"Optional message key. The key decides which partition the message lands on when partition is omitted, so re-injecting a repaired message with its original key keeps that key's ordering. With key_schema give it as JSON, e.g. a quoted string for a string schema."`
	Headers            map[string]string `json:"headers,omitempty" jsonschema:"Optional message headers, as a map of name to value. A header that collides with a provenance header is kept as given and the collision is reported."`
	Partition          *int32            `json:"partition,omitempty" jsonschema:"Optional exact partition to write to. Omit to let the key decide, which is almost always what you want: naming a partition puts a keyed message somewhere its key does not hash to, breaking that key's ordering."`
	Encoding           string            `json:"encoding,omitempty" jsonschema:"Optional encoding of key and value: utf8 (default) or base64. Use base64 to write exact bytes you already have, such as a copy of an existing binary message. Cannot be combined with value_schema or key_schema."`
	ValueSchema        *SchemaRef        `json:"value_schema,omitempty" jsonschema:"Optional. Encode value, given as JSON, to a Schema Registry schema (Avro, Protobuf or JSON Schema) on the destination cluster's registry, framed the way registry-aware consumers expect. An empty object {} uses the latest version of subject <topic>-value. Use this whenever get_message or sample_messages reports the topic's values with a schema_id."`
	KeySchema          *SchemaRef        `json:"key_schema,omitempty" jsonschema:"Optional. Like value_schema, for the key; an empty object uses subject <topic>-key."`
	DestinationCluster string            `json:"destination_cluster,omitempty" jsonschema:"Optional cluster to write to. Defaults to the cluster this endpoint serves. Use list_clusters to see which names are valid. The destination must not be read-only; the endpoint you called may be."`
	MaxValueBytes      int               `json:"max_value_bytes,omitempty" jsonschema:"Optional maximum number of value bytes to show in the response. Defaults to 4096. This affects the preview only: the whole value is always written."`
}

// SchemaRef names the registry schema to encode against. Every field is
// optional.
type SchemaRef struct {
	Subject     string `json:"subject,omitempty" jsonschema:"Optional subject to take the schema from. Defaults to <topic>-value for value_schema and <topic>-key for key_schema."`
	Version     int    `json:"version,omitempty" jsonschema:"Optional subject version. Defaults to the latest."`
	ID          int    `json:"id,omitempty" jsonschema:"Optional exact schema id, e.g. the schema_id get_message reported. Overrides subject and version."`
	MessageType string `json:"message_type,omitempty" jsonschema:"Optional fully qualified Protobuf message name, e.g. shop.Order, when the schema defines several. Defaults to the first message. Ignored for Avro and JSON Schema."`
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
	ValueEncoding      *serde.Encoded  `json:"value_encoding,omitempty"`
	KeyEncoding        *serde.Encoded  `json:"key_encoding,omitempty"`
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

A topic whose consumers read Avro, Protobuf or JSON Schema needs value_schema:
give the value as JSON and it is encoded to the destination registry's schema,
refused with the offending field when it does not fit. A topic with a format
configured in topic_formats is encoded to it without being asked. Check
get_message or sample_messages first: a schema_id on the existing messages means
the topic needs value_schema, and writing plain JSON there breaks its consumers.
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

	key, value, keyEncoding, valueEncoding, err := encode(ctx, destination, input)
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
		Message:            destination.Reader().Render(ctx, record, input.MaxValueBytes),
		ValueEncoding:      valueEncoding,
		KeyEncoding:        keyEncoding,
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

// encode turns the caller's key and value into the bytes to write.
//
// Each part is encoded by the first of: an explicit schema reference against
// the destination's registry, the destination topic's configured format, or
// the plain encoding (utf8 or base64). The destination decides, not the
// endpoint called, because the destination's consumers are the ones reading.
func encode(
	ctx context.Context,
	destination *kafkaclient.Client,
	input Item,
) ([]byte, []byte, *serde.Encoded, *serde.Encoded, error) {

	if input.Encoding == encodingBase64 {
		if input.ValueSchema != nil {
			return nil, nil, nil, nil, fmt.Errorf(
				"value_schema cannot be combined with encoding base64: base64 is for bytes that are already final, and value_schema is for JSON to be encoded")
		}

		if input.KeySchema != nil {
			return nil, nil, nil, nil, fmt.Errorf(
				"key_schema cannot be combined with encoding base64: base64 is for bytes that are already final, and key_schema is for JSON to be encoded")
		}
	}

	key, value, err := decode(input)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// base64 bytes are final; a configured format must not re-encode them.
	if input.Encoding == encodingBase64 {
		return key, value, nil, nil, nil
	}

	codec := destination.Codec()

	value, valueEncoding, err := encodePart(ctx, codec, input.Topic, serde.Value, input.Value, value, input.ValueSchema)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	if input.Key != "" {
		var keyEncoding *serde.Encoded

		key, keyEncoding, err = encodePart(ctx, codec, input.Topic, serde.Key, input.Key, key, input.KeySchema)
		if err != nil {
			return nil, nil, nil, nil, err
		}

		return key, value, keyEncoding, valueEncoding, nil
	}

	if input.KeySchema != nil {
		return nil, nil, nil, nil, fmt.Errorf("key_schema was given without a key to encode")
	}

	return key, value, nil, valueEncoding, nil
}

func encodePart(
	ctx context.Context,
	codec *serde.Codec,
	topic string,
	part serde.Part,
	text string,
	plain []byte,
	ref *SchemaRef,
) ([]byte, *serde.Encoded, error) {

	if ref != nil {
		data, encoded, err := codec.Encode(ctx, topic, part, text, serde.Target{
			Subject:     ref.Subject,
			Version:     ref.Version,
			ID:          ref.ID,
			MessageType: ref.MessageType,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("encode %s: %w", part, err)
		}

		return data, &encoded, nil
	}

	data, encoded, configured, err := codec.EncodeConfigured(topic, part, text)
	if err != nil {
		return nil, nil, err
	}

	if !configured {
		return plain, nil, nil
	}

	return data, &encoded, nil
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
