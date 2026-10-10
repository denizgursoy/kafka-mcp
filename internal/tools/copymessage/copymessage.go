// Package copymessage implements the copy_message MCP tool.
package copymessage

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/denizgursoy/kafka-mcp/internal/domain/batch"
	"github.com/denizgursoy/kafka-mcp/internal/domain/kafkaclient"
	"github.com/denizgursoy/kafka-mcp/internal/domain/records"
	"github.com/denizgursoy/kafka-mcp/internal/domain/serde"
)

// Item is one source address and destination for a copy.
type Item struct {
	SourceTopic        string `json:"source_topic" jsonschema:"Topic holding the message to copy. Matched exactly and case-sensitively."`
	SourcePartition    int32  `json:"source_partition" jsonschema:"Partition holding the message to copy."`
	SourceOffset       int64  `json:"source_offset" jsonschema:"Exact offset of the message to copy."`
	DestinationTopic   string `json:"destination_topic" jsonschema:"Topic to write the copy to. It must already exist. It must differ from the source topic when both are on the same cluster."`
	DestinationCluster string `json:"destination_cluster,omitempty" jsonschema:"Optional cluster to write the copy to. Defaults to the cluster this endpoint serves. Use list_clusters to see which names are valid. The destination cluster must not be read-only; the source may be."`
	TranslateSchema    bool   `json:"translate_schema,omitempty" jsonschema:"Optional. When the key or value carries a Schema Registry id and the destination cluster uses a different registry, register that schema in the destination registry under <destination_topic>-value (or -key) and rewrite the id in the copy, so destination consumers can decode it. Without it the copy keeps the source id, which the destination registry may not have or may map to a different schema. Writes to the destination registry. Has no effect on messages without a schema id."`
	MaxValueBytes      int    `json:"max_value_bytes,omitempty" jsonschema:"Optional maximum number of value bytes to show in the response. Defaults to 4096. This affects the preview only: the copy always carries the whole value."`
}

// Provenance headers added to every copy, so a message that turns up in
// another topic can explain how it got there.
const (
	headerFromCluster   = "kafka-mcp-copied-from-cluster"
	headerFromTopic     = "kafka-mcp-copied-from-topic"
	headerFromPartition = "kafka-mcp-copied-from-partition"
	headerFromOffset    = "kafka-mcp-copied-from-offset"
	headerCopiedAt      = "kafka-mcp-copied-at"
	headerCopiedByTool  = "kafka-mcp-copied-by-tool"
	headerCopiedByUser  = "kafka-mcp-copied-by-principal"
	headerCopiedByAgent = "kafka-mcp-copied-by-client"

	toolName = "copy_message"
)

// Input is the argument set accepted by the copy_message tool.
//
// A message is named by its address rather than its content, so this tool can
// only duplicate something the cluster already holds. It has no way to write a
// message that no producer sent; produce_message does that.
type Input struct {
	Items   []Item `json:"items" jsonschema:"The copies to make, 1 to 20 of them. Copying one message is an array of length one. Two items naming the same source and destination are refused before anything is written."`
	Confirm bool   `json:"confirm,omitempty" jsonschema:"Optional. When false or omitted, nothing is written and the response shows the messages that would be copied. Must be true to actually write them. One confirm covers the whole batch."`
}

// Output is the result returned by the copy_message tool.
type Output struct {
	SourceCluster      string          `json:"source_cluster"`
	DestinationCluster string          `json:"destination_cluster"`
	SourceTopic        string          `json:"source_topic"`
	SourcePartition    int32           `json:"source_partition"`
	SourceOffset       int64           `json:"source_offset"`
	DestinationTopic   string          `json:"destination_topic"`
	Message            records.Message `json:"message"`
	KeySchema          *serde.Encoded  `json:"key_schema,omitempty"`
	ValueSchema        *serde.Encoded  `json:"value_schema,omitempty"`
	Applied            bool            `json:"applied"`
	WrittenPartition   int32           `json:"written_partition"`
	WrittenOffset      int64           `json:"written_offset"`
	ProvenanceHeaders  []string        `json:"provenance_headers,omitempty"`
	Warnings           []string        `json:"warnings,omitempty"`
	Note               string          `json:"note,omitempty"`
}

type BatchOutput = batch.Output[Output]

const description = `
Copy 1 to 20 existing messages in one call through items, each identified by
topic, partition and offset, to an existing topic on this or another configured
cluster. Preserves key, value and headers and adds traceable provenance headers.
Copying one message is an items array of length one.

No message is written unless confirm is true; one confirm covers the whole
batch, and writing is not atomic because Kafka cannot retract a record produced
before a later item failed. Results follow items order, each carrying index with
result or error.

The destination must be writable and requires Kafka write permission; a
read-only cluster may still be the source.

A key or value carrying a Schema Registry id is copied byte for byte. When the
destination cluster uses a different registry the id may mean nothing there, so
the response warns; set translate_schema to register the schema in the
destination registry and rewrite the id.
`

// Register adds the copy_message tool to the MCP server.
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
				return nil, BatchOutput{}, fmt.Errorf("copy messages: %w", err)
			}

			return nil, out, nil
		},
	)
}

// Run previews every copy before writing valid items. Writes are non-atomic
// because Kafka cannot retract a record that was produced before a later item
// failed.
func Run(
	ctx context.Context,
	clusters *kafkaclient.Registry,
	own string,
	input Input,
) (BatchOutput, error) {

	return runBatch(ctx, clusters, own, input.Items, input.Confirm, "")
}

func runBatch(ctx context.Context, clusters *kafkaclient.Registry, own string, items []Item, confirm bool, client string) (BatchOutput, error) {
	if err := batch.Validate(len(items), batch.MaxHeavyItems); err != nil {
		return BatchOutput{}, err
	}
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		key := fmt.Sprintf("%s\x00%d\x00%d\x00%s\x00%s", item.SourceTopic, item.SourcePartition, item.SourceOffset, item.DestinationCluster, item.DestinationTopic)
		if _, ok := seen[key]; ok {
			return BatchOutput{}, fmt.Errorf("duplicate copy of %s partition %d offset %d to %q", item.SourceTopic, item.SourcePartition, item.SourceOffset, item.DestinationTopic)
		}
		seen[key] = struct{}{}
	}

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
	sourceCluster := source.Config().Name

	// The destination defaults to this endpoint's own cluster, so a copy
	// within one cluster needs no extra parameter.
	destination := source
	destinationName := sourceCluster

	if input.DestinationCluster != "" && input.DestinationCluster != sourceCluster {
		var err error
		destination, err = clusters.Destination(own, input.DestinationCluster)
		if err != nil {
			return Output{}, err
		}

		destinationName = input.DestinationCluster
	}

	// read_only protects the cluster being written to. Copying out of a
	// read-only cluster changes nothing there and is how a message is rescued
	// from production, so only the destination is checked.
	//
	// Writing is the only thing this tool does, so it refuses outright rather
	// than offering a preview of a capability it does not have.
	if err := destination.RequireWritable(toolName); err != nil {
		return Output{}, err
	}

	if input.SourceTopic == "" {
		return Output{}, fmt.Errorf("source_topic is required")
	}

	if input.DestinationTopic == "" {
		return Output{}, fmt.Errorf("destination_topic is required")
	}

	if input.SourceTopic == input.DestinationTopic && destinationName == sourceCluster {
		return Output{}, fmt.Errorf(
			"source_topic and destination_topic are both %q: copying a topic onto itself appends a duplicate to the topic being debugged",
			input.SourceTopic)
	}

	if input.SourceOffset < 0 {
		return Output{}, fmt.Errorf(
			"source_offset must not be negative, got %d", input.SourceOffset)
	}

	if err := topicExists(ctx, destination, input.DestinationTopic); err != nil {
		return Output{}, err
	}

	record, err := readSource(ctx, source, source.Reader(), input)
	if err != nil {
		return Output{}, err
	}

	out := Output{
		SourceCluster:      sourceCluster,
		DestinationCluster: destinationName,
		SourceTopic:        input.SourceTopic,
		SourcePartition:    input.SourcePartition,
		SourceOffset:       input.SourceOffset,
		DestinationTopic:   input.DestinationTopic,
		Message:            source.Reader().Render(ctx, record, input.MaxValueBytes),
		Warnings:           []string{},
	}

	headers, added, collisions := withProvenance(record, input, sourceCluster, destination, client)

	out.ProvenanceHeaders = added

	for _, key := range collisions {
		out.Warnings = append(out.Warnings, fmt.Sprintf(
			"the message already carries the header %q, so it was kept and this copy's provenance for it was not recorded",
			key))
	}

	key, value := record.Key, record.Value

	key, out.KeySchema, err = schemaFor(ctx, source, destination, input, serde.Key, key, confirm, &out.Warnings)
	if err != nil {
		return Output{}, err
	}

	value, out.ValueSchema, err = schemaFor(ctx, source, destination, input, serde.Value, value, confirm, &out.Warnings)
	if err != nil {
		return Output{}, err
	}

	if !confirm {
		out.Note = "nothing was written. Call again with confirm true to copy the message."

		return out, nil
	}

	copied := &kgo.Record{Key: key, Value: value}

	written, err := produce(ctx, destination, input.DestinationTopic, copied, headers)
	if err != nil {
		return Output{}, err
	}

	out.Applied = true
	out.WrittenPartition = written.Partition
	out.WrittenOffset = written.Offset
	out.Note = fmt.Sprintf(
		"copied to cluster %s topic %s partition %d offset %d",
		destinationName, input.DestinationTopic, written.Partition, written.Offset)

	return out, nil
}

// schemaFor decides what happens to a schema id one part of the copy carries.
//
// Within one registry an id means the same schema on both sides and nothing
// changes. Across registries it may not: unless translate_schema asks for the
// id to be rewritten, the copy keeps it and a warning says so. Translation
// registers the schema in the destination registry, which is a write, so it
// only happens on confirm; the preview reports the subject it would use.
func schemaFor(
	ctx context.Context,
	source, destination *kafkaclient.Client,
	input Item,
	part serde.Part,
	data []byte,
	confirm bool,
	warnings *[]string,
) ([]byte, *serde.Encoded, error) {

	id, framed := serde.SchemaID(data)
	if !framed || serde.SameRegistry(source.Codec(), destination.Codec()) {
		return data, nil, nil
	}

	subject := input.DestinationTopic + "-" + string(part)

	if !input.TranslateSchema {
		*warnings = append(*warnings, fmt.Sprintf(
			"the %s carries schema id %d from the source cluster's schema registry, and the destination uses a different one or none, where that id may be missing or name a different schema. Set translate_schema to register the schema in the destination registry under %q and rewrite the id",
			part, id, subject))

		return data, nil, nil
	}

	if !destination.Codec().HasRegistry() {
		return nil, nil, fmt.Errorf(
			"translate_schema was set but the destination cluster has no schema_registry configured to register schema id %d in", id)
	}

	if !confirm {
		return data, &serde.Encoded{Subject: subject}, nil
	}

	translated, encoded, err := serde.Translate(ctx, source.Codec(), destination.Codec(), subject, data)
	if err != nil {
		return nil, nil, fmt.Errorf("translate the %s schema: %w", part, err)
	}

	return translated, &encoded, nil
}

// withProvenance returns the headers the copy will carry: the original ones,
// plus a record of where this copy came from.
func withProvenance(
	record *kgo.Record,
	input Item,
	sourceCluster string,
	destination *kafkaclient.Client,
	client string,
) ([]kgo.RecordHeader, []string, []string) {

	existing := make(map[string]bool, len(record.Headers))

	headers := make([]kgo.RecordHeader, 0, len(record.Headers)+7)

	for _, header := range record.Headers {
		existing[header.Key] = true
		headers = append(headers, header)
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
		{Key: headerFromCluster, Value: []byte(sourceCluster)},
		{Key: headerFromTopic, Value: []byte(input.SourceTopic)},
		{Key: headerFromPartition, Value: []byte(strconv.Itoa(int(input.SourcePartition)))},
		{Key: headerFromOffset, Value: []byte(strconv.FormatInt(input.SourceOffset, 10))},
		{Key: headerCopiedAt, Value: []byte(time.Now().UTC().Format(time.RFC3339))},
		{Key: headerCopiedByTool, Value: []byte(toolName)},
		{Key: headerCopiedByUser, Value: []byte(principal)},
	}

	if client != "" {
		provenance = append(provenance,
			kgo.RecordHeader{Key: headerCopiedByAgent, Value: []byte(client)})
	}

	var (
		added      []string
		collisions []string
	)

	for _, header := range provenance {
		// A header the message already carries is data, and data is never
		// overwritten by bookkeeping.
		if existing[header.Key] {
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
	topic string,
	record *kgo.Record,
	headers []kgo.RecordHeader,
) (*kgo.Record, error) {

	copied := &kgo.Record{
		Topic:   topic,
		Key:     record.Key,
		Value:   record.Value,
		Headers: headers,
	}

	results := kafka.Kafka().ProduceSync(ctx, copied)

	if err := results.FirstErr(); err != nil {
		if errors.Is(err, kerr.TopicAuthorizationFailed) {
			return nil, fmt.Errorf(
				"not authorized to write to %q: the broker refused this request. Copying needs write permission on the destination topic for the principal this server connects as: %w",
				topic, err)
		}

		return nil, fmt.Errorf("write the copy to %q: %w", topic, err)
	}

	return copied, nil
}

func readSource(
	ctx context.Context,
	kafka *kafkaclient.Client,
	reader *records.Reader,
	input Item,
) (*kgo.Record, error) {

	// Seeking past the end of a partition does not fail: the read waits and
	// returns the next message produced. Without this check a copy from an
	// offset that holds nothing would quietly duplicate a different message.
	ends, err := kafka.Admin().ListEndOffsets(ctx, input.SourceTopic)
	if err != nil {
		return nil, fmt.Errorf("list end offsets for %q: %w", input.SourceTopic, err)
	}

	end, ok := ends.Lookup(input.SourceTopic, input.SourcePartition)
	if !ok || end.Err != nil {
		return nil, fmt.Errorf(
			"source topic %q has no partition %d", input.SourceTopic, input.SourcePartition)
	}

	if input.SourceOffset >= end.Offset {
		return nil, fmt.Errorf(
			"no message at %s partition %d offset %d: the partition ends at offset %d",
			input.SourceTopic, input.SourcePartition, input.SourceOffset, end.Offset)
	}

	var found *kgo.Record

	err = reader.Scan(
		ctx,
		input.SourceTopic,
		[]records.Range{{
			Partition: input.SourcePartition,
			Start:     input.SourceOffset,
			End:       input.SourceOffset + 1,
		}},
		func(record *kgo.Record) bool {
			found = record

			return false
		},
	)
	if err != nil {
		return nil, fmt.Errorf(
			"read %s partition %d offset %d: %w",
			input.SourceTopic, input.SourcePartition, input.SourceOffset, err)
	}

	if found == nil {
		return nil, fmt.Errorf(
			"no message at %s partition %d offset %d: the offset is past the end of the partition, or the partition does not exist",
			input.SourceTopic, input.SourcePartition, input.SourceOffset)
	}

	return found, nil
}

func topicExists(ctx context.Context, kafka *kafkaclient.Client, topic string) error {
	details, err := kafka.Admin().ListTopics(ctx, topic)
	if err != nil {
		return fmt.Errorf("list topic %q: %w", topic, err)
	}

	detail, ok := details[topic]
	if !ok || errors.Is(detail.Err, kerr.UnknownTopicOrPartition) {
		// The broker reports an absent topic either by omitting it or by
		// returning it with UNKNOWN_TOPIC_OR_PARTITION, and both mean the same
		// thing to the caller.
		return fmt.Errorf(
			"destination topic %q does not exist: create it first, so a mistyped name cannot scatter messages into a topic nobody meant to make",
			topic)
	}

	if detail.Err != nil {
		return fmt.Errorf("destination topic %q: %w", topic, detail.Err)
	}

	return nil
}
