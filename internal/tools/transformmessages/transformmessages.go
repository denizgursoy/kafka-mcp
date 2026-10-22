// Package transformmessages implements the transform_messages MCP tool.
package transformmessages

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/denizgursoy/kafka-mcp/internal/domain/batch"
	"github.com/denizgursoy/kafka-mcp/internal/domain/kafkaclient"
	"github.com/denizgursoy/kafka-mcp/internal/domain/outputfile"
	"github.com/denizgursoy/kafka-mcp/internal/domain/records"
	"github.com/denizgursoy/kafka-mcp/internal/domain/script"
	"github.com/denizgursoy/kafka-mcp/internal/domain/serde"
)

const toolName = "transform_messages"

// Provenance headers added to every rewritten message, so a repaired message
// can always be traced to the original it was made from.
const (
	headerFromCluster   = "kafka-mcp-transformed-from-cluster"
	headerFromTopic     = "kafka-mcp-transformed-from-topic"
	headerFromPartition = "kafka-mcp-transformed-from-partition"
	headerFromOffset    = "kafka-mcp-transformed-from-offset"
	headerAt            = "kafka-mcp-transformed-at"
	headerByTool        = "kafka-mcp-transformed-by-tool"
	headerByPrincipal   = "kafka-mcp-transformed-by-principal"
	headerByClient      = "kafka-mcp-transformed-by-client"
)

// Stop reasons reported back to the caller.
const (
	reasonExhausted   = "range_exhausted"
	reasonMaxMessages = "max_messages"
	reasonTimeout     = "timeout"
	reasonWriteFailed = "write_failed"
)

// Defaults and limits on caller input.
const (
	defaultMaxMessages = 100_000
	maxMaxMessages     = 10_000_000
	defaultTimeout     = 300 * time.Second
	maxTimeoutSeconds  = 3600
	maxValueBytesCap   = 1 << 20

	maxExamples      = 3
	maxErrorGroups   = 20
	maxErrorExamples = 5

	// flushSize is how many records are produced together. One at a time
	// would cost a round trip per message.
	flushSize = 1000
)

// Input is the argument set accepted by the transform_messages tool.
type Input struct {
	Topic              string     `json:"topic" jsonschema:"Topic to read the messages from. Matched exactly and case-sensitively."`
	Transform          string     `json:"transform" jsonschema:"JavaScript that returns the new message, with the same variables as search_messages' script: value (parsed JSON or the decoded Avro/Protobuf/JSON Schema record; text otherwise), key, headers, partition, offset, timestamp, value_bytes, key_bytes, format, schema_id and decode_error. Return an object with any of value, key and headers; a field left out is kept from the original. Return null to skip the message. Example: return { value: value.payload }. A string value is written as its text, any other value as JSON, and a schema-encoded or topic_formats topic is re-encoded to its schema. Returning headers replaces all of them; spread the originals to keep them and set one to null to remove it: { headers: { ...headers, 'error-reason': null } }. Standard JavaScript (ES2020: arrow functions, spread, destructuring, regular expressions, JSON, Math, Date) is available; console, require, timers and network are not. An integer beyond 2^53 arrives as a BigInt so it is written back exactly: pass it through, compare it with a BigInt literal such as 123n, or use String(); turning it into a Number rounds it, and the message is refused rather than written."`
	Script             string     `json:"script,omitempty" jsonschema:"Optional JavaScript predicate, as in search_messages, choosing which messages to transform. Return true to transform the message. Omit to transform every message in the range."`
	Partitions         []int32    `json:"partitions,omitempty" jsonschema:"Optional partitions to read. Defaults to every partition. Partitions are processed one after another in ascending order."`
	FromOffset         *int64     `json:"from_offset,omitempty" jsonschema:"Optional inclusive offset to start at, applied to every partition read. Use it with partitions to resume a run that stopped."`
	ToOffset           *int64     `json:"to_offset,omitempty" jsonschema:"Optional exclusive offset to stop at, applied to every partition read."`
	FromTimestamp      *time.Time `json:"from_timestamp,omitempty" jsonschema:"Optional inclusive start time (RFC3339). Messages whose own timestamp is earlier are skipped and not counted as scanned."`
	ToTimestamp        *time.Time `json:"to_timestamp,omitempty" jsonschema:"Optional exclusive end time (RFC3339). Messages whose own timestamp is at or after it are skipped and not counted as scanned."`
	DestinationTopic   string     `json:"destination_topic,omitempty" jsonschema:"Optional topic to write the new messages to. Defaults to the source topic, where the new messages are appended after the originals; the originals stay. It must already exist."`
	DestinationCluster string     `json:"destination_cluster,omitempty" jsonschema:"Optional cluster to write to. Defaults to the cluster this endpoint serves. Use list_clusters to see which names are valid. It must be writable when confirm is true."`
	KeepPartition      bool       `json:"keep_partition,omitempty" jsonschema:"Optional. Write each message to the partition number it was read from, instead of letting its key decide. The destination needs at least as many partitions. Use it when the source producer chose partitions explicitly."`
	ValueSchema        *SchemaRef `json:"value_schema,omitempty" jsonschema:"Optional. Encode every new value, as JSON, to this Schema Registry schema on the destination cluster. Needed when a schema-encoded value moves to a cluster with a different registry, or to change the schema. An empty object {} uses the latest version of <destination_topic>-value. Without it a value read with a schema id is written with that same id."`
	KeySchema          *SchemaRef `json:"key_schema,omitempty" jsonschema:"Optional. Like value_schema, for the key."`
	MaxMessages        int        `json:"max_messages,omitempty" jsonschema:"Optional maximum number of messages to transform, counting only those the script selects. Defaults to 100000, at most 10000000. complete is false when the limit stops the run."`
	TimeoutSeconds     int        `json:"timeout_seconds,omitempty" jsonschema:"Optional wall-clock limit in seconds. Defaults to 300, at most 3600. Messages already written when it fires stay written; scanned_ranges says how far each partition got."`
	MaxValueBytes      int        `json:"max_value_bytes,omitempty" jsonschema:"Optional maximum number of value bytes to show in each example. Defaults to 4096. Affects the response only."`
	OutputFile         string     `json:"output_file,omitempty" jsonschema:"Optional new file name to save every transformed message to, one produce_message item per line, so the set can be inspected and later sent with produce_message input_file. A name only, not a path: the server chooses the directory and refuses a name that already exists. Saving writes nothing to Kafka and works on a read-only cluster."`
	Confirm            bool       `json:"confirm,omitempty" jsonschema:"Optional. When false or omitted, nothing is written to Kafka: the response counts what would be written, shows examples and groups the failures. Must be true to produce the new messages."`
}

// SchemaRef names the registry schema to encode against. Every field is
// optional.
type SchemaRef struct {
	Subject     string `json:"subject,omitempty" jsonschema:"Optional subject to take the schema from. Defaults to <destination_topic>-value, or -key for key_schema."`
	Version     int    `json:"version,omitempty" jsonschema:"Optional subject version. Defaults to the latest."`
	ID          int    `json:"id,omitempty" jsonschema:"Optional exact schema id. Overrides subject and version."`
	MessageType string `json:"message_type,omitempty" jsonschema:"Optional fully qualified Protobuf message name. Defaults to the first message. Ignored for Avro and JSON Schema."`
}

// Output is the result returned by the transform_messages tool.
type Output struct {
	SourceCluster      string `json:"source_cluster"`
	Topic              string `json:"topic"`
	DestinationCluster string `json:"destination_cluster"`
	DestinationTopic   string `json:"destination_topic"`

	Scanned     int `json:"scanned"`
	Matched     int `json:"matched"`
	Transformed int `json:"transformed"`
	Skipped     int `json:"skipped"`
	Failed      int `json:"failed"`
	Written     int `json:"written"`

	Applied       bool           `json:"applied"`
	Complete      bool           `json:"complete"`
	StoppedReason string         `json:"stopped_reason"`
	ScannedRanges []ScannedRange `json:"scanned_ranges"`

	Examples        []Example    `json:"examples"`
	Errors          []ErrorGroup `json:"errors"`
	ErrorsTruncated bool         `json:"errors_truncated,omitempty"`

	OutputFile  string `json:"output_file,omitempty"`
	FileWritten int    `json:"file_written,omitempty"`

	Warnings []string `json:"warnings,omitempty"`
	Note     string   `json:"note,omitempty"`
}

// ScannedRange is the half-open offset range read in one partition.
type ScannedRange struct {
	Partition int32 `json:"partition"`
	Start     int64 `json:"start"`
	End       int64 `json:"end"`
}

// Example shows one message before and after the transform.
type Example struct {
	Before records.Message `json:"before"`
	After  records.Message `json:"after"`
}

// ErrorGroup is one reason messages could not be transformed or written, with
// how many it hit and the first few addresses.
type ErrorGroup struct {
	Reason   string    `json:"reason"`
	Count    int       `json:"count"`
	Examples []Address `json:"examples"`
}

// Address identifies a message.
type Address struct {
	Partition int32 `json:"partition"`
	Offset    int64 `json:"offset"`
}

const description = `
Rewrite messages with a JavaScript transform and write the results as new
messages: to the same topic, another topic, or a topic on another configured
cluster. Use it to repair messages produced in the wrong shape, for example a
document wrapped in a payload field: transform "return { value: value.payload }".

The transform is JavaScript run once per message. It sees the same variables
as search_messages and returns an object with any of value, key and headers;
what it leaves out is kept from the original, and null skips the message. An
integer beyond 2^53 arrives as a BigInt so ids are written back exactly. An optional script selects which messages
to transform. Schema-encoded values (Avro, Protobuf, JSON Schema) are decoded
for the transform and re-encoded with the same schema id; set value_schema to
encode to a different schema or registry.

The originals are never changed or deleted: Kafka cannot edit a message, so the
new messages are appended. Writing back to the source topic stops at the end
offset the call started with, so it never reads its own output. Every new
message carries provenance headers naming the source topic, partition and
offset.

Nothing is written to Kafka unless confirm is true. Without it the response
counts what would be written, shows before/after examples and groups failures
by reason with example addresses. output_file saves the transformed messages as
produce_message items, one per line, so the set can be inspected and later sent
with produce_message input_file; saving works on a read-only cluster.

Partitions are processed in ascending order and writing is not atomic:
messages written before a timeout or failure stay. complete, stopped_reason and
scanned_ranges say how far the run got; resume with partitions and from_offset.
`

// Register adds the transform_messages tool to the MCP server.
func Register(
	server *mcp.Server,
	clusters *kafkaclient.Registry,
	own string,
	outputDir string,
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
		) (*mcp.CallToolResult, Output, error) {

			client := ""

			if info := req.ClientInfo(); info != nil {
				client = info.Name
			}

			out, err := run(ctx, clusters, own, outputDir, input, client)
			if err != nil {
				return nil, Output{}, fmt.Errorf("transform messages: %w", err)
			}

			return nil, out, nil
		},
	)
}

// Run transforms the messages of a topic and, with confirm, writes the
// results. outputDir is where output_file is created.
func Run(
	ctx context.Context,
	clusters *kafkaclient.Registry,
	own string,
	outputDir string,
	input Input,
) (Output, error) {

	return run(ctx, clusters, own, outputDir, input, "")
}

func run(
	ctx context.Context,
	clusters *kafkaclient.Registry,
	own string,
	outputDir string,
	input Input,
	client string,
) (Output, error) {

	if input.Topic == "" {
		return Output{}, fmt.Errorf("topic is required")
	}

	if input.Transform == "" {
		return Output{}, fmt.Errorf("transform is required: it is the JavaScript that returns each new message")
	}

	for _, limit := range []struct {
		name           string
		value, maximum int
	}{
		{"max_messages", input.MaxMessages, maxMaxMessages},
		{"timeout_seconds", input.TimeoutSeconds, maxTimeoutSeconds},
		{"max_value_bytes", input.MaxValueBytes, maxValueBytesCap},
	} {
		if err := batch.Bounded(limit.name, limit.value, limit.maximum); err != nil {
			return Output{}, err
		}
	}

	source := clusters.Endpoint(own)
	if source == nil {
		// Direct package tests pass a cluster name rather than an endpoint.
		source = clusters.Get(own)
	}

	if source == nil {
		return Output{}, fmt.Errorf("unknown endpoint or cluster %q", own)
	}

	sourceCluster := source.Config().Name
	destination := source
	destinationCluster := sourceCluster

	if input.DestinationCluster != "" && input.DestinationCluster != sourceCluster {
		var err error

		destination, err = clusters.Destination(own, input.DestinationCluster)
		if err != nil {
			return Output{}, err
		}

		destinationCluster = input.DestinationCluster
	}

	destinationTopic := input.DestinationTopic
	if destinationTopic == "" {
		destinationTopic = input.Topic
	}

	// Compiling before reading reports a malformed script at once, rather
	// than after a scan that could not have transformed anything.
	transform, err := script.Compile(input.Transform, script.RecordParameters...)
	if err != nil {
		return Output{}, fmt.Errorf("transform: %w", err)
	}
	defer transform.Close()

	var filter *script.Script

	if input.Script != "" {
		filter, err = script.Compile(input.Script, script.RecordParameters...)
		if err != nil {
			return Output{}, fmt.Errorf("script: %w", err)
		}
		defer filter.Close()
	}

	window := records.Window{
		Partitions:    input.Partitions,
		FromOffset:    input.FromOffset,
		ToOffset:      input.ToOffset,
		FromTimestamp: input.FromTimestamp,
		ToTimestamp:   input.ToTimestamp,
	}

	windows, err := records.ResolveWindows(ctx, source.Admin(), input.Topic, window)
	if err != nil {
		return Output{}, err
	}

	partitions, err := topicPartitions(ctx, destination, destinationTopic)
	if err != nil {
		return Output{}, err
	}

	if input.KeepPartition {
		for _, window := range windows {
			if int(window.Partition) >= partitions {
				return Output{}, fmt.Errorf(
					"keep_partition needs partition %d, but destination topic %q has %d partition(s)",
					window.Partition, destinationTopic, partitions)
			}
		}
	}

	if input.Confirm {
		// Previewing and saving to a file change nothing in Kafka, so only
		// the write itself needs a writable destination.
		if err := destination.RequireWritable(toolName); err != nil {
			return Output{}, err
		}
	}

	out := Output{
		SourceCluster:      sourceCluster,
		Topic:              input.Topic,
		DestinationCluster: destinationCluster,
		DestinationTopic:   destinationTopic,
		ScannedRanges:      []ScannedRange{},
		Examples:           []Example{},
		Errors:             []ErrorGroup{},
		Warnings:           []string{},
		StoppedReason:      reasonExhausted,
	}

	var file *outputfile.Writer

	if input.OutputFile != "" {
		file, err = outputfile.Create(outputDir, "output_file", input.OutputFile)
		if err != nil {
			return Output{}, err
		}
		defer file.Close()
	}

	timeout := defaultTimeout
	if input.TimeoutSeconds > 0 {
		timeout = time.Duration(input.TimeoutSeconds) * time.Second
	}

	scanCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	maxMessages := input.MaxMessages
	if maxMessages <= 0 {
		maxMessages = defaultMaxMessages
	}

	state := &runState{
		input:       input,
		window:      window,
		source:      source,
		destination: destination,
		destTopic:   destinationTopic,
		filter:      filter,
		transform:   transform,
		file:        file,
		out:         &out,
		maxMessages: maxMessages,
		writeCtx:    ctx,
		errors:      map[string]*ErrorGroup{},
		provenance:  provenanceBase(sourceCluster, input.Topic, destination, client),
	}

	stopFilter := script.Guard(scanCtx, filter, "transform stopped")
	stopTransform := script.Guard(scanCtx, transform, "transform stopped")

	err = state.scan(scanCtx, windows)

	stopFilter()
	stopTransform()

	if err != nil {
		return Output{}, err
	}

	if file != nil {
		if err := file.Close(); err != nil {
			return Output{}, fmt.Errorf("close output file: %w", err)
		}

		out.OutputFile = file.Path()
		out.FileWritten = file.Written()
	}

	finish(&out, state)

	return out, nil
}

// finish orders the error groups and writes the note.
func finish(out *Output, state *runState) {
	for _, group := range state.errors {
		out.Errors = append(out.Errors, *group)
	}

	// Map order is random, so the largest groups come first and ties are
	// broken by reason to keep the response stable.
	sort.Slice(out.Errors, func(i, j int) bool {
		if out.Errors[i].Count != out.Errors[j].Count {
			return out.Errors[i].Count > out.Errors[j].Count
		}

		return out.Errors[i].Reason < out.Errors[j].Reason
	})

	if len(out.Errors) > maxErrorGroups {
		out.Errors = out.Errors[:maxErrorGroups]
		out.ErrorsTruncated = true
	}

	out.Complete = out.StoppedReason == reasonExhausted
	out.Applied = out.Written > 0

	switch {
	case !state.input.Confirm:
		out.Note = fmt.Sprintf(
			"nothing was written to Kafka. %d message(s) would be written to %s/%s. Call again with confirm true to write them.",
			out.Transformed, out.DestinationCluster, out.DestinationTopic)
	default:
		out.Note = fmt.Sprintf(
			"wrote %d message(s) to %s/%s. The originals are unchanged, and a written message cannot be deleted until retention removes it.",
			out.Written, out.DestinationCluster, out.DestinationTopic)
	}

	if !out.Complete {
		out.Note += " The run stopped before the end of the range (" + out.StoppedReason +
			"); scanned_ranges shows how far each partition got, and from_offset with partitions resumes it."
	}
}

// provenanceBase is the part of the provenance that is the same for every
// message of one call.
func provenanceBase(sourceCluster, topic string, destination *kafkaclient.Client, client string) []kgo.RecordHeader {
	principal := "anonymous"

	if cfg := destination.Config(); cfg != nil {
		if options := cfg.AuthenticationOptions(); len(options) > 0 && options[0].User != "" {
			principal = options[0].User
		}
	}

	headers := []kgo.RecordHeader{
		{Key: headerFromCluster, Value: []byte(sourceCluster)},
		{Key: headerFromTopic, Value: []byte(topic)},
		{Key: headerAt, Value: []byte(time.Now().UTC().Format(time.RFC3339))},
		{Key: headerByTool, Value: []byte(toolName)},
		{Key: headerByPrincipal, Value: []byte(principal)},
	}

	if client != "" {
		headers = append(headers, kgo.RecordHeader{Key: headerByClient, Value: []byte(client)})
	}

	return headers
}

// withProvenance appends this call's provenance to a message's headers.
//
// A message transformed twice already carries these headers from the first
// run. They are this tool's bookkeeping rather than data, so the earlier ones
// are replaced: two from-offset headers would leave nobody sure which is true.
func withProvenance(headers []kgo.RecordHeader, base []kgo.RecordHeader, record *kgo.Record) []kgo.RecordHeader {
	ours := map[string]bool{
		headerFromCluster: true, headerFromTopic: true, headerFromPartition: true, headerFromOffset: true,
		headerAt: true, headerByTool: true, headerByPrincipal: true, headerByClient: true,
	}

	kept := make([]kgo.RecordHeader, 0, len(headers)+len(base)+2)

	for _, header := range headers {
		if !ours[header.Key] {
			kept = append(kept, header)
		}
	}

	kept = append(kept, base...)
	kept = append(kept,
		kgo.RecordHeader{Key: headerFromPartition, Value: []byte(strconv.Itoa(int(record.Partition)))},
		kgo.RecordHeader{Key: headerFromOffset, Value: []byte(strconv.FormatInt(record.Offset, 10))},
	)

	return kept
}

// encodeTarget converts a SchemaRef to the codec's target.
func encodeTarget(ref *SchemaRef) serde.Target {
	return serde.Target{
		Subject:     ref.Subject,
		Version:     ref.Version,
		ID:          ref.ID,
		MessageType: ref.MessageType,
	}
}
