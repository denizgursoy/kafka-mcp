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

	"github.com/denizgursoy/kafka-mcp/internal/domain/kafkaclient"
	"github.com/denizgursoy/kafka-mcp/internal/domain/records"
)

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
// The message is named by its address rather than its content, so this tool
// can only duplicate something the cluster already holds. It has no way to
// write a message that no producer sent.
type Input struct {
	SourceTopic        string `json:"source_topic" jsonschema:"Topic holding the message to copy. Matched exactly and case-sensitively."`
	SourcePartition    int32  `json:"source_partition" jsonschema:"Partition holding the message to copy."`
	SourceOffset       int64  `json:"source_offset" jsonschema:"Exact offset of the message to copy."`
	DestinationTopic   string `json:"destination_topic" jsonschema:"Topic to write the copy to. It must already exist. It must differ from the source topic when both are on the same cluster."`
	DestinationCluster string `json:"destination_cluster,omitempty" jsonschema:"Optional cluster to write the copy to. Defaults to the cluster this endpoint serves. Use list_clusters to see which names are valid. The destination cluster must not be read-only; the source may be."`
	Confirm            bool   `json:"confirm,omitempty" jsonschema:"Optional. When false or omitted, nothing is written and the response shows the message that would be copied. Must be true to actually write it."`
	MaxValueBytes      int    `json:"max_value_bytes,omitempty" jsonschema:"Optional maximum number of value bytes to show in the response. Defaults to 4096. This affects the preview only: the copy always carries the whole value."`
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
	Applied            bool            `json:"applied"`
	WrittenPartition   int32           `json:"written_partition,omitempty"`
	WrittenOffset      int64           `json:"written_offset,omitempty"`
	ProvenanceHeaders  []string        `json:"provenance_headers,omitempty"`
	Warnings           []string        `json:"warnings,omitempty"`
	Note               string          `json:"note,omitempty"`
}

const description = `
Copy one message to another topic, preserving its key, value and headers.

Use this to preserve a message before it becomes unreachable, most often
writing a message that is blocking a consumer into a dead letter topic before
skipping past it. It also serves copying a message from one environment into
another to reproduce a problem.

The message is named by topic, partition and offset. There is no way to supply
content, so this tool can only duplicate a message the cluster already holds.

Set "destination_cluster" to copy to another cluster this server serves, which
is how a message is taken from production into a preproduction topic to be
debugged safely. Omit it to copy within the cluster this endpoint serves. Use
list_clusters to see which names are valid.

read_only protects the cluster being written to. A read-only cluster can be
the source of a copy, because copying out of it changes nothing; it cannot be
the destination.

Every copy carries provenance headers recording the cluster, topic, partition
and offset it came from, when it was copied, by which tool, and the principal this server
connects as. A copy is therefore traceable back to its original, which is what
keeps a dead letter topic from becoming a pile of messages nobody can explain.
If the original already carries one of those headers, the original is kept and
the collision is reported, so provenance never overwrites real data.

The server cannot identify a person: it is reached over stdio and every call
looks alike, so the recorded principal is the Kafka identity it authenticates
as, not a user.

Nothing is written unless "confirm" is true. Without it the response shows the
message that would be copied.

The destination topic must already exist. This tool is refused entirely on a
read-only server, including the preview, because writing is all it does.
Producing needs write permission on the destination topic.
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
		) (*mcp.CallToolResult, Output, error) {

			client := ""

			if info := req.ClientInfo(); info != nil {
				client = info.Name
			}

			out, err := run(ctx, clusters, own, input, client)
			if err != nil {
				return nil, Output{}, fmt.Errorf("copy message: %w", err)
			}

			return nil, out, nil
		},
	)
}

// Run previews or performs a copy.
func Run(
	ctx context.Context,
	clusters *kafkaclient.Registry,
	own string,
	input Input,
) (Output, error) {

	return run(ctx, clusters, own, input, "")
}

func run(
	ctx context.Context,
	clusters *kafkaclient.Registry,
	own string,
	input Input,
	client string,
) (Output, error) {

	source := clusters.Get(own)
	if source == nil {
		return Output{}, fmt.Errorf("unknown cluster %q", own)
	}

	// The destination defaults to this endpoint's own cluster, so a copy
	// within one cluster needs no extra parameter.
	destination := source
	destinationName := own

	if input.DestinationCluster != "" && input.DestinationCluster != own {
		destination = clusters.Get(input.DestinationCluster)
		if destination == nil {
			return Output{}, fmt.Errorf(
				"unknown destination_cluster %q: use list_clusters to see which clusters this server serves",
				input.DestinationCluster)
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

	if input.SourceTopic == input.DestinationTopic && destinationName == own {
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
		SourceCluster:      own,
		DestinationCluster: destinationName,
		SourceTopic:        input.SourceTopic,
		SourcePartition:    input.SourcePartition,
		SourceOffset:       input.SourceOffset,
		DestinationTopic:   input.DestinationTopic,
		Message:            records.Render(record, input.MaxValueBytes),
		Warnings:           []string{},
	}

	headers, added, collisions := withProvenance(record, input, own, destination, client)

	out.ProvenanceHeaders = added

	for _, key := range collisions {
		out.Warnings = append(out.Warnings, fmt.Sprintf(
			"the message already carries the header %q, so it was kept and this copy's provenance for it was not recorded",
			key))
	}

	if !input.Confirm {
		out.Note = "nothing was written. Call again with confirm true to copy the message."

		return out, nil
	}

	written, err := produce(ctx, destination, input.DestinationTopic, record, headers)
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

// withProvenance returns the headers the copy will carry: the original ones,
// plus a record of where this copy came from.
func withProvenance(
	record *kgo.Record,
	input Input,
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

	if cfg := destination.Config(); cfg != nil && cfg.SASL != nil && cfg.SASL.User != "" {
		principal = cfg.SASL.User
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
	input Input,
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
	if !ok {
		return fmt.Errorf(
			"destination topic %q does not exist: create it first, so a mistyped name cannot scatter messages into a topic nobody meant to make",
			topic)
	}

	if detail.Err != nil {
		return fmt.Errorf("destination topic %q: %w", topic, detail.Err)
	}

	return nil
}
