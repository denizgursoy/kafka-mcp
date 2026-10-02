// Package samplemessages implements the sample_messages MCP tool.
package samplemessages

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/denizgursoy/kafka-mcp/internal/domain/batch"
	"github.com/denizgursoy/kafka-mcp/internal/domain/records"
	"github.com/denizgursoy/kafka-mcp/internal/domain/serde"
)

// Item is one topic to sample.
type Item struct {
	Topic         string  `json:"topic" jsonschema:"Topic to sample. Matched exactly and case-sensitively."`
	SampleSize    int     `json:"sample_size,omitempty" jsonschema:"Optional number of messages to read in total, spread across partitions. Defaults to 20."`
	Partitions    []int32 `json:"partitions,omitempty" jsonschema:"Optional partitions to sample. Defaults to every partition."`
	MaxValueBytes int     `json:"max_value_bytes,omitempty" jsonschema:"Optional maximum value bytes to return per message. Defaults to 512."`
}

// Input is the argument set accepted by the sample_messages tool.
type Input struct {
	Items []Item `json:"items" jsonschema:"The topics to sample, 1 to 20 of them. Sampling one topic is an array of length one. Results follow this order and a topic that cannot be sampled is reported against its own item."`
}

// Formats counts how many sampled values were of each kind.
type Formats struct {
	JSON        int `json:"json"`
	Text        int `json:"text"`
	Binary      int `json:"binary"`
	Avro        int `json:"avro,omitempty"`
	Protobuf    int `json:"protobuf,omitempty"`
	JSONSchema  int `json:"json_schema,omitempty"`
	Msgpack     int `json:"msgpack,omitempty"`
	Null        int `json:"null,omitempty"`
	Undecodable int `json:"undecodable,omitempty"`
}

// Schema is one schema the sampled values were written with.
type Schema struct {
	Format      string `json:"format"`
	SchemaID    int    `json:"schema_id,omitempty"`
	MessageType string `json:"message_type,omitempty"`
	Count       int    `json:"count"`
}

// Field is one path discovered in the sampled values.
type Field struct {
	Path    string   `json:"path"`
	Types   []string `json:"types"`
	Present int      `json:"present"`
	Example string   `json:"example,omitempty"`
}

// KeyStats describes the message keys in the sample.
type KeyStats struct {
	Present   int      `json:"present"`
	Absent    int      `json:"absent"`
	Unique    int      `json:"unique"`
	AllUnique bool     `json:"all_unique"`
	Examples  []string `json:"examples,omitempty"`
}

// SampledRange reports the offsets a partition was sampled from.
type SampledRange struct {
	Partition int32 `json:"partition"`
	Start     int64 `json:"start"`
	End       int64 `json:"end"`
}

// Output is the result returned by the sample_messages tool.
type Output struct {
	Topic         string            `json:"topic"`
	Messages      []records.Message `json:"messages"`
	SampledRanges []SampledRange    `json:"sampled_ranges"`
	ValueFormats  Formats           `json:"value_formats"`
	JSONFields    []Field           `json:"json_fields"`
	KeyStats      KeyStats          `json:"key_stats"`
	KeyInValue    []string          `json:"key_in_value"`
	Schemas       []Schema          `json:"schemas"`
}

type BatchOutput = batch.Output[Output]

const (
	defaultSampleSize    = 20
	defaultMaxValueBytes = 512

	maxExamples = 3
)

const description = `
Sample the newest messages of 1 to 20 topics in one call through items, and
summarize value formats, field paths, key usage, the schemas values were
written with, and sampled offset ranges. Use this to design a search_messages
predicate, and to find the schema to produce against.

Avro, Protobuf and JSON Schema values carrying a Schema Registry id, and topics
with a configured format, are decoded, so their field paths are reported like
JSON. value_formats.undecodable counts values that named a schema but could not
be decoded; decode_error on each message says why.

Results follow items order, each carrying index with result or error. Sampling
one topic is an items array of length one. The sample describes recent data
only, and keys must not be used to guess partitions.
`

// Register adds the sample_messages tool to the MCP server.
func Register(server *mcp.Server, admin *kadm.Client, reader *records.Reader) {
	mcp.AddTool(
		server,
		&mcp.Tool{
			Name:        "sample_messages",
			Description: description,
		},
		func(
			ctx context.Context,
			req *mcp.CallToolRequest,
			input Input,
		) (*mcp.CallToolResult, BatchOutput, error) {

			out, err := Run(ctx, admin, reader, input)
			if err != nil {
				return nil, BatchOutput{}, fmt.Errorf("sample messages: %w", err)
			}

			return nil, out, nil
		},
	)
}

// Run samples every requested topic with bounded concurrency.
func Run(
	ctx context.Context,
	admin *kadm.Client,
	reader *records.Reader,
	input Input,
) (BatchOutput, error) {

	return batch.Run(ctx, input.Items, batch.MaxHeavyItems, func(ctx context.Context, item Item) (Output, error) {
		return sample(ctx, admin, reader, item)
	})
}

// sample reads the newest messages of one topic and describes their shape.
func sample(
	ctx context.Context,
	admin *kadm.Client,
	reader *records.Reader,
	input Item,
) (Output, error) {

	if input.Topic == "" {
		return Output{}, fmt.Errorf("topic is required")
	}

	size := input.SampleSize
	if size <= 0 {
		size = defaultSampleSize
	}

	maxValueBytes := input.MaxValueBytes
	if maxValueBytes <= 0 {
		maxValueBytes = defaultMaxValueBytes
	}

	ranges, err := newestRanges(ctx, admin, input, size)
	if err != nil {
		return Output{}, err
	}

	out := Output{
		Topic:         input.Topic,
		Messages:      []records.Message{},
		SampledRanges: []SampledRange{},
		JSONFields:    []Field{},
		KeyInValue:    []string{},
		Schemas:       []Schema{},
	}

	for _, rng := range ranges {
		out.SampledRanges = append(out.SampledRanges, SampledRange{
			Partition: rng.Partition,
			Start:     rng.Start,
			End:       rng.End,
		})
	}

	if len(ranges) == 0 {
		return out, nil
	}

	collected := make([]*kgo.Record, 0, size)

	err = reader.Scan(ctx, input.Topic, ranges, func(record *kgo.Record) bool {
		collected = append(collected, record)

		return len(collected) < size
	})
	if err != nil {
		return Output{}, fmt.Errorf("sample %s: %w", input.Topic, err)
	}

	// Records arrive per partition, so order them for a stable report.
	sort.Slice(collected, func(i, j int) bool {
		if collected[i].Partition != collected[j].Partition {
			return collected[i].Partition < collected[j].Partition
		}

		return collected[i].Offset < collected[j].Offset
	})

	describe(ctx, reader, &out, collected, maxValueBytes)

	return out, nil
}

// newestRanges works out which offsets to read: the newest few of every
// sampled partition, so the sample reflects what the topic looks like now.
func newestRanges(
	ctx context.Context,
	admin *kadm.Client,
	input Item,
	size int,
) ([]records.Range, error) {

	details, err := admin.ListTopics(ctx, input.Topic)
	if err != nil {
		return nil, fmt.Errorf("list topic %q: %w", input.Topic, err)
	}

	detail, ok := details[input.Topic]
	if !ok {
		return nil, fmt.Errorf("topic %q does not exist", input.Topic)
	}

	if detail.Err != nil {
		return nil, fmt.Errorf("topic %q: %w", input.Topic, detail.Err)
	}

	wanted := make(map[int32]bool, len(input.Partitions))

	for _, partition := range input.Partitions {
		if _, ok := detail.Partitions[partition]; !ok {
			return nil, fmt.Errorf("topic %q has no partition %d", input.Topic, partition)
		}

		wanted[partition] = true
	}

	starts, err := admin.ListStartOffsets(ctx, input.Topic)
	if err != nil {
		return nil, fmt.Errorf("list start offsets for %q: %w", input.Topic, err)
	}

	ends, err := admin.ListEndOffsets(ctx, input.Topic)
	if err != nil {
		return nil, fmt.Errorf("list end offsets for %q: %w", input.Topic, err)
	}

	partitions := make([]int32, 0, len(detail.Partitions))

	for id := range detail.Partitions {
		if len(wanted) > 0 && !wanted[id] {
			continue
		}

		partitions = append(partitions, id)
	}

	sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })

	if len(partitions) == 0 {
		return nil, nil
	}

	// Spread the sample evenly, giving the earlier partitions the remainder so
	// a sample smaller than the partition count still reads something.
	per := size / len(partitions)
	remainder := size % len(partitions)

	ranges := make([]records.Range, 0, len(partitions))

	for i, id := range partitions {
		share := per
		if i < remainder {
			share++
		}

		if share == 0 {
			continue
		}

		start, hasStart := starts.Lookup(input.Topic, id)
		end, hasEnd := ends.Lookup(input.Topic, id)

		if !hasStart || !hasEnd || end.Offset <= start.Offset {
			continue
		}

		from := end.Offset - int64(share)
		if from < start.Offset {
			from = start.Offset
		}

		ranges = append(ranges, records.Range{
			Partition: id,
			Start:     from,
			End:       end.Offset,
		})
	}

	return ranges, nil
}

// describe fills in the shape report from the sampled records.
func describe(ctx context.Context, reader *records.Reader, out *Output, collected []*kgo.Record, maxValueBytes int) {
	fields := make(map[string]*Field)
	keys := make(map[string]bool)
	keyPaths := make(map[string]int)
	schemas := make(map[Schema]int)
	structuredCount := 0

	for _, record := range collected {
		key := reader.Decode(ctx, record, serde.Key)
		value := reader.Decode(ctx, record, serde.Value)

		out.Messages = append(out.Messages, records.RenderDecoded(record, key, value, maxValueBytes))

		if record.Key == nil {
			out.KeyStats.Absent++
		} else {
			out.KeyStats.Present++
			keys[key.Text] = true

			if len(out.KeyStats.Examples) < maxExamples {
				out.KeyStats.Examples = append(out.KeyStats.Examples, key.Text)
			}
		}

		count(&out.ValueFormats, value)

		if value.SchemaID != 0 || value.MessageType != "" {
			schemas[Schema{Format: value.Format, SchemaID: value.SchemaID, MessageType: value.MessageType}]++
		}

		if value.Document == nil {
			continue
		}

		structuredCount++

		walk("", value.Document, fields)

		if record.Key != nil {
			findKey(key.Text, "", value.Document, keyPaths)
		}
	}

	for schema, seen := range schemas {
		schema.Count = seen
		out.Schemas = append(out.Schemas, schema)
	}

	sort.Slice(out.Schemas, func(i, j int) bool {
		if out.Schemas[i].SchemaID != out.Schemas[j].SchemaID {
			return out.Schemas[i].SchemaID < out.Schemas[j].SchemaID
		}

		return out.Schemas[i].MessageType < out.Schemas[j].MessageType
	})

	out.KeyStats.Unique = len(keys)
	out.KeyStats.AllUnique = out.KeyStats.Present > 0 &&
		len(keys) == out.KeyStats.Present

	for _, field := range fields {
		sort.Strings(field.Types)

		out.JSONFields = append(out.JSONFields, *field)
	}

	sort.Slice(out.JSONFields, func(i, j int) bool {
		return out.JSONFields[i].Path < out.JSONFields[j].Path
	})

	// Only report a path as carrying the key when it did so for every
	// structured message sampled: a single coincidence must not be presented
	// as the identifier.
	for path, count := range keyPaths {
		if count == structuredCount {
			out.KeyInValue = append(out.KeyInValue, path)
		}
	}

	sort.Strings(out.KeyInValue)
}

// count adds one value to the format tally.
func count(formats *Formats, value serde.Decoded) {
	if value.Error != "" {
		formats.Undecodable++

		return
	}

	switch value.Format {
	case serde.FormatJSON:
		formats.JSON++
	case serde.FormatText:
		formats.Text++
	case serde.FormatAvro:
		formats.Avro++
	case serde.FormatProtobuf:
		formats.Protobuf++
	case serde.FormatJSONSchema:
		formats.JSONSchema++
	case serde.FormatMsgpack:
		formats.Msgpack++
	case serde.FormatNull:
		formats.Null++
	default:
		formats.Binary++
	}
}

// walk records every leaf path in a JSON document, with the types seen at it.
func walk(prefix string, document any, fields map[string]*Field) {
	switch typed := document.(type) {
	case map[string]any:
		for key, child := range typed {
			walk(join(prefix, key), child, fields)
		}

	case []any:
		// Arrays are reported by their element paths, using the [*] form the
		// filter language understands.
		for _, child := range typed {
			walk(prefix+"[*]", child, fields)
		}

	default:
		if prefix == "" {
			return
		}

		field, ok := fields[prefix]
		if !ok {
			field = &Field{Path: prefix, Example: example(document)}
			fields[prefix] = field
		}

		field.Present++

		name := typeName(document)

		for _, seen := range field.Types {
			if seen == name {
				return
			}
		}

		field.Types = append(field.Types, name)
	}
}

// findKey records the paths whose value equals the message key, which is what
// shows that the key is a field of the message rather than unrelated.
func findKey(key, prefix string, document any, paths map[string]int) {
	switch typed := document.(type) {
	case map[string]any:
		for name, child := range typed {
			findKey(key, join(prefix, name), child, paths)
		}

	case []any:
		for _, child := range typed {
			findKey(key, prefix+"[*]", child, paths)
		}

	case string:
		if typed == key && prefix != "" {
			paths[prefix]++
		}
	}
}

func join(prefix, key string) string {
	if prefix == "" {
		return key
	}

	return prefix + "." + key
}

func typeName(document any) string {
	switch document.(type) {
	case nil:
		return "null"
	case bool:
		return "boolean"
	case float64:
		return "number"
	case string:
		return "string"
	}

	return "unknown"
}

func example(document any) string {
	switch typed := document.(type) {
	case nil:
		return "null"
	case bool:
		return strconv.FormatBool(typed)
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case string:
		if len(typed) > 60 {
			return typed[:60]
		}

		return typed
	}

	return strings.TrimSpace(fmt.Sprint(document))
}
