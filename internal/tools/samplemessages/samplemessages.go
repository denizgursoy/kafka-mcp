// Package samplemessages implements the sample_messages MCP tool.
package samplemessages

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/denizgursoy/kafka-mcp/internal/domain/batch"
	"github.com/denizgursoy/kafka-mcp/internal/domain/records"
)

// Item is one topic sample in a batch request.
type Item struct {
	Topic         string  `json:"topic" jsonschema:"Topic to sample. Matched exactly and case-sensitively."`
	SampleSize    int     `json:"sample_size,omitempty" jsonschema:"Optional number of messages to read. Defaults to 20."`
	Partitions    []int32 `json:"partitions,omitempty" jsonschema:"Optional partitions to sample. Defaults to every partition."`
	MaxValueBytes int     `json:"max_value_bytes,omitempty" jsonschema:"Optional maximum value bytes per message. Defaults to 512."`
}

// Input is the argument set accepted by the sample_messages tool.
type Input struct {
	Topic         string  `json:"topic,omitempty" jsonschema:"Topic to sample for a single operation. Omit when items is used."`
	SampleSize    int     `json:"sample_size,omitempty" jsonschema:"Optional number of messages to read in total, spread across partitions. Defaults to 20."`
	Partitions    []int32 `json:"partitions,omitempty" jsonschema:"Optional partitions to sample. Defaults to every partition."`
	MaxValueBytes int     `json:"max_value_bytes,omitempty" jsonschema:"Optional maximum value bytes to return per message. Defaults to 512."`
	Items         []Item  `json:"items,omitempty" jsonschema:"Optional batch of 1 to 20 topic samples. Do not combine with single-operation fields. Results preserve input order."`
}

// Formats counts how many sampled values were of each kind.
type Formats struct {
	JSON   int `json:"json"`
	Text   int `json:"text"`
	Binary int `json:"binary"`
}

// Field is one path discovered in the sampled JSON values.
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
}

type BatchOutput = batch.Output[Output]

type Response struct {
	*Output
	*BatchOutput
}

const (
	defaultSampleSize    = 20
	defaultMaxValueBytes = 512

	maxExamples = 3
)

const description = `
Sample one topic's newest messages, or up to 20 topics through items, and summarize value formats, JSON field paths,
key usage and sampled offset ranges. Use this to design a search_messages
predicate. The sample describes recent data only, and keys must not be used to
guess partitions.
`

// Register adds the sample_messages tool to the MCP server.
func Register(server *mcp.Server, admin *kadm.Client, reader *records.Reader) {
	mcp.AddTool(
		server,
		&mcp.Tool{
			Name:         "sample_messages",
			Description:  description,
			OutputSchema: batch.OutputSchema[Output](),
		},
		func(
			ctx context.Context,
			req *mcp.CallToolRequest,
			input Input,
		) (*mcp.CallToolResult, Response, error) {

			if input.Items != nil {
				if input.Topic != "" || input.SampleSize != 0 || len(input.Partitions) != 0 || input.MaxValueBytes != 0 {
					return nil, Response{}, fmt.Errorf("sample messages: items cannot be combined with single-operation fields")
				}
				out, err := RunBatch(ctx, admin, reader, input.Items)
				if err != nil {
					return nil, Response{}, fmt.Errorf("sample messages batch: %w", err)
				}
				return nil, Response{BatchOutput: &out}, nil
			}

			out, err := Run(ctx, admin, reader, input)
			if err != nil {
				return nil, Response{}, fmt.Errorf("sample messages: %w", err)
			}

			return nil, Response{Output: &out}, nil
		},
	)
}

// RunBatch samples independent topics with bounded concurrency.
func RunBatch(ctx context.Context, admin *kadm.Client, reader *records.Reader, items []Item) (BatchOutput, error) {
	return batch.Run(ctx, items, batch.MaxHeavyItems, func(ctx context.Context, item Item) (Output, error) {
		return Run(ctx, admin, reader, Input{
			Topic: item.Topic, SampleSize: item.SampleSize, Partitions: item.Partitions,
			MaxValueBytes: item.MaxValueBytes,
		})
	})
}

// Run samples the newest messages of a topic and describes their shape.
func Run(
	ctx context.Context,
	admin *kadm.Client,
	reader *records.Reader,
	input Input,
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

	describe(&out, collected, maxValueBytes)

	return out, nil
}

// newestRanges works out which offsets to read: the newest few of every
// sampled partition, so the sample reflects what the topic looks like now.
func newestRanges(
	ctx context.Context,
	admin *kadm.Client,
	input Input,
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
func describe(out *Output, collected []*kgo.Record, maxValueBytes int) {
	fields := make(map[string]*Field)
	keys := make(map[string]bool)
	keyPaths := make(map[string]int)

	for _, record := range collected {
		out.Messages = append(out.Messages, records.Render(record, maxValueBytes))

		if record.Key == nil {
			out.KeyStats.Absent++
		} else {
			out.KeyStats.Present++
			keys[string(record.Key)] = true

			if len(out.KeyStats.Examples) < maxExamples {
				out.KeyStats.Examples = append(out.KeyStats.Examples, string(record.Key))
			}
		}

		var document any

		if err := json.Unmarshal(record.Value, &document); err != nil {
			if utf8.Valid(record.Value) {
				out.ValueFormats.Text++
			} else {
				out.ValueFormats.Binary++
			}

			continue
		}

		out.ValueFormats.JSON++

		walk("", document, fields)

		if record.Key != nil {
			findKey(string(record.Key), "", document, keyPaths)
		}
	}

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

	// Only report a path as carrying the key when it did so for every JSON
	// message sampled: a single coincidence must not be presented as the
	// identifier.
	for path, count := range keyPaths {
		if count == out.ValueFormats.JSON {
			out.KeyInValue = append(out.KeyInValue, path)
		}
	}

	sort.Strings(out.KeyInValue)
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
