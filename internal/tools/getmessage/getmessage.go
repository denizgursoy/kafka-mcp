// Package getmessage implements the get_message MCP tool.
package getmessage

import (
	"context"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/denizgursoy/kafka-mcp/internal/domain/records"
)

// Input is the argument set accepted by the get_message tool.
type Input struct {
	Topic         string `json:"topic" jsonschema:"Topic to read from. Matched exactly and case-sensitively."`
	Partition     int32  `json:"partition" jsonschema:"Partition to read from."`
	Offset        int64  `json:"offset" jsonschema:"Exact offset of the message to read."`
	Context       int    `json:"context,omitempty" jsonschema:"Optional number of messages to also return either side of this offset. Defaults to 0. Clamped to what the partition holds."`
	MaxValueBytes int    `json:"max_value_bytes,omitempty" jsonschema:"Optional maximum number of value bytes to return per message. Defaults to 4096. Values longer than this are cut and flagged with truncated=true."`
}

// Output is the result returned by the get_message tool.
type Output struct {
	Topic   string            `json:"topic"`
	Message records.Message   `json:"message"`
	Before  []records.Message `json:"before"`
	After   []records.Message `json:"after"`
}

const description = `
Read one Kafka message at an exact topic, partition and offset, and optionally
the messages either side of it.

Use this after search_messages has located an offset, to see the full message.
Unlike search results, which are previews, this returns the value up to
max_value_bytes and reports the original size.

Set "context" to also return that many messages before and after the offset,
which shows what a consumer saw around the message. Context is clamped to the
partition's bounds, so asking for more than exists is not an error.

Values that are not valid UTF-8 are base64 encoded and "encoding" is set to
"base64". Fails if the partition does not exist or the offset is past the end
of the partition.
`

// Register adds the get_message tool to the MCP server.
func Register(server *mcp.Server, reader *records.Reader) {
	mcp.AddTool(
		server,
		&mcp.Tool{
			Name:        "get_message",
			Description: description,
		},
		func(
			ctx context.Context,
			req *mcp.CallToolRequest,
			input Input,
		) (*mcp.CallToolResult, Output, error) {

			out, err := Run(ctx, reader, input)
			if err != nil {
				return nil, Output{}, fmt.Errorf("get message: %w", err)
			}

			return nil, out, nil
		},
	)
}

// Run reads the message at an exact offset, plus optional surrounding context.
func Run(
	ctx context.Context,
	reader *records.Reader,
	input Input,
) (Output, error) {

	if input.Topic == "" {
		return Output{}, fmt.Errorf("topic is required")
	}

	if input.Offset < 0 {
		return Output{}, fmt.Errorf("offset must not be negative, got %d", input.Offset)
	}

	if input.Context < 0 {
		return Output{}, fmt.Errorf("context must not be negative, got %d", input.Context)
	}

	start := input.Offset - int64(input.Context)
	if start < 0 {
		start = 0
	}

	end := input.Offset + int64(input.Context) + 1

	found := make([]*kgo.Record, 0, end-start)

	err := reader.Scan(
		ctx,
		input.Topic,
		[]records.Range{{
			Partition: input.Partition,
			Start:     start,
			End:       end,
		}},
		func(record *kgo.Record) bool {
			found = append(found, record)

			return true
		},
	)
	if err != nil {
		return Output{}, fmt.Errorf(
			"read %s partition %d offset %d: %w",
			input.Topic, input.Partition, input.Offset, err,
		)
	}

	out := Output{
		Topic:  input.Topic,
		Before: []records.Message{},
		After:  []records.Message{},
	}

	var hit bool

	for _, record := range found {
		message := records.Render(record, input.MaxValueBytes)

		switch {
		case record.Offset < input.Offset:
			out.Before = append(out.Before, message)
		case record.Offset == input.Offset:
			out.Message = message
			hit = true
		default:
			out.After = append(out.After, message)
		}
	}

	if !hit {
		return Output{}, fmt.Errorf(
			"no message at %s partition %d offset %d: the offset is past the end of the partition, or the partition does not exist",
			input.Topic, input.Partition, input.Offset,
		)
	}

	return out, nil
}
