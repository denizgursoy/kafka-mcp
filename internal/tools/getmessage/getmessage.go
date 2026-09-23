// Package getmessage implements the get_message MCP tool.
package getmessage

import (
	"context"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/denizgursoy/kafka-mcp/internal/domain/batch"
	"github.com/denizgursoy/kafka-mcp/internal/domain/records"
)

// Item is one exact message address in a batch request.
type Item struct {
	Topic         string `json:"topic" jsonschema:"Topic to read from. Matched exactly and case-sensitively."`
	Partition     int32  `json:"partition" jsonschema:"Partition to read from."`
	Offset        int64  `json:"offset" jsonschema:"Exact offset of the message to read."`
	Context       int    `json:"context,omitempty" jsonschema:"Optional number of messages to also return either side of this offset. Defaults to 0."`
	MaxValueBytes int    `json:"max_value_bytes,omitempty" jsonschema:"Optional maximum number of value bytes to return per message. Defaults to 4096."`
}

// Input is the argument set accepted by the get_message tool.
type Input struct {
	Topic         string `json:"topic,omitempty" jsonschema:"Topic to read from for a single operation. Omit when items is used."`
	Partition     int32  `json:"partition" jsonschema:"Partition to read from."`
	Offset        int64  `json:"offset" jsonschema:"Exact offset of the message to read."`
	Context       int    `json:"context,omitempty" jsonschema:"Optional number of messages to also return either side of this offset. Defaults to 0. Clamped to what the partition holds."`
	MaxValueBytes int    `json:"max_value_bytes,omitempty" jsonschema:"Optional maximum number of value bytes to return per message. Defaults to 4096. Values longer than this are cut and flagged with truncated=true."`
	Items         []Item `json:"items,omitempty" jsonschema:"Optional batch of 1 to 20 message addresses. Do not combine with the single-operation fields. Results preserve input order and item failures do not hide successful reads."`
}

// Output is the result returned by the get_message tool.
type Output struct {
	Topic   string            `json:"topic"`
	Message records.Message   `json:"message"`
	Before  []records.Message `json:"before"`
	After   []records.Message `json:"after"`
}

type BatchOutput = batch.Output[Output]

// Response preserves the original single-operation result shape while adding
// the batch envelope when items is supplied.
type Response struct {
	*Output
	*BatchOutput
}

const description = `
Read one Kafka message, or up to 20 addresses through items, and optionally
nearby messages for context. Returns key, value, headers, timestamp and original
value size; binary values are base64 encoded. Fails for an invalid partition or
an offset beyond the partition end.
`

// Register adds the get_message tool to the MCP server.
func Register(server *mcp.Server, reader *records.Reader) {
	mcp.AddTool(
		server,
		&mcp.Tool{
			Name:         "get_message",
			Description:  description,
			OutputSchema: batch.OutputSchema[Output](),
		},
		func(
			ctx context.Context,
			req *mcp.CallToolRequest,
			input Input,
		) (*mcp.CallToolResult, Response, error) {

			if input.Items != nil {
				if input.Topic != "" || input.Partition != 0 || input.Offset != 0 || input.Context != 0 || input.MaxValueBytes != 0 {
					return nil, Response{}, fmt.Errorf("get message: items cannot be combined with single-operation fields")
				}

				out, err := RunBatch(ctx, reader, input.Items)
				if err != nil {
					return nil, Response{}, fmt.Errorf("get messages: %w", err)
				}

				return nil, Response{BatchOutput: &out}, nil
			}

			out, err := Run(ctx, reader, input)
			if err != nil {
				return nil, Response{}, fmt.Errorf("get message: %w", err)
			}

			return nil, Response{Output: &out}, nil
		},
	)
}

// RunBatch reads independent message addresses with bounded concurrency.
func RunBatch(ctx context.Context, reader *records.Reader, items []Item) (BatchOutput, error) {
	return batch.Run(ctx, items, batch.MaxHeavyItems, func(ctx context.Context, item Item) (Output, error) {
		return Run(ctx, reader, Input{
			Topic: item.Topic, Partition: item.Partition, Offset: item.Offset,
			Context: item.Context, MaxValueBytes: item.MaxValueBytes,
		})
	})
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
