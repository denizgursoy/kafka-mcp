// Package listtopics implements the list_topics MCP tool.
package listtopics

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/twmb/franz-go/pkg/kadm"
)

// Input is the argument set accepted by the list_topics tool.
type Input struct {
	Search string `json:"search,omitempty" jsonschema:"Optional case-insensitive substring to filter topic names by. Omit or leave empty to list every topic."`
}

// Output is the result returned by the list_topics tool.
type Output struct {
	Topics []string `json:"topics"`
	Count  int      `json:"count"`
}

const description = `
List the Kafka topics on the connected cluster.

Returns the matching topic names sorted alphabetically and their count.
If "search" is provided, only topics whose name contains that text are
returned, matched case-insensitively. If "search" is omitted, every topic
is returned.
`

// Register adds the list_topics tool to the MCP server. The tool owns its own
// name, description and schema, so the server only has to make this one call.
func Register(server *mcp.Server, admin *kadm.Client) {
	mcp.AddTool(
		server,
		&mcp.Tool{
			Name:        "list_topics",
			Description: description,
		},
		func(
			ctx context.Context,
			req *mcp.CallToolRequest,
			input Input,
		) (*mcp.CallToolResult, Output, error) {

			out, err := Run(ctx, admin, input)
			if err != nil {
				return nil, Output{}, fmt.Errorf("list topics: %w", err)
			}

			return nil, out, nil
		},
	)
}

// Run lists Kafka topics, optionally filtered by a case-insensitive substring.
// The returned topic names are sorted so results are deterministic.
func Run(
	ctx context.Context,
	admin *kadm.Client,
	input Input,
) (Output, error) {

	details, err := admin.ListTopics(ctx)
	if err != nil {
		return Output{}, fmt.Errorf("list topics: %w", err)
	}

	search := strings.ToLower(input.Search)

	topics := make([]string, 0, len(details))

	for topic := range details {
		if search != "" &&
			!strings.Contains(strings.ToLower(topic), search) {
			continue
		}

		topics = append(topics, topic)
	}

	sort.Strings(topics)

	return Output{
		Topics: topics,
		Count:  len(topics),
	}, nil
}
