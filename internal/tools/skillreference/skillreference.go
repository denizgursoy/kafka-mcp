// Package skillreference serves the agent guidance bundled with this release.
package skillreference

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/denizgursoy/kafka-mcp/internal/domain/batch"
)

// Item identifies a built-in document, not a path on the server's filesystem.
type Item struct {
	Skill string `json:"skill" jsonschema:"Required skill name, matched exactly and case-sensitively. Available: kafka-debugging."`
	File  string `json:"file,omitempty" jsonschema:"Optional document name relative to the skill root. Defaults to SKILL.md, the scenario routing guide. Use references/check-lag.md for a scenario guide. Names are exact and case-sensitive; absolute paths and path traversal are not accepted."`
}

// Input requests one or more independent document reads.
type Input struct {
	Items []Item `json:"items" jsonschema:"Required array of 1 to 1000 document requests. Read one document with an array of length one. Each result has its input index and either a document or an error."`
}

// Output contains the original Markdown and the names available for a follow-up.
type Output struct {
	Skill   string   `json:"skill"`
	File    string   `json:"file"`
	Content string   `json:"content"`
	Files   []string `json:"files"`
}

type BatchOutput = batch.Output[Output]

const description = `Read agent skill documents built into this version of kafka-mcp, without local file access or a separate skill installation. Start with items: [{"skill":"kafka-debugging"}] to load SKILL.md and choose a scenario, then request its references/*.md guide before calling Kafka tools. Each result returns skill, file, the full Markdown content and a sorted files catalog. The optional file defaults to SKILL.md; skill and file names match exactly and case-sensitively. Unknown names return an item error listing available choices. Reads only bundled text, never disk paths, URLs or Kafka. Takes 1 to 1000 items; results preserve input order with index and result or error. The batch is non-atomic: one invalid document does not hide the others. No changes are applied.`

const instructions = `Kafka debugging guidance is built into this server. Before investigating a Kafka scenario, call skill_reference with {"items":[{"skill":"kafka-debugging"}]} to read SKILL.md, then use the same tool with the chosen file (for example references/check-lag.md). File names are relative to that skill's root; resolve links in a references/*.md guide relative to that guide. Read only the scenario guides you need. Before promising any change, call server_config to check the endpoint's cluster, permissions and available tools; a guide does not grant capabilities withheld by this endpoint. Preview writes and obtain explicit user approval before confirming them.`

// Register adds both the read-only tool and its connection-time bootstrap. The
// endpoint registry calls this only when the tool is enabled, so the instructions
// can never point at a withheld tool. Host instructions are retained unchanged.
func Register(server *mcp.Server) {
	server.AddReceivingMiddleware(bootstrap)
	closedWorld, nonDestructive := false, false
	mcp.AddTool(server, &mcp.Tool{
		Name: "skill_reference", Description: description,
		Annotations: &mcp.ToolAnnotations{
			ReadOnlyHint: true, IdempotentHint: true,
			OpenWorldHint: &closedWorld, DestructiveHint: &nonDestructive,
		},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input Input) (*mcp.CallToolResult, BatchOutput, error) {
		out, err := Run(ctx, input)
		if err != nil {
			return nil, BatchOutput{}, fmt.Errorf("skill reference: %w", err)
		}
		return nil, out, nil
	})
}

// Run reads only the embedded catalog. It deliberately takes no Kafka client:
// serving release-specific instructions requires no cluster access.
func Run(ctx context.Context, input Input) (BatchOutput, error) {
	if err := batch.Validate(len(input.Items), batch.MaxItems); err != nil {
		return BatchOutput{}, err
	}
	bundled, err := catalog()
	if err != nil {
		return BatchOutput{}, err
	}
	return batch.Run(ctx, input.Items, batch.MaxItems, func(_ context.Context, item Item) (Output, error) {
		docs, ok := bundled[item.Skill]
		if !ok {
			return Output{}, fmt.Errorf("unknown skill %q; available skills: %s", item.Skill, strings.Join(names(bundled), ", "))
		}
		file := item.File
		if file == "" {
			file = "SKILL.md"
		}
		content, ok := docs[file]
		if !ok {
			return Output{}, fmt.Errorf("unknown file %q for %s; available files: %s", file, item.Skill, strings.Join(names(docs), ", "))
		}
		return Output{Skill: item.Skill, File: file, Content: content, Files: names(docs)}, nil
	})
}

func names[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

// bootstrap changes only discovery results, copying them before appending so a
// host or another middleware never sees its own result mutated. Both protocol
// generations need the pointer: initialize and the newer server/discover.
func bootstrap(next mcp.MethodHandler) mcp.MethodHandler {
	return func(ctx context.Context, method string, request mcp.Request) (mcp.Result, error) {
		result, err := next(ctx, method, request)
		if err != nil {
			return result, err
		}
		switch result := result.(type) {
		case *mcp.InitializeResult:
			copy := *result
			copy.Instructions = appendInstructions(copy.Instructions)
			return &copy, nil
		case *mcp.DiscoverResult:
			copy := *result
			copy.Instructions = appendInstructions(copy.Instructions)
			return &copy, nil
		default:
			return result, nil
		}
	}
}

func appendInstructions(existing string) string {
	if existing == "" {
		return instructions
	}
	return existing + "\n\n" + instructions
}
