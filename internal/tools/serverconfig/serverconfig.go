// Package serverconfig implements the server_config MCP tool.
package serverconfig

import (
	"context"
	"fmt"
	"sort"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/denizgursoy/kafka-mcp/internal/domain/kafkaclient"
)

// Input is the argument set accepted by the server_config tool. It takes no
// arguments: there is only one configuration to report.
type Input struct{}

// Output is the result returned by the server_config tool.
//
// It deliberately carries no password field. This result is sent to an MCP
// client and may be logged or shown to a model, so a secret must never be
// able to reach it.
type Output struct {
	Environment    string   `json:"environment,omitempty"`
	Brokers        []string `json:"brokers"`
	Authentication string   `json:"authentication"`
	SASLUser       string   `json:"sasl_user,omitempty"`
	TLS            bool     `json:"tls"`
	ReadOnly       bool     `json:"read_only"`
	OutputDir      string   `json:"output_dir"`
	ConfigFile     string   `json:"config_file,omitempty"`
	Tools          []string `json:"tools"`
	Note           string   `json:"note"`
}

const description = `
Report how this server is configured: which brokers it is connected to, which
identity it authenticates as, whether it may change the cluster, and which
tools it exposes.

Use this when a result is surprising. An empty topic list means something very
different depending on whether the server is pointed at a local broker or a
production cluster, and this is the only way to tell from inside a session.

"environment" is a free-form label from the configuration. It changes no
behaviour and is only as accurate as whoever wrote the config file.

"read_only" means this server refuses operations that would change the
cluster. It protects a cluster that has no ACLs of its own; it is not a
security boundary, because whoever can edit the configuration can turn it off.

"sasl_user" is the principal the broker sees. Kafka ACLs are enforced against
it, so it explains why a write may be refused even when read_only is false.

The password is never reported.
`

// Register adds the server_config tool to the MCP server.
//
// The tool names are passed in because the MCP server exposes no way to read
// back what has been registered. Keeping the list in main, beside the
// registrations themselves, is the closest thing to a single source.
func Register(server *mcp.Server, kafka *kafkaclient.Client, tools []string) {
	mcp.AddTool(
		server,
		&mcp.Tool{
			Name:        "server_config",
			Description: description,
		},
		func(
			ctx context.Context,
			req *mcp.CallToolRequest,
			input Input,
		) (*mcp.CallToolResult, Output, error) {

			out, err := Run(kafka, tools)
			if err != nil {
				return nil, Output{}, fmt.Errorf("server config: %w", err)
			}

			return nil, out, nil
		},
	)
}

// Run reports the effective configuration of the running server.
func Run(kafka *kafkaclient.Client, tools []string) (Output, error) {
	cfg := kafka.Config()

	if cfg == nil {
		return Output{}, fmt.Errorf("server has no configuration")
	}

	out := Output{
		Environment:    cfg.Environment,
		Brokers:        cfg.Brokers,
		Authentication: "none",
		TLS:            cfg.TLS != nil && cfg.TLS.Enabled,
		ReadOnly:       cfg.ReadOnly,
		OutputDir:      cfg.OutputDir,
		ConfigFile:     cfg.Path,
		Tools:          append([]string{}, tools...),
		Note: "read_only protects a cluster without ACLs and can be turned off by " +
			"anyone who can edit the configuration. Real authorisation comes from " +
			"Kafka ACLs on the principal in sasl_user.",
	}

	if cfg.SASL != nil {
		out.Authentication = cfg.SASL.Mechanism
		out.SASLUser = cfg.SASL.User
	}

	sort.Strings(out.Tools)

	return out, nil
}
