// Package serverconfig implements the server_config MCP tool.
package serverconfig

import (
	"context"
	"fmt"
	"sort"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/denizgursoy/kafka-mcp/internal/domain/config"
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
	Cluster        string                `json:"cluster"`
	Brokers        []string              `json:"brokers"`
	Authentication string                `json:"authentication"`
	SASLUser       string                `json:"sasl_user,omitempty"`
	SASLOptions    []config.SASLIdentity `json:"sasl_options,omitempty"`
	TLS            bool                  `json:"tls"`
	ReadOnly       bool                  `json:"read_only"`
	OutputDir      string                `json:"output_dir"`
	ConfigFile     string                `json:"config_file,omitempty"`
	HTTPAddress    string                `json:"http_address,omitempty"`
	Tools          []string              `json:"tools"`
	Note           string                `json:"note"`
}

const description = `
Report how this server is configured: which brokers it is connected to, which
identity it authenticates as, whether it may change the cluster, and which
tools it exposes.

Use this when a result is surprising. An empty topic list means something very
different depending on whether the server is pointed at a local broker or a
production cluster, and this is the only way to tell from inside a session.

"cluster" is the cluster this endpoint serves. A server may serve several,
each on its own endpoint, so this is how a session confirms which one it is
talking to rather than inferring it from a tool name the client chose.

"read_only" means this server refuses operations that would change the
cluster. It protects a cluster that has no ACLs of its own; it is not a
security boundary, because whoever can edit the configuration can turn it off.

"tools" is what this endpoint exposes, not what the deployment can do. A
read-only cluster does not register the tools whose only purpose is to change
it, so they are absent here and absent from the tool list. Do not tell the user
a tool is missing when this reports read_only true: the cluster is protected,
which is a different answer.

"authentication" and "sasl_user" describe the first configured SASL option.
"sasl_options" lists all configured mechanisms and identities in preference
order, including optional authorization identities (zid). These are configured
preferences, not the negotiated identity of an individual broker connection.
Kafka ACLs apply to the authenticated identity even when read_only is false.

The password is never reported.
`

// Register adds the server_config tool to the MCP server.
//
// The tool names are passed in because the MCP server exposes no way to read
// back what has been registered. Keeping the list in main, beside the
// registrations themselves, is the closest thing to a single source.
func Register(
	server *mcp.Server,
	kafka *kafkaclient.Client,
	cfg *config.Config,
	tools []string,
) {
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

			out, err := Run(kafka, cfg, tools)
			if err != nil {
				return nil, Output{}, fmt.Errorf("server config: %w", err)
			}

			return nil, out, nil
		},
	)
}

// Run reports the effective configuration of the running server.
func Run(kafka *kafkaclient.Client, server *config.Config, tools []string) (Output, error) {
	cfg := kafka.Config()

	if cfg == nil {
		return Output{}, fmt.Errorf("server has no configuration")
	}

	out := Output{
		Cluster:        cfg.Name,
		Brokers:        cfg.Brokers,
		Authentication: "none",
		TLS:            cfg.TLS != nil && cfg.TLS.Enabled,
		ReadOnly:       cfg.ReadOnly,
		Tools:          append([]string{}, tools...),
		Note: "read_only protects a cluster without ACLs and can be turned off by " +
			"anyone who can edit the configuration. Real authorisation comes from " +
			"Kafka ACLs on the authenticated identity; sasl_options reports configured preferences.",
	}

	if server != nil {
		out.OutputDir = server.OutputDir
		out.ConfigFile = server.Path
		out.HTTPAddress = server.HTTP.Address
	}

	out.SASLOptions = cfg.AuthenticationOptions()
	if len(out.SASLOptions) > 0 {
		out.Authentication = out.SASLOptions[0].Mechanism
		out.SASLUser = out.SASLOptions[0].User
	}

	sort.Strings(out.Tools)

	return out, nil
}
