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
	Endpoint       string                `json:"endpoint"`
	Path           string                `json:"path"`
	Description    string                `json:"description,omitempty"`
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
Report this endpoint's name, path, purpose, cluster, brokers, authentication,
TLS, read-only policy and exposed tools. Use it to confirm the target and
permissions before acting; several endpoints may target one cluster with
different policies. Passwords are never returned.
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
	endpoint *config.Endpoint,
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

			out, err := Run(kafka, cfg, endpoint, tools)
			if err != nil {
				return nil, Output{}, fmt.Errorf("server config: %w", err)
			}

			return nil, out, nil
		},
	)
}

// Run reports the effective configuration of the running server.
func Run(kafka *kafkaclient.Client, server *config.Config, endpoint *config.Endpoint, tools []string) (Output, error) {
	cfg := kafka.Config()

	if cfg == nil {
		return Output{}, fmt.Errorf("server has no configuration")
	}

	if endpoint == nil {
		endpoint = kafka.Endpoint()
	}
	if endpoint == nil {
		endpoint = &config.Endpoint{
			Name:     cfg.Name,
			Cluster:  cfg.Name,
			Path:     "/mcp/" + cfg.Name,
			ReadOnly: cfg.ReadOnly,
		}
	}

	out := Output{
		Endpoint:       endpoint.Name,
		Path:           endpoint.Path,
		Description:    endpoint.Description,
		Cluster:        cfg.Name,
		Brokers:        cfg.Brokers,
		Authentication: "none",
		TLS:            cfg.TLS != nil && cfg.TLS.Enabled,
		ReadOnly:       endpoint.ReadOnly,
		Tools:          append([]string{}, tools...),
		Note: "read_only protects a cluster without ACLs and can be turned off by " +
			"anyone who can edit the configuration. Real authorisation comes from " +
			"Kafka ACLs on the authenticated identity; sasl_options reports configured preferences.",
	}

	if server != nil {
		out.OutputDir = server.OutputDir
		out.ConfigFile = server.Path
		out.HTTPAddress = server.HTTP.Address
		out.Path = server.HTTP.BasePath + endpoint.Path
	}

	out.SASLOptions = cfg.AuthenticationOptions()
	if len(out.SASLOptions) > 0 {
		out.Authentication = out.SASLOptions[0].Mechanism
		out.SASLUser = out.SASLOptions[0].User
	}

	sort.Strings(out.Tools)

	return out, nil
}
