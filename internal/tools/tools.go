// Package tools registers the MCP tools one endpoint policy exposes.
//
// It sits at the top of internal/tools, above the one-package-per-tool
// directories it registers. Those directories are still the list of tools the
// server has; this file is the list of tools a given endpoint offers, which is
// not the same thing once a read-only endpoint drops the ones that write, or
// an endpoint switches one off in its `tools` configuration.
//
// Keeping it here rather than in cmd/server means adding a tool changes this
// package and nothing in main.
package tools

import (
	"fmt"
	"sort"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/denizgursoy/kafka-mcp/internal/domain/config"
	"github.com/denizgursoy/kafka-mcp/internal/domain/kafkaclient"
	"github.com/denizgursoy/kafka-mcp/internal/tools/addpartitions"
	"github.com/denizgursoy/kafka-mcp/internal/tools/commitoffset"
	"github.com/denizgursoy/kafka-mcp/internal/tools/consumerlag"
	"github.com/denizgursoy/kafka-mcp/internal/tools/copymessage"
	"github.com/denizgursoy/kafka-mcp/internal/tools/createtopic"
	"github.com/denizgursoy/kafka-mcp/internal/tools/describetopic"
	"github.com/denizgursoy/kafka-mcp/internal/tools/getmessage"
	"github.com/denizgursoy/kafka-mcp/internal/tools/listclusters"
	"github.com/denizgursoy/kafka-mcp/internal/tools/listconsumergroups"
	"github.com/denizgursoy/kafka-mcp/internal/tools/listtopics"
	"github.com/denizgursoy/kafka-mcp/internal/tools/samplemessages"
	"github.com/denizgursoy/kafka-mcp/internal/tools/searchmessages"
	"github.com/denizgursoy/kafka-mcp/internal/tools/serverconfig"
)

// ServerConfig is the one tool a cluster may not switch off.
//
// It is how a session learns which cluster it reached, whether that cluster is
// read-only, and which tools this endpoint has. A session without it cannot
// tell a withheld tool from a missing feature, and every skill that changes
// anything starts by calling it.
const ServerConfig = "server_config"

// Names is every tool this server has, whatever any one endpoint exposes.
//
// It is the set a cluster's `tools` configuration may switch off, so it has to
// name the tools a given endpoint drops as well — otherwise disabling
// add_partitions would read as a typo on a read-only endpoint. tools_test.go
// asserts that a writable cluster with nothing disabled exposes exactly this
// list, so a tool added without a line here fails the build's tests rather
// than becoming un-switchable.
func Names() []string {
	return []string{
		"add_partitions",
		"commit_offset",
		"consumer_lag",
		"copy_message",
		"create_topic",
		"describe_topic",
		"get_message",
		"list_clusters",
		"list_consumer_groups",
		"list_topics",
		"sample_messages",
		"search_messages",
		ServerConfig,
	}
}

// Register adds every tool the named endpoint exposes.
//
// Each tool is bound to that cluster here, so nothing a caller sends can
// redirect it to another. The registry is passed as well because two tools
// need the whole roster rather than one cluster.
//
// It returns an error when the cluster's `tools` configuration names something
// that is not a tool, so a mistyped switch stops the server at startup instead
// of leaving the tool it was meant to withhold quietly registered.
func Register(
	server *mcp.Server,
	cfg *config.Config,
	clusters *kafkaclient.Registry,
	name string,
) error {

	endpointConfig := cfg.Endpoints[name]
	if endpointConfig == nil {
		return fmt.Errorf("unknown endpoint %q", name)
	}
	kafka := clusters.Endpoint(name)
	if kafka == nil {
		return fmt.Errorf("endpoint %q references unavailable cluster %q", name, endpointConfig.Cluster)
	}

	if err := Validate(endpointConfig); err != nil {
		return err
	}

	endpoint := &endpoint{config: endpointConfig}

	// Every tool registers itself: one call per tool, no Kafka logic and no
	// tool schema here.
	//
	// The name is passed to add rather than kept in a second list, because
	// server_config reports what this endpoint exposes and the MCP server
	// offers no way to read back what was registered. Tying the name to the
	// registration is what stops the two from disagreeing.
	endpoint.add("list_topics", func() { listtopics.Register(server, kafka.Admin()) })
	endpoint.add("list_consumer_groups", func() { listconsumergroups.Register(server, kafka.Admin()) })
	endpoint.add("consumer_lag", func() { consumerlag.Register(server, kafka.Admin()) })
	endpoint.add("describe_topic", func() { describetopic.Register(server, kafka.Admin(), kafka.Reader()) })
	endpoint.add("sample_messages", func() { samplemessages.Register(server, kafka.Admin(), kafka.Reader()) })
	endpoint.add("search_messages", func() {
		searchmessages.Register(server, kafka.Admin(), kafka.Reader(), cfg.OutputDir)
	})
	endpoint.add("get_message", func() { getmessage.Register(server, kafka.Reader()) })

	// A read-only endpoint is not offered the tools whose only purpose is to
	// change it. They all refuse at the point of mutation anyway, but a
	// preview they can never apply describes a capability this endpoint does
	// not have, and a tool a caller never sees costs no call to discover.
	//
	// This decides what is advertised, not what is permitted: RequireWritable
	// stays inside each tool, so the refusal survives a registration mistake.
	if !endpointConfig.ReadOnly {
		endpoint.add("add_partitions", func() { addpartitions.Register(server, kafka, kafka.Reader()) })
		endpoint.add("commit_offset", func() { commitoffset.Register(server, kafka) })
		endpoint.add("create_topic", func() { createtopic.Register(server, kafka) })
	}

	// These two need the whole roster rather than one cluster: copy_message so
	// it can write to another cluster, list_clusters so a caller can discover
	// which names are valid.
	//
	// A read-only endpoint keeps copy_message. read_only protects the cluster
	// being written to, and the destination is chosen per call, so hiding it
	// here would block rescuing a message out of a protected cluster, which is
	// the case the tool exists for.
	endpoint.add("copy_message", func() { copymessage.Register(server, clusters, name) })
	endpoint.add("list_clusters", func() { listclusters.Register(server, clusters) })

	// server_config is registered last and unconditionally, because it is what
	// reports the list the additions above have been building.
	endpoint.names = append(endpoint.names, ServerConfig)

	serverconfig.Register(server, kafka, cfg, endpointConfig, endpoint.names)

	return nil
}

// Validate reports whether a cluster's `tools` configuration makes sense.
//
// It is exported so a deployment fails at startup rather than on the call that
// discovers a tool is missing.
func Validate(endpoint *config.Endpoint) error {
	if endpoint == nil || len(endpoint.Tools) == 0 {
		return nil
	}

	known := make(map[string]struct{}, len(Names()))
	for _, name := range Names() {
		known[name] = struct{}{}
	}

	unknown := make([]string, 0)

	for name := range endpoint.Tools {
		if _, ok := known[name]; !ok {
			unknown = append(unknown, name)
		}
	}

	if len(unknown) > 0 {
		// Go map order is random, and an error that lists the same names in a
		// different order every run is harder to compare against the file.
		sort.Strings(unknown)

		return fmt.Errorf(
			"endpoint %q configures unknown tool(s) %v: a name that is not a tool switches nothing off, so the tool it was meant to withhold would stay exposed. The tools are %v",
			endpoint.Name, unknown, Names())
	}

	if !endpoint.ToolEnabled(ServerConfig) {
		return fmt.Errorf(
			"endpoint %q disables %s, which is not allowed: it is how a session learns the cluster it reached, whether that endpoint is read-only, and which tools it has, so withholding it leaves a caller unable to tell a disabled tool from a missing one",
			endpoint.Name, ServerConfig)
	}

	return nil
}

// endpoint collects the tools one endpoint policy ends up with.
//
// The names it gathers are what server_config reports, so registration and
// reporting cannot drift: a tool is either added through here and reported, or
// not added at all.
type endpoint struct {
	config *config.Endpoint
	names  []string
}

// add registers one tool unless this cluster switched it off.
func (e *endpoint) add(name string, register func()) {
	if !e.config.ToolEnabled(name) {
		return
	}

	register()

	e.names = append(e.names, name)
}
