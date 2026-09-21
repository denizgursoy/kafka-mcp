// Package tools registers the MCP tools one cluster's endpoint exposes.
//
// It sits at the top of internal/tools, above the one-package-per-tool
// directories it registers. Those directories are still the list of tools the
// server has; this file is the list of tools a given endpoint offers, which is
// not the same thing once a read-only cluster drops the ones that write.
//
// Keeping it here rather than in cmd/server means adding a tool changes this
// package and nothing in main.
package tools

import (
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/denizgursoy/kafka-mcp/internal/domain/config"
	"github.com/denizgursoy/kafka-mcp/internal/domain/kafkaclient"
	"github.com/denizgursoy/kafka-mcp/internal/tools/addpartitions"
	"github.com/denizgursoy/kafka-mcp/internal/tools/commitoffset"
	"github.com/denizgursoy/kafka-mcp/internal/tools/consumerlag"
	"github.com/denizgursoy/kafka-mcp/internal/tools/copymessage"
	"github.com/denizgursoy/kafka-mcp/internal/tools/describetopic"
	"github.com/denizgursoy/kafka-mcp/internal/tools/getmessage"
	"github.com/denizgursoy/kafka-mcp/internal/tools/listclusters"
	"github.com/denizgursoy/kafka-mcp/internal/tools/listconsumergroups"
	"github.com/denizgursoy/kafka-mcp/internal/tools/listtopics"
	"github.com/denizgursoy/kafka-mcp/internal/tools/samplemessages"
	"github.com/denizgursoy/kafka-mcp/internal/tools/searchmessages"
	"github.com/denizgursoy/kafka-mcp/internal/tools/serverconfig"
)

// Register adds every tool the named cluster's endpoint exposes.
//
// Each tool is bound to that cluster here, so nothing a caller sends can
// redirect it to another. The registry is passed as well because two tools
// need the whole roster rather than one cluster.
func Register(
	server *mcp.Server,
	cfg *config.Config,
	clusters *kafkaclient.Registry,
	name string,
) {

	kafka := clusters.Get(name)

	// Every tool registers itself: one call per tool, no Kafka logic and no
	// tool schema here.
	//
	// names is what server_config reports. The MCP server offers no way to
	// read back what has been registered, so the names are listed here beside
	// the registrations they describe, and must be kept with them: an endpoint
	// that drops a tool while still claiming it is worse than one that never
	// reported the list at all.
	listtopics.Register(server, kafka.Admin())
	listconsumergroups.Register(server, kafka.Admin())
	consumerlag.Register(server, kafka.Admin())
	describetopic.Register(server, kafka.Admin(), kafka.Reader())
	samplemessages.Register(server, kafka.Admin(), kafka.Reader())
	searchmessages.Register(server, kafka.Admin(), kafka.Reader(), cfg.OutputDir)
	getmessage.Register(server, kafka.Reader())

	names := []string{
		"consumer_lag",
		"describe_topic",
		"get_message",
		"list_consumer_groups",
		"list_topics",
		"sample_messages",
		"search_messages",
	}

	// A read-only cluster is not offered the tools whose only purpose is to
	// change it. Both refuse at the point of mutation anyway, but a preview
	// they can never apply describes a capability this endpoint does not have,
	// and a tool a caller never sees costs no call to discover.
	//
	// This decides what is advertised, not what is permitted: RequireWritable
	// stays inside both tools, so the refusal survives a registration mistake.
	if !kafka.Config().ReadOnly {
		addpartitions.Register(server, kafka, kafka.Reader())
		commitoffset.Register(server, kafka)

		names = append(names, "add_partitions", "commit_offset")
	}

	// These two need the whole roster rather than one cluster: copy_message so
	// it can write to another cluster, list_clusters so a caller can discover
	// which names are valid.
	//
	// A read-only cluster keeps copy_message. read_only protects the cluster
	// being written to, and the destination is chosen per call, so hiding it
	// here would block rescuing a message out of a protected cluster, which is
	// the case the tool exists for.
	copymessage.Register(server, clusters, name)
	listclusters.Register(server, clusters)

	names = append(names, "copy_message", "list_clusters", "server_config")

	serverconfig.Register(server, kafka, cfg, names)
}
