// Command server runs the Kafka MCP server over HTTP.
//
// A deployment may serve several Kafka clusters. Each is served on its own
// path, so a session is bound to one cluster by the endpoint it connects to
// rather than by a parameter a caller could forget to send.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/rakunlabs/ada"
	"github.com/rakunlabs/into"
	"github.com/rakunlabs/logi"

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

// pathPrefix is where the per-cluster endpoints are mounted. A cluster named
// "prod" is served at /mcp/prod.
const pathPrefix = "/mcp/"

// toolNames is what server_config reports. The MCP server offers no way to
// read back what has been registered, so the list is kept here beside the
// registrations it describes.
var toolNames = []string{
	"add_partitions",
	"commit_offset",
	"consumer_lag",
	"copy_message",
	"describe_topic",
	"get_message",
	"list_clusters",
	"list_consumer_groups",
	"list_topics",
	"sample_messages",
	"search_messages",
	"server_config",
}

func main() {
	into.Init(run,
		into.WithLogger(logi.InitializeLog(logi.WithCaller(false))),
		into.WithMsgf("kafka-mcp"),
	)
}

func run(ctx context.Context) error {
	cfg, err := config.Load(ctx)
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}

	clusters, err := kafkaclient.NewRegistry(cfg)
	if err != nil {
		return fmt.Errorf("create Kafka registry: %w", err)
	}

	defer clusters.Close()

	servers := make(map[string]*mcp.Server, len(cfg.Clusters))

	for _, name := range cfg.ClusterNames() {
		servers[name] = newServer(cfg, clusters, name)

		slog.Info("serving cluster", "cluster", name,
			"path", pathPrefix+name, "read_only", cfg.Clusters[name].ReadOnly)
	}

	// The SDK turns a nil server into a 400, so an unknown cluster needs no
	// special case here.
	handler := mcp.NewStreamableHTTPHandler(
		func(request *http.Request) *mcp.Server {
			return servers[strings.TrimPrefix(request.URL.Path, pathPrefix)]
		},
		nil,
	)

	server := ada.New(ada.WithLogger(slog.Default()))
	server.HandleWildcard(pathPrefix, handler)

	// A liveness endpoint that needs no MCP session, so a container
	// orchestrator can tell the process is up without speaking the protocol.
	server.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, "ok")
	})

	return server.StartWithContext(ctx, cfg.HTTP.Address)
}

// newServer builds the MCP server for one cluster. Every tool is bound to
// that cluster, so nothing a caller sends can redirect it to another.
func newServer(
	cfg *config.Config,
	clusters *kafkaclient.Registry,
	name string,
) *mcp.Server {

	kafka := clusters.Get(name)

	server := mcp.NewServer(
		&mcp.Implementation{
			// Naming the server after its cluster makes a misdirected client
			// visible in the initialize handshake, before any tool is called.
			Name:    "kafka-debugger-" + name,
			Version: "1.0.0",
		},
		nil,
	)

	// Every tool registers itself: one call per tool, no Kafka logic and no
	// tool schema here.
	listtopics.Register(server, kafka.Admin())
	listconsumergroups.Register(server, kafka.Admin())
	consumerlag.Register(server, kafka.Admin())
	describetopic.Register(server, kafka.Admin(), kafka.Reader())
	samplemessages.Register(server, kafka.Admin(), kafka.Reader())
	searchmessages.Register(server, kafka.Admin(), kafka.Reader(), cfg.OutputDir)
	getmessage.Register(server, kafka.Reader())
	addpartitions.Register(server, kafka, kafka.Reader())
	commitoffset.Register(server, kafka)

	// These two need the whole roster rather than one cluster: copy_message so
	// it can write to another cluster, list_clusters so a caller can discover
	// which names are valid.
	copymessage.Register(server, clusters, name)
	listclusters.Register(server, clusters)

	serverconfig.Register(server, kafka, cfg, toolNames)

	return server
}
