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

	mcors "github.com/rakunlabs/ada/middleware/cors"
	mlog "github.com/rakunlabs/ada/middleware/log"
	mrecover "github.com/rakunlabs/ada/middleware/recover"
	mrequestid "github.com/rakunlabs/ada/middleware/requestid"
	mserver "github.com/rakunlabs/ada/middleware/server"
	mtelemetry "github.com/rakunlabs/ada/middleware/telemetry"

	"github.com/denizgursoy/kafka-mcp/internal/domain/config"
	"github.com/denizgursoy/kafka-mcp/internal/domain/kafkaclient"
	"github.com/denizgursoy/kafka-mcp/internal/tools"
)

var (
	version = "dev"
	commit  = "-"
	date    = "-"
)

// mcpPathSuffix is appended to the configured HTTP base path. With no base
// path, a cluster named "prod" is served at /mcp/prod.
const mcpPathSuffix = "/mcp/"

func main() {
	into.Init(run,
		into.WithLogger(logi.InitializeLog(logi.WithCaller(false))),
		into.WithMsgf("kafka-mcp version:[%s] commit:[%s] buildDate:[%s]", version, commit, date),
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
	mcpPathPrefix := cfg.HTTP.BasePath + mcpPathSuffix

	for _, name := range cfg.ClusterNames() {
		server, err := newServer(cfg, clusters, name)
		if err != nil {
			return fmt.Errorf("serve cluster %q: %w", name, err)
		}

		servers[name] = server

		slog.Info("serving cluster",
			"cluster", name,
			"path", mcpPathPrefix+name,
			"read_only", cfg.Clusters[name].ReadOnly,
			"disabled_tools", cfg.Clusters[name].DisabledTools(),
		)
	}

	server := newHTTPServer(cfg, servers)

	return server.StartWithContext(ctx, cfg.HTTP.Address)
}

// newHTTPServer mounts every public endpoint below the configured base path.
// cfg.Load canonicalizes that path to either empty or a leading-slash path
// without a trailing slash.
func newHTTPServer(cfg *config.Config, servers map[string]*mcp.Server) *ada.Server {
	mcpPathPrefix := cfg.HTTP.BasePath + mcpPathSuffix

	// The SDK turns a nil server into a 400, so an unknown cluster needs no
	// special case here.
	handler := mcp.NewStreamableHTTPHandler(
		func(request *http.Request) *mcp.Server {
			return servers[strings.TrimPrefix(request.URL.Path, mcpPathPrefix)]
		},
		nil,
	)

	server := ada.New()
	server.Use(
		mrecover.Middleware(),
		mserver.Middleware("kafka-mcp/"+version),
		mcors.Middleware(mcors.WithConfig(cfg.HTTP.CORS)),
		mrequestid.Middleware(),
		mlog.Middleware(),
		mtelemetry.Middleware(),
	)
	server.HandleWildcard(mcpPathPrefix, handler)

	// A liveness endpoint that needs no MCP session, so a container
	// orchestrator can tell the process is up without speaking the protocol.
	server.HandleFunc(cfg.HTTP.BasePath+"/healthz", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, "ok")
	})

	return server
}

// newServer builds the MCP server for one cluster.
//
// Which tools that server ends up with is not decided here: internal/tools
// owns it, so adding or gating a tool never touches main. What main does own
// is refusing to start when registration rejects the configuration, because a
// server that started anyway would serve a cluster with the wrong tools.
func newServer(
	cfg *config.Config,
	clusters *kafkaclient.Registry,
	name string,
) (*mcp.Server, error) {

	server := mcp.NewServer(
		&mcp.Implementation{
			// Naming the server after its cluster makes a misdirected client
			// visible in the initialize handshake, before any tool is called.
			Name: "kafka-mcp-" + name,

			// The build's own version, not a constant. It is the only thing a
			// client sees that says which binary is answering, so a stale
			// deployment is visible in the handshake rather than guessed at
			// from tool behaviour. Local builds report "dev", which is the
			// honest answer for one.
			Version: version,
		},
		nil,
	)

	if err := tools.Register(server, cfg, clusters, name); err != nil {
		return nil, err
	}

	return server, nil
}
