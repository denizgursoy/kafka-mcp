// Command server runs the Kafka MCP server over HTTP.
//
// A deployment may serve several Kafka clusters and several endpoint policies.
// Each endpoint has its own path and is bound to one cluster, so a caller
// cannot redirect ordinary tools with a parameter it could forget to send.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"

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

	servers := make(map[string]*mcp.Server, len(cfg.Endpoints))

	for _, name := range cfg.EndpointNames() {
		server, err := newServer(cfg, clusters, name)
		if err != nil {
			return fmt.Errorf("serve endpoint %q: %w", name, err)
		}

		servers[name] = server
		endpoint := cfg.Endpoints[name]

		slog.Info("serving endpoint",
			"endpoint", name,
			"cluster", endpoint.Cluster,
			"path", cfg.HTTP.BasePath+endpoint.Path,
			"description", endpoint.Description,
			"read_only", endpoint.ReadOnly,
			"disabled_tools", endpoint.DisabledTools(),
		)
	}

	server := newHTTPServer(cfg, servers)

	return server.StartWithContext(ctx, cfg.HTTP.Address)
}

// newHTTPServer mounts every public endpoint below the configured base path.
// cfg.Load canonicalizes that path to either empty or a leading-slash path
// without a trailing slash.
func newHTTPServer(cfg *config.Config, servers map[string]*mcp.Server) *ada.Server {
	byPath := make(map[string]*mcp.Server, len(cfg.Endpoints))
	for name, endpoint := range cfg.Endpoints {
		byPath[cfg.HTTP.BasePath+endpoint.Path] = servers[name]
	}

	handler := mcp.NewStreamableHTTPHandler(
		func(request *http.Request) *mcp.Server {
			return byPath[request.URL.Path]
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
	// Routes are exact. In particular /mcp and /mcp/rw may safely describe
	// different permissions without the shorter path capturing the longer one.
	for endpointPath := range byPath {
		server.Handle(endpointPath, handler)
	}

	// A liveness endpoint that needs no MCP session, so a container
	// orchestrator can tell the process is up without speaking the protocol.
	server.HandleFunc(cfg.HTTP.BasePath+"/healthz", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, "ok")
	})

	return server
}

// newServer builds the MCP server for one endpoint policy.
//
// Which tools that server ends up with is not decided here: internal/tools
// owns it, so adding or gating a tool never touches main. What main does own
// is refusing to start when registration rejects the configuration, because a
// server that started anyway would serve an endpoint with the wrong tools.
func newServer(
	cfg *config.Config,
	clusters *kafkaclient.Registry,
	name string,
) (*mcp.Server, error) {

	server := mcp.NewServer(
		&mcp.Implementation{
			// Naming the server after its endpoint makes a misdirected client
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
