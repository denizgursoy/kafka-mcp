package kafkaclient

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/denizgursoy/kafka-mcp/internal/domain/config"
)

// Registry holds one client per configured cluster.
//
// A deployment may serve several clusters at once, each on its own endpoint.
// The registry is what lets a tool reach a cluster other than its own, which
// only copy_message needs, and what lets list_clusters report the roster.
type Registry struct {
	clients map[string]*Client
	names   []string
}

// NewRegistry connects to every configured cluster.
//
// A cluster that cannot be reached is still registered. Its endpoint is
// served and its tools return a connection error naming it, which is more
// useful than refusing to start: one cluster being down must not block
// debugging the others. A cluster that cannot be constructed at all is a
// configuration mistake and does stop the server.
func NewRegistry(cfg *config.Config) (*Registry, error) {
	registry := &Registry{
		clients: make(map[string]*Client, len(cfg.Clusters)),
		names:   make([]string, 0, len(cfg.Clusters)),
	}

	for name, cluster := range cfg.Clusters {
		client, err := New(cluster)
		if err != nil {
			registry.Close()

			return nil, fmt.Errorf("cluster %q: %w", name, err)
		}

		registry.clients[name] = client
		registry.names = append(registry.names, name)
	}

	// Go map order is random, so sort once here rather than at every call
	// that reports the roster.
	sort.Strings(registry.names)

	return registry, nil
}

// Get returns the client for a cluster, or nil when the name is unknown.
//
// Nil is deliberate: the HTTP router turns a nil server into a 400, and a
// tool turns a nil client into an error naming the unknown cluster.
func (r *Registry) Get(name string) *Client {
	return r.clients[name]
}

// Names returns every cluster name, sorted.
func (r *Registry) Names() []string {
	return append([]string{}, r.names...)
}

// Close releases every client.
func (r *Registry) Close() {
	for _, client := range r.clients {
		client.Close()
	}
}

// connectTimeout bounds a connectivity check. list_clusters pings every
// cluster, so an unreachable one must fail quickly rather than hold up the
// answer for the clusters that are up.
const connectTimeout = 3 * time.Second

// Connected reports whether the brokers answer right now.
//
// This is checked per call rather than recorded at startup, because a cluster
// that died since startup is exactly the case someone is asking about.
func (c *Client) Connected(ctx context.Context) bool {
	ctx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()

	return c.Ping(ctx) == nil
}
