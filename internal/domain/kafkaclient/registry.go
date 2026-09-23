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
// A deployment may serve several clusters and several endpoint policies at
// once. The registry reuses one physical connection for every endpoint that
// targets a cluster. It also lets copy_message reach another cluster and lets
// list_clusters report the roster.
type Registry struct {
	clients map[string]*Client
	names   []string
	cfg     *config.Config
}

// NewRegistry connects to every configured cluster.
//
// A cluster that cannot be reached is still registered. Its endpoints are
// served and their tools return a connection error naming it, which is more
// useful than refusing to start: one cluster being down must not block
// debugging the others. A cluster that cannot be constructed at all is a
// configuration mistake and does stop the server.
func NewRegistry(cfg *config.Config) (*Registry, error) {
	registry := &Registry{
		clients: make(map[string]*Client, len(cfg.Clusters)),
		names:   make([]string, 0, len(cfg.Clusters)),
		cfg:     cfg,
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

// Endpoint returns a policy-scoped client for one configured MCP endpoint.
func (r *Registry) Endpoint(name string) *Client {
	if r == nil || r.cfg == nil {
		return nil
	}
	endpoint := r.cfg.Endpoints[name]
	if endpoint == nil {
		return nil
	}
	client := r.clients[endpoint.Cluster]
	if client == nil {
		return nil
	}

	return client.ForEndpoint(endpoint)
}

// ReadOnly reports whether every endpoint for a cluster refuses writes. This
// is the cluster-level answer used when choosing a cross-cluster destination.
func (r *Registry) ReadOnly(name string) bool {
	if r == nil || r.cfg == nil {
		return true
	}
	found := false
	for _, endpoint := range r.cfg.Endpoints {
		if endpoint.Cluster != name {
			continue
		}
		found = true
		if !endpoint.ReadOnly {
			return false
		}
	}
	if found {
		return true
	}
	client := r.clients[name]
	return client == nil || client.Config().ReadOnly
}

// Destination returns a cluster client scoped to the effective destination
// policy used by cross-cluster copy operations.
func (r *Registry) Destination(name string) *Client {
	client := r.Get(name)
	if client == nil {
		return nil
	}

	return client.ForEndpoint(&config.Endpoint{
		Name:     name,
		Cluster:  name,
		ReadOnly: r.ReadOnly(name),
	})
}

// Get returns the client for a cluster, or nil when the name is unknown.
//
// Nil is deliberate: a tool turns it into an error naming the unknown cluster.
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
