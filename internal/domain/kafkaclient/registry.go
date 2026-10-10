package kafkaclient

import (
	"context"
	"fmt"
	"slices"
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

// endpointConfig returns the policy of a configured endpoint. A name that is
// not an endpoint but is a cluster gets that cluster's legacy policy, which is
// what direct package tests and callers predating explicit endpoints pass.
func (r *Registry) endpointConfig(name string) *config.Endpoint {
	if r == nil || r.cfg == nil {
		return nil
	}
	if endpoint := r.cfg.Endpoints[name]; endpoint != nil {
		return endpoint
	}
	if r.cfg.Clusters[name] != nil {
		return config.LegacyEndpoints(r.cfg.Clusters)[name]
	}

	return nil
}

// Writable reports whether a session on endpoint may write to cluster: its
// own cluster when the endpoint is not read_only, another only when the
// endpoint lists it in destinations, and never a cluster configured read_only.
func (r *Registry) Writable(endpoint string, cluster string) bool {
	policy := r.endpointConfig(endpoint)
	client := r.clients[cluster]
	if policy == nil || client == nil || client.Config().ReadOnly {
		return false
	}
	if cluster == policy.Cluster {
		return !policy.ReadOnly
	}

	return slices.Contains(policy.Destinations, cluster)
}

// Destination returns the client a cross-cluster write from endpoint should
// use for cluster, scoped so RequireWritable gives the effective answer.
//
// A cluster the endpoint does not list is refused here, before anything is
// read, even when some other endpoint could write to it: that permission
// belongs to a different session. A listed cluster configured read_only is
// returned, and refuses at RequireWritable with its own reason.
func (r *Registry) Destination(endpoint string, cluster string) (*Client, error) {
	policy := r.endpointConfig(endpoint)
	if policy == nil {
		return nil, fmt.Errorf("unknown endpoint %q", endpoint)
	}

	client := r.Exposed(cluster)
	if client == nil {
		return nil, fmt.Errorf(
			"unknown destination_cluster %q: use list_clusters to see which clusters this server serves", cluster)
	}

	if cluster != policy.Cluster && !slices.Contains(policy.Destinations, cluster) {
		return nil, fmt.Errorf(
			"endpoint %q may not write to cluster %q: it is not in this endpoint's destinations %v",
			policy.Name, cluster, policy.Destinations)
	}

	return client.ForEndpoint(&config.Endpoint{
		Name:     policy.Name,
		Cluster:  cluster,
		ReadOnly: !r.Writable(endpoint, cluster),
	}), nil
}

// Exposed returns the client for a cluster that at least one endpoint serves,
// or nil. Cross-cluster tools use it so that a cluster defined in the
// configuration but never given an endpoint stays unreachable.
func (r *Registry) Exposed(name string) *Client {
	if r == nil || r.cfg == nil {
		return nil
	}
	if len(r.cfg.Endpoints) == 0 {
		return r.clients[name]
	}
	for _, endpoint := range r.cfg.Endpoints {
		if endpoint.Cluster == name {
			return r.clients[name]
		}
	}

	return nil
}

// ExposedNames returns, sorted, every cluster at least one endpoint serves.
func (r *Registry) ExposedNames() []string {
	names := make([]string, 0, len(r.names))
	for _, name := range r.names {
		if r.Exposed(name) != nil {
			names = append(names, name)
		}
	}

	return names
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
