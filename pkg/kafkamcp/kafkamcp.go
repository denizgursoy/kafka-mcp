// Package kafkamcp registers kafka-mcp's tools on an MCP server owned by
// another Go application, using Kafka clients that application already has.
//
// It opens no listener, loads no configuration and dials nothing: routing,
// authentication and transport stay with the host. The tools, their results,
// read-only refusals and audit logging are the same ones the kafka-mcp
// executable serves.
package kafkamcp

import (
	"fmt"
	"slices"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"

	"github.com/denizgursoy/kafka-mcp/internal/domain/config"
	"github.com/denizgursoy/kafka-mcp/internal/domain/kafkaclient"
	"github.com/denizgursoy/kafka-mcp/internal/tools"
)

// Connection is one cluster's clients, lent by the host. None of them is
// closed by the tools except the fresh consumers NewReader returns.
type Connection struct {
	// Name identifies the cluster in tool results, audit records and
	// destination_cluster arguments.
	Name string

	// Kafka is the host's shared client. It is used for producing and
	// metadata, never for consuming.
	Kafka *kgo.Client
	// Admin is optional; one is built on Kafka when nil.
	Admin *kadm.Client
	// NewReader must return a new client consuming topic directly
	// (kgo.ConsumePartitions). Consuming is client-wide state, so sharing
	// Kafka here would disturb concurrent calls.
	NewReader func(topic string) (*kgo.Client, error)
	// ManualProducer is optional and returns a producer honouring
	// Record.Partition. Without it, writes that name a partition fail.
	ManualProducer func() (*kgo.Client, error)
	// SchemaRegistry is optional.
	SchemaRegistry *sr.Client

	// ReadOnly protects the cluster from every server, including writes
	// arriving from another cluster's server as a destination.
	ReadOnly bool

	// Brokers and Principal are reported by server_config, list_clusters and
	// audit records only; they are never used to connect.
	Brokers   []string
	Principal string
}

// Options is what the host decides for one cluster's server.
type Options struct {
	// ReadOnly hides tools that change this cluster and refuses writes to it.
	ReadOnly bool
	// Destinations are the other clusters this server's copy_message and
	// produce_message may write to. Empty means none. A destination whose
	// Connection is ReadOnly still refuses.
	Destinations []string
	// Tools switches individual tools off (false). Unknown names are an error.
	Tools map[string]bool
	// Path is reported by server_config as where the host serves the server.
	Path string
	// OutputDir is where search_messages and transform_messages write
	// output_file, and where produce_message reads input_file.
	OutputDir string
}

// Clusters holds the lent connections. Tools registered for one cluster can
// read the others (compare_clusters, list_clusters) and, when Options allows,
// write to them (copy_message, produce_message).
type Clusters struct {
	clusters map[string]*config.Cluster
	clients  map[string]*kafkaclient.Client
}

// ToolNames returns every tool name Options.Tools accepts.
func ToolNames() []string { return tools.Names() }

// NewClusters wraps the host's connections. Nothing is dialled.
func NewClusters(connections ...Connection) (*Clusters, error) {
	c := &Clusters{
		clusters: make(map[string]*config.Cluster, len(connections)),
		clients:  make(map[string]*kafkaclient.Client, len(connections)),
	}

	for _, conn := range connections {
		if conn.Name == "" {
			return nil, fmt.Errorf("kafkamcp: Connection.Name is required")
		}
		if _, exists := c.clusters[conn.Name]; exists {
			return nil, fmt.Errorf("kafkamcp: cluster %q is given twice", conn.Name)
		}

		// The SASL entry is for reporting only: the host authenticated the
		// client however it chose, and Borrow never builds a mechanism from it.
		cluster := &config.Cluster{
			Name: conn.Name, Brokers: conn.Brokers, ReadOnly: conn.ReadOnly,
			SASL: []*config.SASL{{Mechanism: "host-managed", User: conn.Principal}},
		}

		client, err := kafkaclient.Borrow(cluster, kafkaclient.Borrowed{
			Kafka:          conn.Kafka,
			Admin:          conn.Admin,
			NewReader:      conn.NewReader,
			ManualProducer: conn.ManualProducer,
			SchemaRegistry: conn.SchemaRegistry,
		})
		if err != nil {
			return nil, fmt.Errorf("kafkamcp: %w", err)
		}

		c.clusters[conn.Name] = cluster
		c.clients[conn.Name] = client
	}

	return c, nil
}

// Register adds the tools for the named cluster to server.
func (c *Clusters) Register(server *mcp.Server, name string, opts Options) error {
	if server == nil {
		return fmt.Errorf("kafkamcp: an MCP server is required")
	}
	cluster := c.clusters[name]
	if cluster == nil {
		return fmt.Errorf("kafkamcp: unknown cluster %q", name)
	}

	destinations := slices.Clone(opts.Destinations)
	slices.Sort(destinations)
	for _, destination := range destinations {
		if c.clusters[destination] == nil {
			return fmt.Errorf("kafkamcp: cluster %q lists unknown destination %q", name, destination)
		}
	}

	cfg := &config.Config{
		OutputDir: opts.OutputDir,
		Clusters:  c.clusters,
		Endpoints: make(map[string]*config.Endpoint, len(c.clusters)),
	}

	// The registry only lets cross-cluster tools reach clusters some endpoint
	// serves. Every lent cluster gets one here; their policy is not used,
	// because writes are decided by this endpoint's destinations and the
	// destination cluster's own read_only.
	for other := range c.clusters {
		cfg.Endpoints[other] = &config.Endpoint{Name: other, Cluster: other, ReadOnly: true}
	}
	cfg.Endpoints[name] = &config.Endpoint{
		Name:         name,
		Cluster:      name,
		Path:         opts.Path,
		ReadOnly:     opts.ReadOnly || cluster.ReadOnly,
		Tools:        opts.Tools,
		Destinations: destinations,
	}

	return tools.Register(server, cfg, kafkaclient.BorrowRegistry(cfg, c.clients), name)
}
