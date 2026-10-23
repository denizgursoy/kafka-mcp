// Package kafkamcp registers kafka-mcp's tools on an MCP server owned by
// another Go application, using Kafka clients that application already has.
//
// It opens no listener, loads no configuration and dials nothing: routing,
// authentication and transport stay with the host. The tools, their results,
// read-only refusals and audit logging are the same ones the kafka-mcp
// executable serves.
package kafkamcp

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/denizgursoy/kafka-mcp/internal/domain/config"
	"github.com/denizgursoy/kafka-mcp/internal/domain/kafkaclient"
	"github.com/denizgursoy/kafka-mcp/internal/domain/transfer"
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
	// Kafka here would disturb concurrent calls. The tools close it.
	NewReader func(topic string) (*kgo.Client, error)
	// AcquireReader replaces NewReader when set: it lends a client for
	// topic and the release function the tools call once they are done
	// with it, instead of closing it. It may wait for a free slot until ctx
	// ends, so the host can bound how many readers MCP keeps open. A lent
	// client should keep control records (kgo.KeepControlRecords); they are
	// skipped, and without them a scan ending on a transaction marker waits
	// for data that never comes.
	AcquireReader func(ctx context.Context, topic string) (client *kgo.Client, release func(), err error)
	// ManualProducer is optional and returns a producer honouring
	// Record.Partition. Without it, writes that name a partition fail.
	ManualProducer func() (*kgo.Client, error)
	// BulkProducer is optional and returns the producer for batch writes
	// (produce_message upload, transform_messages), with manual
	// partitioning when manual is set. Without it they use Kafka and
	// ManualProducer, where single writes queue behind their batches.
	BulkProducer func(manual bool) (*kgo.Client, error)
	// SchemaRegistry is optional.
	SchemaRegistry *sr.Client
	// ProtoMessage is optional and names the protobuf message the key or
	// value of a topic holds when it is written without Schema Registry
	// framing; nil for none. It is consulted after registry framing.
	ProtoMessage func(topic string, key bool) protoreflect.MessageDescriptor

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
	// Files hands out the links search_messages and transform_messages
	// download through and produce_message uploads through. Nil leaves
	// those modes refusing, as over stdio. Create it with NewFiles and serve
	// its Handler.
	Files *Files
	// Logger receives the audit records of tool calls. Nil means
	// slog.Default.
	Logger *slog.Logger
	// User names the person behind a call from its HTTP request headers,
	// for audit records, as established by something the host trusts (an
	// authenticating proxy). Nil or an empty name records none.
	User func(header http.Header) string
}

// Files is where one endpoint's download and upload links are served. Files
// pass straight between the caller and Kafka and are never stored. A link
// carries the request it runs, sealed with a key, so any instance sharing
// the key can serve a link another made.
type Files struct{ hub *transfer.Hub }

// FilesPath is where an endpoint at path serves its links, e.g. /mcp/prod
// serves them below /mcp/prod/files.
func FilesPath(path string) string { return strings.TrimRight(path, "/") + tools.FilesPath }

// NewFiles returns the links for an endpoint served at path, the path the
// host routes, including any base path. Every instance behind one address
// must pass the same key; an empty one makes a random key, so links open
// only on this process.
func NewFiles(path string, key []byte) (*Files, error) {
	hub, err := transfer.NewHub(FilesPath(path), key)
	if err != nil {
		return nil, err
	}

	return &Files{hub: hub}, nil
}

// NewLocalFiles saves downloads in dir and reads uploads from it, for a host
// running on the user's own machine, such as over stdio. It serves no links.
func NewLocalFiles(dir string) *Files { return &Files{hub: transfer.NewLocalHub(dir)} }

// Path is where Handler must be mounted, with everything below it.
func (f *Files) Path() string { return f.hub.Path() }

// Handler serves the links. Mount it at Path() and below, behind the same
// authentication and origin checks as the MCP endpoint, but without the
// bearer token: a link is sealed and expires; the agent's command fetching
// it does not carry the MCP
// client's headers.
func (f *Files) Handler() http.Handler { return f.hub.Handler() }

// Middleware must wrap the MCP endpoint's handler: it records the address
// each call arrived at, which links are built on. publicURL, when set, is
// used instead, for a host behind a proxy that changes the address.
func Middleware(publicURL string) func(http.Handler) http.Handler {
	return transfer.Middleware(publicURL)
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

		borrowed := kafkaclient.Borrowed{
			Kafka:          conn.Kafka,
			Admin:          conn.Admin,
			NewReader:      conn.NewReader,
			ManualProducer: conn.ManualProducer,
			BulkProducer:   conn.BulkProducer,
			SchemaRegistry: conn.SchemaRegistry,
		}
		if conn.AcquireReader != nil {
			borrowed.AcquireReader = conn.AcquireReader
		}
		if conn.ProtoMessage != nil {
			borrowed.ProtoMessage = conn.ProtoMessage
		}

		client, err := kafkaclient.Borrow(cluster, borrowed)
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

	auditing := tools.Audit{Logger: opts.Logger}
	if opts.User != nil {
		auditing.User = opts.User
	}

	var files *transfer.Hub
	if opts.Files != nil {
		files = opts.Files.hub
	}

	return tools.Register(server, cfg, kafkaclient.BorrowRegistry(cfg, c.clients), name, auditing, files)
}
