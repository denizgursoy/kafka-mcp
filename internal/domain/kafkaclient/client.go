// Package kafkaclient owns the Kafka connection the tools are given.
//
// It lives in internal/domain because every tool is handed a piece of it,
// and it is not a tool itself.
package kafkaclient

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/twmb/tlscfg"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/denizgursoy/kafka-mcp/internal/domain/config"
	"github.com/denizgursoy/kafka-mcp/internal/domain/records"
)

// Client owns one cluster's Kafka connection, shared by every tool bound to
// that cluster.
type Client struct {
	client   *kgo.Client
	admin    *kadm.Client
	reader   *records.Reader
	manual   *manualProducer
	cfg      *config.Cluster
	endpoint *config.Endpoint
}

// manualProducer is a second producer that honours Record.Partition.
//
// The shared client uses franz-go's default partitioner, which ignores that
// field and balances records itself. Switching the shared client to
// ManualPartitioner is not an option: an unset Partition is 0 rather than
// "unset", so every record produced without an explicit partition would land
// on partition 0.
//
// It is built on first use and shared by every endpoint view of the cluster,
// so a server whose callers never name a partition opens no extra connection.
type manualProducer struct {
	options []kgo.Opt

	once   sync.Once
	client *kgo.Client
	err    error
}

func (m *manualProducer) get() (*kgo.Client, error) {
	m.once.Do(func() {
		m.client, m.err = kgo.NewClient(
			append(m.options, kgo.RecordPartitioner(kgo.ManualPartitioner()))...)
	})

	if m.err != nil {
		return nil, fmt.Errorf("connect a manual-partition producer: %w", m.err)
	}

	return m.client, nil
}

func (m *manualProducer) close() {
	if m.client != nil {
		m.client.Close()
	}
}

// ForEndpoint returns an endpoint-scoped view over the same Kafka connection.
// Closing remains the registry's responsibility; the view only adds policy.
func (c *Client) ForEndpoint(endpoint *config.Endpoint) *Client {
	if c == nil {
		return nil
	}

	return &Client{
		client:   c.client,
		admin:    c.admin,
		reader:   c.reader,
		manual:   c.manual,
		cfg:      c.cfg,
		endpoint: endpoint,
	}
}

// Endpoint returns the policy applied to this view, or nil for a direct
// cluster client retained for backwards-compatible tests and integrations.
func (c *Client) Endpoint() *config.Endpoint {
	return c.endpoint
}

// New connects to the cluster described by cfg.
func New(cfg *config.Cluster) (*Client, error) {
	options := []kgo.Opt{kgo.SeedBrokers(cfg.Brokers...)}

	if cfg.TLS != nil && cfg.TLS.Enabled {
		tlsConfig, err := tlsConfig(cfg.TLS)
		if err != nil {
			return nil, err
		}

		options = append(options, kgo.DialTLSConfig(tlsConfig))
	}

	mechanisms := make([]sasl.Mechanism, 0, len(cfg.SASL))
	for _, auth := range cfg.SASL {
		mechanism, err := saslMechanism(auth)
		if err != nil {
			return nil, err
		}

		mechanisms = append(mechanisms, mechanism)
	}
	if len(mechanisms) > 0 {
		options = append(options, kgo.SASL(mechanisms...))
	}

	client, err := kgo.NewClient(options...)
	if err != nil {
		return nil, fmt.Errorf("connect to %v: %w", cfg.Brokers, err)
	}

	return &Client{
		client: client,
		admin:  kadm.NewClient(client),
		reader: records.NewReaderWithOptions(options...),
		manual: &manualProducer{options: options},
		cfg:    cfg,
	}, nil
}

// saslMechanism builds the authentication the server connects with. This is
// the identity Kafka ACLs are enforced against, which is how two people
// running the same server can have different permissions.
func saslMechanism(auth *config.SASL) (sasl.Mechanism, error) {
	switch auth.Mechanism {
	case config.MechanismOAuth:
		if auth.OAuth == nil {
			return nil, fmt.Errorf("oauth settings are required")
		}
		return oauthMechanism(auth.OAuth)
	case config.MechanismPlain:
		return plain.Auth{
			Zid:  auth.Zid,
			User: auth.User,
			Pass: auth.Password,
		}.AsMechanism(), nil

	case config.MechanismScramSHA256:
		return scram.Auth{
			Zid:     auth.Zid,
			IsToken: auth.IsToken,
			User:    auth.User,
			Pass:    auth.Password,
		}.AsSha256Mechanism(), nil

	case config.MechanismScramSHA512:
		return scram.Auth{
			Zid:     auth.Zid,
			IsToken: auth.IsToken,
			User:    auth.User,
			Pass:    auth.Password,
		}.AsSha512Mechanism(), nil
	}

	return nil, fmt.Errorf("unsupported sasl mechanism %q", auth.Mechanism)
}

func tlsConfig(settings *config.TLS) (*tls.Config, error) {
	cfg, err := tlscfg.New(
		tlscfg.MaybeWithDiskKeyPair(settings.CertFile, settings.KeyFile),
		tlscfg.MaybeWithDiskCA(settings.CAFile, tlscfg.ForClient),
		tlscfg.WithSystemCertPool(),
	)
	if err != nil {
		return nil, fmt.Errorf("configure tls: %w", err)
	}

	return cfg, nil
}

// Config returns the cluster this client was built from.
func (c *Client) Config() *config.Cluster {
	return c.cfg
}

// Name returns the cluster's name, which is also the endpoint path it is
// served on.
func (c *Client) Name() string {
	return c.cfg.Name
}

// RequireWritable reports whether the server is allowed to change the
// cluster, and is the single gate every mutating tool calls.
//
// This protects clusters that have no ACLs of their own. It is not a security
// boundary: whoever can edit the config file can turn it off. Where real
// enforcement is needed, it belongs in Kafka ACLs against the SASL principal.
func (c *Client) RequireWritable(operation string) error {
	readOnly := c.cfg.ReadOnly
	where := fmt.Sprintf("cluster %q", c.cfg.Name)
	if c.endpoint != nil {
		readOnly = c.endpoint.ReadOnly
		where = fmt.Sprintf("endpoint %q for cluster %q", c.endpoint.Name, c.cfg.Name)
	}
	if !readOnly {
		return nil
	}

	return fmt.Errorf(
		"%s is read-only: it is configured with read_only, so %s cannot change it",
		where, operation)
}

// Admin returns the admin client used by tools that need metadata operations.
func (c *Client) Admin() *kadm.Client {
	return c.admin
}

// Kafka returns the underlying record-level client.
func (c *Client) Kafka() *kgo.Client {
	return c.client
}

// ManualProducer returns a producer that writes a record to the partition set
// on it, for a caller who chose the partition deliberately.
//
// This is a separate connection from Kafka() on purpose. The shared client
// partitions by key, which is what almost every write wants; honouring
// Record.Partition there would send every record with no explicit partition to
// partition 0, because an unset field is indistinguishable from a deliberate
// zero.
func (c *Client) ManualProducer() (*kgo.Client, error) {
	if c.manual == nil {
		return nil, fmt.Errorf("cluster %q has no manual-partition producer", c.cfg.Name)
	}

	return c.manual.get()
}

// Reader returns the reader used by tools that read message content.
func (c *Client) Reader() *records.Reader {
	return c.reader
}

// Ping confirms the brokers answer, so a misconfigured connection fails at
// startup rather than on the first tool call.
func (c *Client) Ping(ctx context.Context) error {
	if err := c.client.Ping(ctx); err != nil {
		return fmt.Errorf("reach brokers %v: %w", c.cfg.Brokers, err)
	}

	return nil
}

func (c *Client) Close() {
	if c.manual != nil {
		c.manual.close()
	}

	c.client.Close()
}
