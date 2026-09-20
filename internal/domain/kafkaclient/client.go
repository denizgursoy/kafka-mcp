// Package kafkaclient owns the Kafka connection the tools are given.
//
// It lives in internal/domain because every tool is handed a piece of it,
// and it is not a tool itself.
package kafkaclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/denizgursoy/kafka-mcp/internal/domain/config"
	"github.com/denizgursoy/kafka-mcp/internal/domain/records"
)

// Client owns one cluster's Kafka connection, shared by every tool bound to
// that cluster.
type Client struct {
	client *kgo.Client
	admin  *kadm.Client
	reader *records.Reader
	cfg    *config.Cluster
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

	if cfg.SASL != nil {
		mechanism, err := saslMechanism(cfg.SASL)
		if err != nil {
			return nil, err
		}

		options = append(options, kgo.SASL(mechanism))
	}

	client, err := kgo.NewClient(options...)
	if err != nil {
		return nil, fmt.Errorf("connect to %v: %w", cfg.Brokers, err)
	}

	return &Client{
		client: client,
		admin:  kadm.NewClient(client),
		reader: records.NewReader(cfg.Brokers...),
		cfg:    cfg,
	}, nil
}

// saslMechanism builds the authentication the server connects with. This is
// the identity Kafka ACLs are enforced against, which is how two people
// running the same server can have different permissions.
func saslMechanism(auth *config.SASL) (sasl.Mechanism, error) {
	switch auth.Mechanism {
	case config.MechanismPlain:
		return plain.Auth{
			User: auth.User,
			Pass: auth.Password,
		}.AsMechanism(), nil

	case config.MechanismScramSHA256:
		return scram.Auth{
			User: auth.User,
			Pass: auth.Password,
		}.AsSha256Mechanism(), nil

	case config.MechanismScramSHA512:
		return scram.Auth{
			User: auth.User,
			Pass: auth.Password,
		}.AsSha512Mechanism(), nil
	}

	return nil, fmt.Errorf("unsupported sasl mechanism %q", auth.Mechanism)
}

func tlsConfig(settings *config.TLS) (*tls.Config, error) {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}

	if settings.CAFile == "" {
		return tlsConfig, nil
	}

	pem, err := os.ReadFile(settings.CAFile)
	if err != nil {
		return nil, fmt.Errorf("read tls ca_file: %w", err)
	}

	pool := x509.NewCertPool()

	// A CA that does not parse would otherwise leave an empty pool, which
	// fails later as an opaque handshake error.
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("tls ca_file %s holds no usable certificate", settings.CAFile)
	}

	tlsConfig.RootCAs = pool

	return tlsConfig, nil
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
	if !c.cfg.ReadOnly {
		return nil
	}

	where := fmt.Sprintf("cluster %q", c.cfg.Name)

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
	c.client.Close()
}
