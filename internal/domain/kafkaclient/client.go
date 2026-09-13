// Package kafkaclient owns the Kafka connection the tools are given.
//
// It lives in internal/domain because every tool is handed a piece of it,
// and it is not a tool itself.
package kafkaclient

import (
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/denizgursoy/kafka-mcp/internal/domain/records"
)

// Client owns the Kafka connection shared by every tool package.
type Client struct {
	client *kgo.Client
	admin  *kadm.Client
	reader *records.Reader
}

func New(brokers ...string) (*Client, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
	)
	if err != nil {
		return nil, err
	}

	return &Client{
		client: client,
		admin:  kadm.NewClient(client),
		reader: records.NewReader(brokers...),
	}, nil
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

func (c *Client) Close() {
	c.client.Close()
}
