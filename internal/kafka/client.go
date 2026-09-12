package kafka

import (
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Client owns the Kafka connection shared by every tool package.
type Client struct {
	client *kgo.Client
	admin  *kadm.Client
}

func NewClient(brokers ...string) (*Client, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
	)
	if err != nil {
		return nil, err
	}

	return &Client{
		client: client,
		admin:  kadm.NewClient(client),
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

func (c *Client) Close() {
	c.client.Close()
}
