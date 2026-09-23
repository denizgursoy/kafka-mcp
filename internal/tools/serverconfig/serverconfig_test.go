package serverconfig_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/denizgursoy/kafka-mcp/internal/domain/config"
	"github.com/denizgursoy/kafka-mcp/internal/domain/kafkaclient"
	"github.com/denizgursoy/kafka-mcp/internal/tools/serverconfig"
)

type ServerConfigSuite struct {
	suite.Suite
}

func TestServerConfigSuite(t *testing.T) {
	suite.Run(t, new(ServerConfigSuite))
}

// client builds a client for the given configuration. No broker needs to be
// reachable: reporting the configuration must work even when the cluster is
// down, which is exactly when an operator asks what the server is pointed at.
func (s *ServerConfigSuite) client(cfg *config.Cluster) *kafkaclient.Client {
	s.T().Helper()

	client, err := kafkaclient.New(cfg)
	s.Require().NoError(err, "building a client from a valid configuration must succeed")

	s.T().Cleanup(client.Close)

	return client
}

func (s *ServerConfigSuite) TestReportsTheConnectionAndPermissions() {
	client := s.client(&config.Cluster{
		Name:     "production",
		Brokers:  []string{"kafka-1:9093", "kafka-2:9093"},
		ReadOnly: true,
	})

	out, err := serverconfig.Run(
		client,
		&config.Config{OutputDir: "/var/tmp/exports", HTTP: config.HTTP{Address: ":8080"}},
		&config.Endpoint{Name: "prod-read", Cluster: "production", Path: "/mcp", Description: "Production investigation", ReadOnly: true},
		[]string{"list_topics", "describe_topic"},
	)

	s.Require().NoError(err, "reporting the configuration must succeed")

	s.Run("the brokers are reported", func() {
		s.Require().Equal([]string{"kafka-1:9093", "kafka-2:9093"}, out.Brokers,
			"an operator asking what the server is connected to must get the actual broker list")
	})

	s.Run("the cluster is reported", func() {
		s.Require().Equal("production", out.Cluster,
			"a server may serve several clusters, so a session must be able to confirm which one this endpoint is bound to")
	})

	s.Run("read only is reported", func() {
		s.Require().True(out.ReadOnly,
			"a caller must be able to learn that writes are blocked before attempting one")
	})

	s.Run("the endpoint purpose is reported", func() {
		s.Require().Equal("prod-read", out.Endpoint,
			"two endpoint policies may target one cluster, so the session must identify the policy it reached")
		s.Require().Equal("/mcp", out.Path,
			"the caller must see the exact configured route rather than infer it from a cluster name")
		s.Require().Equal("Production investigation", out.Description,
			"the operator-provided purpose tells an MCP caller when this endpoint should be used")
	})

	s.Run("the registered tools are listed", func() {
		s.Require().Equal([]string{"describe_topic", "list_topics"}, out.Tools,
			"the tool list answers whether a running server has a given capability, and must be sorted for a stable answer")
	})
}

func (s *ServerConfigSuite) TestReportsWhenThereIsNoAuthentication() {
	client := s.client(&config.Cluster{Name: "local", Brokers: []string{"localhost:19092"}})

	out, err := serverconfig.Run(client, nil, nil, nil)

	s.Require().NoError(err, "a cluster without authentication must still be reportable")
	s.Require().Equal("none", out.Authentication,
		"an unauthenticated connection must say so plainly, because it means Kafka ACLs cannot apply to this server")
	s.Require().Empty(out.SASLUser,
		"there is no principal to report when no credentials are configured")
	s.Require().False(out.ReadOnly,
		"writes are allowed unless read_only was set")
}

func (s *ServerConfigSuite) TestReportsTheSASLPrincipal() {
	client := s.client(&config.Cluster{
		Name:    "prod",
		Brokers: []string{"kafka:9093"},
		SASL: []*config.SASL{{
			Mechanism: config.MechanismScramSHA256,
			User:      "kafka-mcp-readonly",
			Password:  "super-secret-value",
		}},
	})

	out, err := serverconfig.Run(client, nil, nil, nil)

	s.Require().NoError(err, "reporting a SASL connection must succeed")
	s.Require().Equal("scram-sha-256", out.Authentication,
		"the mechanism must be reported, since it is part of how the broker identifies this server")
	s.Require().Equal("kafka-mcp-readonly", out.SASLUser,
		"the principal must be reported: it is what Kafka ACLs are enforced against, so it explains why a write was refused")
}

func (s *ServerConfigSuite) TestNeverRevealsThePassword() {
	const secret = "super-secret-value"

	client := s.client(&config.Cluster{
		Name:    "prod",
		Brokers: []string{"kafka:9093"},
		SASL: []*config.SASL{{
			Mechanism: config.MechanismPlain,
			User:      "ali",
			Password:  secret,
		}},
	})

	out, err := serverconfig.Run(client, nil, nil, nil)

	s.Require().NoError(err, "reporting must succeed")

	rendered, err := json.Marshal(out)
	s.Require().NoError(err, "the output must be serialisable, since it is returned over MCP as JSON")

	s.Require().False(strings.Contains(string(rendered), secret),
		"the password must never appear anywhere in the output: this result is sent to an MCP client and may be logged or shown to a model")
}
