package tools

import (
	"encoding/json"
	"sort"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/suite"

	"github.com/denizgursoy/kafka-mcp/internal/domain/config"
	"github.com/denizgursoy/kafka-mcp/internal/domain/kafkaclient"
	"github.com/denizgursoy/kafka-mcp/internal/tools/serverconfig"
)

// RegistrationSuite covers which tools each cluster's endpoint exposes.
//
// This suite starts no broker, unlike the tool suites. Listing tools and
// reading back the server's own configuration are answered entirely from the
// registration wiring, so requiring Docker here would only mean the wiring
// went unchecked in `go test -short ./...` — the run most likely to catch a
// tool registered against the wrong cluster.
type RegistrationSuite struct {
	suite.Suite

	cfg      *config.Config
	clusters *kafkaclient.Registry
}

func TestRegistrationSuite(t *testing.T) {
	suite.Run(t, new(RegistrationSuite))
}

func (s *RegistrationSuite) SetupSuite() {
	s.cfg = &config.Config{
		OutputDir: s.T().TempDir(),
		Clusters: map[string]*config.Cluster{
			"readonly": {
				Name:     "readonly",
				Brokers:  []string{"127.0.0.1:19092"},
				ReadOnly: true,
			},
			"writable": {
				Name:    "writable",
				Brokers: []string{"127.0.0.1:19092"},
			},
			"limited": {
				Name:    "limited",
				Brokers: []string{"127.0.0.1:19092"},
				Tools: map[string]bool{
					"create_topic": false,
					"list_topics":  false,
					"get_message":  true,
				},
			},
			"typo": {
				Name:    "typo",
				Brokers: []string{"127.0.0.1:19092"},
				Tools:   map[string]bool{"create_topics": false},
			},
			"silent": {
				Name:    "silent",
				Brokers: []string{"127.0.0.1:19092"},
				Tools:   map[string]bool{"server_config": false},
			},
		},
		Endpoints: map[string]*config.Endpoint{
			"readonly": {
				Name: "readonly", Cluster: "readonly", Path: "/mcp/readonly", ReadOnly: true,
			},
			"writable": {
				Name: "writable", Cluster: "writable", Path: "/mcp/writable",
			},
			"limited": {
				Name: "limited", Cluster: "limited", Path: "/mcp/limited",
				Tools: map[string]bool{
					"create_topic": false,
					"list_topics":  false,
					"get_message":  true,
				},
			},
			"typo": {
				Name: "typo", Cluster: "typo", Path: "/mcp/typo",
				Tools: map[string]bool{"create_topics": false},
			},
			"silent": {
				Name: "silent", Cluster: "silent", Path: "/mcp/silent",
				Tools: map[string]bool{"server_config": false},
			},
		},
	}

	clusters, err := kafkaclient.NewRegistry(s.cfg)
	s.Require().NoError(err,
		"the registry must build from a plain config, because every endpoint is served from it")

	s.clusters = clusters
}

func (s *RegistrationSuite) TearDownSuite() {
	s.clusters.Close()
}

// exposed returns the tools the cluster's endpoint lists over the protocol,
// and the tools server_config claims it exposes. They are read from one
// session so the two answers describe the same server.
func (s *RegistrationSuite) exposed(cluster string) (listed, reported []string) {
	ctx := s.T().Context()

	serverTransport, clientTransport := mcp.NewInMemoryTransports()

	// Registered the way cmd/server registers it, so the test asserts on what
	// an endpoint actually offers rather than on a list assembled for the
	// test. The implementation name and version are main's business and bear
	// on nothing here.
	server := mcp.NewServer(&mcp.Implementation{Name: "kafka-mcp-" + cluster, Version: "test"}, nil)
	s.Require().NoError(Register(server, s.cfg, s.clusters, cluster),
		"registration must accept a valid configuration for cluster %q, or the endpoint serves nothing", cluster)

	serverSession, err := server.Connect(ctx, serverTransport, nil)
	s.Require().NoError(err,
		"the MCP server for cluster %q must accept a connection, or no tool of it is reachable", cluster)

	defer serverSession.Close()

	clientSession, err := mcp.NewClient(
		&mcp.Implementation{Name: "registration-test", Version: "1"},
		nil,
	).Connect(ctx, clientTransport, nil)
	s.Require().NoError(err,
		"the client must complete the initialize handshake before any tool can be listed")

	defer clientSession.Close()

	result, err := clientSession.ListTools(ctx, nil)
	s.Require().NoError(err,
		"tools/list must succeed, because it is how a client discovers what the endpoint allows")

	listed = make([]string, 0, len(result.Tools))
	for _, tool := range result.Tools {
		listed = append(listed, tool.Name)
	}

	sort.Strings(listed)

	call, err := clientSession.CallTool(ctx, &mcp.CallToolParams{Name: "server_config"})
	s.Require().NoError(err,
		"server_config must be callable on every endpoint, because it is what tells a session whether the cluster is writable")
	s.Require().False(call.IsError,
		"server_config must not fail: it reads the server's own configuration and touches no broker")

	encoded, err := json.Marshal(call.StructuredContent)
	s.Require().NoError(err,
		"the structured result must be JSON, because that is what an MCP client receives")

	var out serverconfig.Output
	s.Require().NoError(json.Unmarshal(encoded, &out),
		"the structured result must decode into the tool's own Output type")

	return listed, out.Tools
}

func (s *RegistrationSuite) TestExposedTools() {
	s.Run("a read-only cluster does not expose the tools whose only purpose is to change it", func() {
		listed, _ := s.exposed("readonly")

		s.Require().NotContains(listed, "add_partitions",
			"add_partitions can only ever refuse on a read-only cluster, and offering its preview advertises a capability the server does not have")
		s.Require().NotContains(listed, "commit_offset",
			"commit_offset can only ever refuse on a read-only cluster, so listing it wastes a call and misleads the caller about what is possible")
	})

	s.Run("a read-only cluster still exposes every tool that only reads", func() {
		listed, _ := s.exposed("readonly")

		s.Require().Contains(listed, "list_topics",
			"read-only is the reason this server exists for a protected cluster, so reading must be untouched")
		s.Require().Contains(listed, "search_messages",
			"searching writes only to output_dir, never to the cluster, so read_only does not bear on it")
		s.Require().Contains(listed, "server_config",
			"server_config is how a session learns the cluster is read-only, so hiding it would remove the explanation")
	})

	s.Run("a read-only cluster still exposes copy_message", func() {
		listed, _ := s.exposed("readonly")

		s.Require().Contains(listed, "copy_message",
			"read_only protects the cluster being written to, not the one being read from: rescuing a message out of a read-only production cluster is the case this tool exists for")
	})

	s.Run("a writable cluster exposes the tools that change it", func() {
		listed, _ := s.exposed("writable")

		s.Require().Contains(listed, "add_partitions",
			"a writable cluster must keep every tool, or gating by read_only would have disabled the server instead of protecting a cluster")
		s.Require().Contains(listed, "commit_offset",
			"a writable cluster must keep every tool, or gating by read_only would have disabled the server instead of protecting a cluster")
	})

	s.Run("server_config reports exactly what a read-only endpoint exposes", func() {
		listed, reported := s.exposed("readonly")

		s.Require().Equal(listed, reported,
			"server_config is the only way a session can read back the tool list, so a hardcoded list that outlives a conditional registration would make it lie about a protected cluster")
	})

	s.Run("server_config reports exactly what a writable endpoint exposes", func() {
		listed, reported := s.exposed("writable")

		s.Require().Equal(listed, reported,
			"the reported list must match the registrations on every endpoint, not only on the one that drops tools")
	})

	s.Run("a writable cluster with nothing disabled exposes every tool this server has", func() {
		listed, _ := s.exposed("writable")

		expected := Names()
		sort.Strings(expected)

		s.Require().Equal(expected, listed,
			"Names is what a cluster's tools configuration is checked against, so a tool missing from it could never be switched off, and a name in it that nothing registers would look like a switch that does nothing")
	})
}

func (s *RegistrationSuite) TestDisabledTools() {
	s.Run("a disabled tool is not exposed", func() {
		listed, _ := s.exposed("limited")

		s.Require().NotContains(listed, "create_topic",
			"a tool switched off in the cluster's configuration must not be registered, which is the whole point of the setting")
		s.Require().NotContains(listed, "list_topics",
			"reading is not exempt: the configuration withholds whatever it names, including tools read_only would have allowed")
	})

	s.Run("server_config agrees about what was withheld", func() {
		listed, reported := s.exposed("limited")

		s.Require().Equal(listed, reported,
			"a disabled tool must disappear from both the protocol listing and server_config, or a caller would be told about a tool it can never call")
	})

	s.Run("tools left alone or set to true are untouched", func() {
		listed, _ := s.exposed("limited")

		s.Require().Contains(listed, "get_message",
			"an explicit true must keep the tool, since the map is a list of exceptions rather than an allow-list")
		s.Require().Contains(listed, "add_partitions",
			"a writable cluster that disabled other tools must keep the ones it did not name, or one switch would quietly withhold the rest")
		s.Require().Contains(listed, "server_config",
			"server_config is how the caller discovers what is left, so it survives every other tool being withheld")
	})
}

func (s *RegistrationSuite) TestRejectsAnUnusableToolConfiguration() {
	s.Run("an unknown tool name stops the server", func() {
		server := mcp.NewServer(&mcp.Implementation{Name: "kafka-mcp-typo", Version: "test"}, nil)

		err := Register(server, s.cfg, s.clusters, "typo")

		s.Require().Error(err,
			"a misspelled tool name switches nothing off, so the tool it was meant to withhold would stay exposed: that must fail at startup rather than at the call that discovers it")
		s.Require().Contains(err.Error(), "create_topics",
			"the error must name the key from the file, so the operator can find and fix it")
	})

	s.Run("disabling server_config stops the server", func() {
		server := mcp.NewServer(&mcp.Implementation{Name: "kafka-mcp-silent", Version: "test"}, nil)

		err := Register(server, s.cfg, s.clusters, "silent")

		s.Require().Error(err,
			"without server_config a session cannot tell a withheld tool from a missing feature, or learn the cluster is read-only, so the configuration is refused rather than honoured")
		s.Require().Contains(err.Error(), "server_config",
			"the error must name the tool it refuses to withhold")
	})
}
