package kafkamcp_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/suite"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"

	"github.com/denizgursoy/kafka-mcp/internal/domain/testenv"
	"github.com/denizgursoy/kafka-mcp/pkg/kafkamcp"
)

type RegisterSuite struct {
	suite.Suite
	env *testenv.Environment
}

func TestRegisterSuite(t *testing.T) { suite.Run(t, new(RegisterSuite)) }

func (s *RegisterSuite) SetupSuite()    { s.env = testenv.Start(s.T()) }
func (s *RegisterSuite) TearDownSuite() { s.env.Stop() }

// connect registers the tools on a fresh server, serves it over the SDK's own
// HTTP handler the way a host would, and returns a connected client session.
func (s *RegisterSuite) connect(conn kafkamcp.Connection, opts kafkamcp.Options) *mcp.ClientSession {
	clusters, err := kafkamcp.NewClusters(conn)
	s.Require().NoError(err, "the host's connection must be accepted")

	return s.serve(clusters, conn.Name, opts)
}

func (s *RegisterSuite) serve(clusters *kafkamcp.Clusters, name string, opts kafkamcp.Options) *mcp.ClientSession {
	server := mcp.NewServer(&mcp.Implementation{Name: "host", Version: "1"}, nil)
	s.Require().NoError(clusters.Register(server, name, opts), "registering on the host's server must succeed")

	host := httptest.NewServer(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, nil))
	s.T().Cleanup(host.Close)

	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil)
	session, err := client.Connect(s.T().Context(), &mcp.StreamableClientTransport{Endpoint: host.URL}, nil)
	s.Require().NoError(err, "an MCP client must reach the host's server")
	s.T().Cleanup(func() { _ = session.Close() })

	return session
}

// lend returns a connection to the test broker under name, built the way a
// host would build its own.
func (s *RegisterSuite) lend(name string, readOnly bool) kafkamcp.Connection {
	return kafkamcp.Connection{
		Name: name, Kafka: s.env.Kafka(), ReadOnly: readOnly,
		NewReader: func(topic string) (*kgo.Client, error) {
			return kgo.NewClient(kgo.SeedBrokers(s.env.Broker()),
				kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {}}))
		},
		ManualProducer: func() (*kgo.Client, error) { return s.env.Kafka(), nil },
	}
}

func (s *RegisterSuite) call(session *mcp.ClientSession, name string, args map[string]any) string {
	out, err := session.CallTool(s.T().Context(), &mcp.CallToolParams{Name: name, Arguments: args})
	s.Require().NoError(err, "%s must be callable", name)
	encoded, err := json.Marshal(out.StructuredContent)
	s.Require().NoError(err, "%s must return structured content", name)

	return string(encoded)
}

func (s *RegisterSuite) toolNames(session *mcp.ClientSession) map[string]bool {
	listed, err := session.ListTools(s.T().Context(), nil)
	s.Require().NoError(err, "tools must be listable")
	names := map[string]bool{}
	for _, tool := range listed.Tools {
		names[tool.Name] = true
	}

	return names
}

func (s *RegisterSuite) TestUsesTheHostsClientsWithoutOwningThem() {
	topic := s.env.CreateTopic(s.T(), "borrowed")
	offsets := s.env.Produce(s.T(), topic, testenv.Message{Key: "shared", Value: `{"status":"BORROWED"}`})
	registry, err := sr.NewClient(sr.URLs(s.env.SchemaRegistry()))
	s.Require().NoError(err, "the host builds its own registry client")
	schemaID := s.env.RegisterSchema(s.T(), topic+"-value", sr.Schema{Type: sr.TypeJSON,
		Schema: `{"type":"object","properties":{"status":{"type":"string"}}}`})

	var readers, manual atomic.Int32
	session := s.connect(kafkamcp.Connection{
		Name:  "local",
		Kafka: s.env.Kafka(), Admin: s.env.Admin(), SchemaRegistry: registry,
		NewReader: func(topic string) (*kgo.Client, error) {
			readers.Add(1)
			return kgo.NewClient(kgo.SeedBrokers(s.env.Broker()),
				kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {}}))
		},
		ManualProducer: func() (*kgo.Client, error) {
			manual.Add(1)
			return s.env.Kafka(), nil
		},
		Principal: "console-user",
	}, kafkamcp.Options{Path: "/mcp/local"})

	read := s.call(session, "get_message", map[string]any{
		"items": []map[string]any{{"topic": topic, "partition": 0, "offset": offsets[0]}}})
	s.Require().Contains(read, "BORROWED", "reads must go through the host's reader factory to the real broker")
	s.Require().Equal(int32(1), readers.Load(), "one read must use one fresh consumer from the host")

	schema := s.call(session, "get_schema", map[string]any{"items": []map[string]any{{"subject": topic + "-value"}}})
	s.Require().Contains(schema, `"succeeded":1`, "schema lookups must use the host's registry client")

	write := s.call(session, "produce_message", map[string]any{"confirm": true, "items": []map[string]any{{
		"topic": topic, "partition": 0, "value": `{"status":"HOST"}`, "value_schema": map[string]any{"id": schemaID}}}})
	s.Require().Contains(write, `"applied":1`, "a writable registration must produce through the host")
	s.Require().Positive(manual.Load(), "an explicit partition must use the host's manual producer")

	reported := s.call(session, "server_config", map[string]any{})
	s.Require().Contains(reported, `"path":"/mcp/local"`, "server_config must report where the host serves it")
	s.Require().Contains(reported, `"sasl_user":"console-user"`, "the host's principal must be reported")
	s.Require().Contains(reported, `"authentication":"host-managed"`, "a lent client must not be reported as unauthenticated")

	s.Require().NoError(s.env.Kafka().Ping(s.T().Context()), "the host's client must stay open after the tools used it")
}

func (s *RegisterSuite) TestReadOnlyHidesAndRefusesWrites() {
	topic := s.env.CreateTopic(s.T(), "protected")
	conn := s.lend("local", false)

	for name, setup := range map[string]func(*kafkamcp.Connection, *kafkamcp.Options){
		"options":    func(_ *kafkamcp.Connection, o *kafkamcp.Options) { o.ReadOnly = true },
		"connection": func(c *kafkamcp.Connection, _ *kafkamcp.Options) { c.ReadOnly = true },
	} {
		s.Run(name, func() {
			c, opts := conn, kafkamcp.Options{}
			setup(&c, &opts)
			session := s.connect(c, opts)

			names := s.toolNames(session)
			s.Require().False(names["delete_topic"], "a read-only registration must not advertise mutating tools")
			s.Require().True(names["list_topics"], "reading tools stay available")

			refused := s.call(session, "produce_message", map[string]any{"confirm": true,
				"items": []map[string]any{{"topic": topic, "value": "must-not-be-written"}}})
			s.Require().Contains(refused, `"failed":1`, "produce must refuse on a read-only registration")
			s.Require().Contains(refused, "read-only", "the refusal must say why")
		})
	}
}

func (s *RegisterSuite) TestSwitchesToolsOff() {
	session := s.connect(s.lend("local", false), kafkamcp.Options{Tools: map[string]bool{"list_acls": false}})

	names := s.toolNames(session)
	s.Require().False(names["list_acls"], "a tool the host switched off must not be registered")
	s.Require().True(names["server_config"], "server_config is always registered")
}

func (s *RegisterSuite) TestCrossClusterTools() {
	topic := s.env.CreateTopic(s.T(), "cross")
	target := s.env.CreateTopic(s.T(), "cross-target")
	offsets := s.env.Produce(s.T(), topic, testenv.Message{Key: "k", Value: "moved"})

	clusters, err := kafkamcp.NewClusters(s.lend("here", false), s.lend("there", false), s.lend("locked", true))
	s.Require().NoError(err, "several host connections must be accepted together")

	copyTo := func(session *mcp.ClientSession, destination string) string {
		return s.call(session, "copy_message", map[string]any{"confirm": true, "items": []map[string]any{{
			"source_topic": topic, "source_partition": 0, "source_offset": offsets[0],
			"destination_topic": target, "destination_cluster": destination}}})
	}

	s.Run("list_clusters reports every lent cluster", func() {
		listed := s.call(s.serve(clusters, "here", kafkamcp.Options{}), "list_clusters", map[string]any{})
		for _, name := range []string{"here", "there", "locked"} {
			s.Require().Contains(listed, `"`+name+`"`, "list_clusters must name %s", name)
		}
	})

	s.Run("compare_clusters reads the other cluster", func() {
		compared := s.call(s.serve(clusters, "here", kafkamcp.Options{ReadOnly: true}), "compare_clusters",
			map[string]any{"items": []map[string]any{{"cluster": "there"}}})
		s.Require().Contains(compared, `"succeeded":1`, "comparing reads only, so it works from a read-only server")
	})

	s.Run("a listed destination is written", func() {
		session := s.serve(clusters, "here", kafkamcp.Options{ReadOnly: true, Destinations: []string{"there"}})
		s.Require().Contains(copyTo(session, "there"), `"applied":1`,
			"a read-only source may still copy out to a destination the host allowed")
	})

	s.Run("an unlisted destination is refused", func() {
		session := s.serve(clusters, "here", kafkamcp.Options{})
		refused := copyTo(session, "there")
		s.Require().Contains(refused, `"failed":1`, "writing elsewhere needs the host's explicit permission")
		s.Require().Contains(refused, "destinations", "the refusal must say which permission is missing")
	})

	s.Run("a read-only destination is refused even when listed", func() {
		session := s.serve(clusters, "here", kafkamcp.Options{Destinations: []string{"locked"}})
		refused := copyTo(session, "locked")
		s.Require().Contains(refused, `"failed":1`, "the destination cluster's own protection must win")
		s.Require().Contains(refused, "read-only", "the refusal must say why")
	})
}

func (s *RegisterSuite) TestRejectsInvalidRegistrations() {
	server := mcp.NewServer(&mcp.Implementation{Name: "host", Version: "1"}, nil)
	clusters, err := kafkamcp.NewClusters(s.lend("local", false))
	s.Require().NoError(err, "a valid connection must be accepted")

	s.Run("unknown tool name", func() {
		err := clusters.Register(server, "local", kafkamcp.Options{Tools: map[string]bool{"typo": false}})
		s.Require().ErrorContains(err, "unknown tool", "a mistyped switch must not leave the tool exposed")
	})
	s.Run("unknown cluster", func() {
		s.Require().ErrorContains(clusters.Register(server, "nope", kafkamcp.Options{}), "unknown cluster",
			"a server for a cluster the host did not lend must fail")
	})
	s.Run("unknown destination", func() {
		err := clusters.Register(server, "local", kafkamcp.Options{Destinations: []string{"nope"}})
		s.Require().ErrorContains(err, "unknown destination", "a mistyped destination must fail at startup")
	})
	s.Run("missing reader factory", func() {
		c := s.lend("local", false)
		c.NewReader = nil
		_, err := kafkamcp.NewClusters(c)
		s.Require().Error(err, "without a factory reads would share the host's consuming state")
	})
	s.Run("missing or repeated name", func() {
		_, err := kafkamcp.NewClusters(s.lend("", false))
		s.Require().Error(err, "results and audit records need a cluster name")
		_, err = kafkamcp.NewClusters(s.lend("a", false), s.lend("a", false))
		s.Require().ErrorContains(err, "twice", "one name must mean one cluster")
	})
	s.Require().NoError(s.env.Kafka().Ping(s.T().Context()), "a failed registration must not close the host's client")
}
