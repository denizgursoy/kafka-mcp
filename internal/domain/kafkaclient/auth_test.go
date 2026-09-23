package kafkaclient_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/suite"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/denizgursoy/kafka-mcp/internal/domain/config"
	"github.com/denizgursoy/kafka-mcp/internal/domain/kafkaclient"
	"github.com/denizgursoy/kafka-mcp/internal/domain/records"
	"github.com/denizgursoy/kafka-mcp/internal/domain/testenv"
	"github.com/denizgursoy/kafka-mcp/internal/tools/getmessage"
	"github.com/denizgursoy/kafka-mcp/internal/tools/serverconfig"
)

type AuthenticationSuite struct {
	suite.Suite
	env *testenv.Environment
}

func TestAuthenticationSuite(t *testing.T) { suite.Run(t, new(AuthenticationSuite)) }

func (s *AuthenticationSuite) SetupSuite()    { s.env = testenv.StartSASL(s.T()) }
func (s *AuthenticationSuite) TearDownSuite() { s.env.Stop() }

func (s *AuthenticationSuite) TestAuthenticatedConnections() {
	topic := s.env.CreateTopic(s.T(), "authenticated-reader")
	s.env.Produce(s.T(), topic, testenv.Message{Value: "protected-message"})
	cfg := &config.Cluster{
		Name: "secured", Brokers: []string{s.env.Broker()},
		SASL: []*config.SASL{{Mechanism: config.MechanismScramSHA256, User: testenv.SASLUser, Password: testenv.SASLPassword}},
	}
	client, err := kafkaclient.New(cfg)
	s.Require().NoError(err, "valid SCRAM settings must build a client")
	s.T().Cleanup(client.Close)

	s.Run("admin authenticates", func() {
		ctx, cancel := context.WithTimeout(s.T().Context(), 10*time.Second)
		defer cancel()
		s.Require().NoError(client.Ping(ctx), "the main client must authenticate to the protected broker")
	})
	s.Run("testenv cluster client preserves authentication", func() {
		ctx, cancel := context.WithTimeout(s.T().Context(), 10*time.Second)
		defer cancel()

		testClient := s.env.ClusterClient(s.T(), false)

		s.Require().NoError(testClient.Ping(ctx),
			"the shared production-shaped test client must carry the environment's SCRAM credentials rather than connecting anonymously")
	})
	s.Run("reader authenticates independently", func() {
		session, err := client.Reader().Session(topic)
		s.Require().NoError(err, "the independent reading session must build")
		defer session.Close()
		ctx, cancel := context.WithTimeout(s.T().Context(), 10*time.Second)
		defer cancel()
		var values []string
		err = session.Scan(ctx, []records.Range{{Partition: 0, Start: 0, End: 1}}, func(record *kgo.Record) bool {
			values = append(values, string(record.Value))
			return true
		})
		s.Require().NoError(err, "reader connections must inherit SASL rather than connecting anonymously")
		s.Require().Equal([]string{"protected-message"}, values, "successful admin authentication alone does not prove messages are readable")
	})
	s.Run("wrong password is refused", func() {
		bad, err := kafkaclient.New(&config.Cluster{Brokers: cfg.Brokers,
			SASL: []*config.SASL{{Mechanism: config.MechanismScramSHA256, User: testenv.SASLUser, Password: "wrong"}}})
		s.Require().NoError(err, "credentials are checked by the broker on connection")
		defer bad.Close()
		ctx, cancel := context.WithTimeout(s.T().Context(), 5*time.Second)
		defer cancel()
		s.Require().Error(bad.Ping(ctx), "the broker must refuse invalid credentials rather than falling back to anonymous access")
	})
	s.Run("MCP reads authenticated records", func() {
		server := mcp.NewServer(&mcp.Implementation{Name: "auth-test", Version: "1"}, nil)
		getmessage.Register(server, client.Reader())
		serverconfig.Register(server, client, nil, nil, []string{"get_message", "server_config"})
		httpServer := httptest.NewServer(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, nil))
		defer httpServer.Close()
		ctx, cancel := context.WithTimeout(s.T().Context(), 10*time.Second)
		defer cancel()
		caller := mcp.NewClient(&mcp.Implementation{Name: "test-caller", Version: "1"}, nil)
		session, err := caller.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: httpServer.URL}, nil)
		s.Require().NoError(err, "MCP initialization must succeed over real HTTP")
		defer session.Close()
		result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "get_message", Arguments: map[string]any{"topic": topic, "partition": 0, "offset": 0}})
		s.Require().NoError(err, "the MCP session must call the registered reading tool")
		s.Require().False(result.IsError, "MCP message reading must authenticate its own broker connection")
		data, err := json.Marshal(result)
		s.Require().NoError(err, "the tool result must serialize across the protocol")
		s.Require().Contains(string(data), "protected-message", "the authenticated Kafka message must reach the MCP caller")
		report, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "server_config", Arguments: map[string]any{}})
		s.Require().NoError(err, "the updated configuration schema must work over MCP")
		s.Require().False(report.IsError, "configuration reporting must not fail output schema validation")
		data, err = json.Marshal(report)
		s.Require().NoError(err, "the configuration report must serialize")
		s.Require().Contains(string(data), testenv.SASLUser, "configured identities must remain visible to operators")
		s.Require().NotContains(string(data), testenv.SASLPassword, "credentials must never cross the MCP boundary")
	})
}
