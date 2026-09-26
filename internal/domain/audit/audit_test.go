package audit_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/suite"

	"github.com/denizgursoy/kafka-mcp/internal/domain/audit"
	"github.com/denizgursoy/kafka-mcp/internal/domain/config"
)

// AuditSuite starts no broker. What a log line contains is decided entirely by
// the middleware and the request it is handed, so requiring Docker here would
// only keep this out of `go test -short`, the run most likely to catch a field
// that stopped being recorded.
type AuditSuite struct {
	suite.Suite

	records *capture
	logger  *slog.Logger
}

func TestAuditSuite(t *testing.T) {
	suite.Run(t, new(AuditSuite))
}

func (s *AuditSuite) SetupTest() {
	s.records = &capture{}
	s.logger = slog.New(s.records)
}

// capture keeps every record the middleware emits, so a test can assert on the
// attributes rather than on formatted text.
type capture struct {
	entries []entry
}

type entry struct {
	level   slog.Level
	message string
	attrs   map[string]slog.Value
}

func (c *capture) Enabled(context.Context, slog.Level) bool { return true }

func (c *capture) Handle(_ context.Context, record slog.Record) error {
	captured := entry{
		level:   record.Level,
		message: record.Message,
		attrs:   map[string]slog.Value{},
	}

	record.Attrs(func(attr slog.Attr) bool {
		captured.attrs[attr.Key] = attr.Value

		return true
	})

	c.entries = append(c.entries, captured)

	return nil
}

func (c *capture) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c *capture) WithGroup(string) slog.Handler      { return c }

func (c *capture) only() entry { return c.entries[0] }

// endpoint is a writable endpoint bound to one cluster, which is the ordinary
// case every assertion below builds on.
func (s *AuditSuite) endpoint() *config.Endpoint {
	return &config.Endpoint{Name: "prod-write", Cluster: "prod"}
}

// call runs one tool call through the middleware and returns the captured
// records. The handler stands in for the real tool.
func (s *AuditSuite) call(
	endpoint *config.Endpoint,
	principal string,
	name string,
	arguments string,
	result *mcp.CallToolResult,
	handlerErr error,
) []entry {
	s.T().Helper()

	middleware := audit.Middleware(s.logger, endpoint, principal)

	handler := middleware(func(context.Context, string, mcp.Request) (mcp.Result, error) {
		if handlerErr != nil {
			return nil, handlerErr
		}

		return result, nil
	})

	request := &mcp.CallToolRequest{
		Params: &mcp.CallToolParamsRaw{Name: name, Arguments: json.RawMessage(arguments)},
	}

	_, _ = handler(s.T().Context(), "tools/call", request)

	return s.records.entries
}

func (s *AuditSuite) TestRecordsTheCallAndItsTarget() {
	entries := s.call(s.endpoint(), "kafka-mcp-rw", "produce_message",
		`{"confirm":true,"items":[{"topic":"orders","value":"hello"}]}`,
		&mcp.CallToolResult{}, nil)

	s.Require().Len(entries, 1,
		"one tool call must produce exactly one audit record, or the log cannot be counted")

	logged := s.records.only()

	s.Run("the tool and where it ran are recorded", func() {
		s.Require().Equal("produce_message", logged.attrs["tool"].String(),
			"the tool name is the first thing an audit reader looks for")
		s.Require().Equal("prod-write", logged.attrs["endpoint"].String(),
			"one server serves several endpoints, so the endpoint is what says which policy allowed this")
		s.Require().Equal("prod", logged.attrs["cluster"].String(),
			"the cluster is the thing that was actually changed, and is what a reader needs to know")
	})

	s.Run("the Kafka identity is recorded", func() {
		s.Require().Equal("kafka-mcp-rw", logged.attrs["principal"].String(),
			"the principal is the identity Kafka enforced its ACLs against, which is the only identity this server can prove")
	})

	s.Run("the outcome and cost are recorded", func() {
		s.Require().Equal("ok", logged.attrs["outcome"].String(),
			"an audit line that does not say whether the call succeeded cannot answer whether the change happened")
		_, measured := logged.attrs["duration_ms"]
		s.Require().True(measured,
			"duration must be recorded, because a slow tool call is how a stuck investigation looks in a log")
	})
}

func (s *AuditSuite) TestRecordsWhetherTheCallWasConfirmed() {
	s.Run("a confirmed write says so", func() {
		s.call(s.endpoint(), "p", "produce_message",
			`{"confirm":true,"items":[{"topic":"orders","value":"x"}]}`,
			&mcp.CallToolResult{}, nil)

		s.Require().True(s.records.only().attrs["confirm"].Bool(),
			"confirm is what separates a write that happened from a preview, and is the single most important field in a write audit")
	})

	s.Run("a preview says so", func() {
		s.SetupTest()

		s.call(s.endpoint(), "p", "produce_message",
			`{"items":[{"topic":"orders","value":"x"}]}`,
			&mcp.CallToolResult{}, nil)

		s.Require().False(s.records.only().attrs["confirm"].Bool(),
			"a preview changes nothing, so recording it as a write would make the log accuse someone of a change they did not make")
	})
}

func (s *AuditSuite) TestRecordsBatchSizeAndTargets() {
	s.call(s.endpoint(), "p", "commit_offset",
		`{"confirm":true,"items":[
			{"topic":"orders","group":"payments","partition":0,"offset":43},
			{"topic":"events","group":"billing","partition":2,"offset":10}]}`,
		&mcp.CallToolResult{}, nil)

	logged := s.records.only()

	s.Run("the batch size is recorded", func() {
		s.Require().EqualValues(2, logged.attrs["item_count"].Int64(),
			"item_count is the blast radius of one call, so a two-item batch must not read like a single change")
	})

	s.Run("the targets are named", func() {
		targets := logged.attrs["targets"].String()

		s.Require().Contains(targets, "orders",
			"an audit reader needs to know which topic was touched, not merely that some topic was")
		s.Require().Contains(targets, "payments",
			"the group is part of the target for an offset move, since the same topic is consumed by many groups")
		s.Require().Contains(targets, "events",
			"every item's target must appear, or a batch's later changes are invisible")
	})
}

func (s *AuditSuite) TestNeverRecordsMessageContent() {
	// The regression guard that matters most. produce_message and copy_message
	// carry arbitrary payloads, and a log that copies them becomes an
	// uncontrolled duplicate of topic data, including anything personal in it.
	s.call(s.endpoint(), "p", "produce_message",
		`{"confirm":true,"items":[{
			"topic":"orders",
			"key":"customer-4242",
			"value":"{\"card\":\"4111111111111111\"}",
			"headers":{"authorization":"Bearer secret-token"}}]}`,
		&mcp.CallToolResult{}, nil)

	logged := s.records.only()

	all := logged.message
	for key, value := range logged.attrs {
		all += " " + key + "=" + value.String()
	}

	s.Run("the value never appears", func() {
		s.Require().NotContains(all, "4111111111111111",
			"a message value may hold card numbers or personal data, and logging it turns the audit trail into a second copy of the topic")
	})

	s.Run("the key never appears", func() {
		s.Require().NotContains(all, "customer-4242",
			"a key commonly identifies a person, so it is payload rather than a target name")
	})

	s.Run("headers never appear", func() {
		s.Require().NotContains(all, "secret-token",
			"headers carry credentials, and an audit log is usually readable by more people than the data it describes")
	})

	s.Run("the topic is still recorded", func() {
		s.Require().Contains(logged.attrs["targets"].String(), "orders",
			"withholding payload must not cost the target, which is the whole point of the record")
	})
}

func (s *AuditSuite) TestLevelSeparatesWritesFromReads() {
	s.Run("a mutating tool is logged at info", func() {
		s.call(s.endpoint(), "p", "add_partitions",
			`{"confirm":true,"items":[{"topic":"orders","partitions":6}]}`,
			&mcp.CallToolResult{}, nil)

		s.Require().Equal(slog.LevelInfo, s.records.only().level,
			"a change to the cluster must be visible at default level, because it is the thing an audit exists to record")
	})

	s.Run("a read-only tool is logged at debug", func() {
		s.SetupTest()

		s.call(s.endpoint(), "p", "describe_topic",
			`{"items":[{"topic":"orders"}]}`,
			&mcp.CallToolResult{}, nil)

		s.Require().Equal(slog.LevelDebug, s.records.only().level,
			"reads are frequent and change nothing, so logging them at info would bury the writes among them")
	})
}

func (s *AuditSuite) TestDistinguishesRefusalFromProtocolError() {
	s.Run("a tool refusal is recorded as such", func() {
		s.call(s.endpoint(), "p", "create_topic",
			`{"items":[{"topic":"orders"}]}`,
			&mcp.CallToolResult{IsError: true}, nil)

		s.Require().Equal("tool_error", s.records.only().attrs["outcome"].String(),
			"a refused call is a decision the tool made and the cluster is unchanged, which is different from the call failing to run")
	})

	s.Run("a protocol error is recorded as such", func() {
		s.SetupTest()

		s.call(s.endpoint(), "p", "create_topic",
			`{"items":[{"topic":"orders"}]}`,
			nil, errors.New("schema validation failed"))

		s.Require().Equal("error", s.records.only().attrs["outcome"].String(),
			"a call that never reached the tool must not be recorded as the tool refusing, because the two point at different faults")
	})
}

func (s *AuditSuite) TestRecordsTheEndpointPolicy() {
	s.call(&config.Endpoint{Name: "prod-read", Cluster: "prod", ReadOnly: true},
		"p", "describe_topic", `{"items":[{"topic":"orders"}]}`,
		&mcp.CallToolResult{}, nil)

	s.Require().True(s.records.only().attrs["read_only"].Bool(),
		"whether the endpoint could have changed anything is what tells a reader a write was impossible rather than merely absent")
}

func (s *AuditSuite) TestSurvivesUnparseableArguments() {
	entries := s.call(s.endpoint(), "p", "produce_message",
		`{"items":`, &mcp.CallToolResult{}, nil)

	s.Require().Len(entries, 1,
		"a call must still be recorded when its arguments cannot be read: an unparseable call is exactly what an audit should show")

	s.Require().Equal("produce_message", s.records.only().attrs["tool"].String(),
		"the tool name comes from the protocol rather than the arguments, so it must survive a malformed body")
}

func (s *AuditSuite) TestIgnoresMethodsThatAreNotToolCalls() {
	middleware := audit.Middleware(s.logger, s.endpoint(), "p")

	handler := middleware(func(context.Context, string, mcp.Request) (mcp.Result, error) {
		return &mcp.ListToolsResult{}, nil
	})

	_, err := handler(s.T().Context(), "tools/list", &mcp.ListToolsRequest{})
	s.Require().NoError(err, "the middleware must pass other methods through untouched")

	s.Require().Empty(s.records.entries,
		"listing tools changes nothing and happens on every connection, so recording it would drown the calls that matter")
}

func (s *AuditSuite) TestRecordsTheCallingClient() {
	// ClientInfo comes from the session, which a unit test cannot construct, so
	// this asserts the field is absent rather than wrong when there is no
	// session. The end-to-end check covers the populated case.
	s.call(s.endpoint(), "p", "describe_topic",
		`{"items":[{"topic":"orders"}]}`, &mcp.CallToolResult{}, nil)

	_, present := s.records.only().attrs["client"]
	s.Require().False(present,
		"an absent client must be omitted rather than logged as empty, so a reader can tell 'not reported' from 'reported as blank'")
}

func (s *AuditSuite) TestRecordsAnAuthenticatedUserWhenOneExists() {
	// TokenInfo is only populated when an inbound TokenVerifier is installed,
	// which this server does not do today. The field must therefore appear the
	// moment inbound auth exists, and stay absent until then, rather than being
	// logged as an empty promise of per-user attribution.
	middleware := audit.Middleware(s.logger, s.endpoint(), "p")

	handler := middleware(func(context.Context, string, mcp.Request) (mcp.Result, error) {
		return &mcp.CallToolResult{}, nil
	})

	request := &mcp.CallToolRequest{
		Params: &mcp.CallToolParamsRaw{Name: "describe_topic", Arguments: json.RawMessage(`{"items":[]}`)},
		Extra: &mcp.RequestExtra{
			TokenInfo: &auth.TokenInfo{UserID: "deniz@example.com", Scopes: []string{"kafka.write"}},
			Header:    http.Header{"X-Request-Id": []string{"req-77"}},
		},
	}

	_, _ = handler(s.T().Context(), "tools/call", request)

	logged := s.records.only()

	s.Run("the authenticated user is recorded", func() {
		s.Require().Equal("deniz@example.com", logged.attrs["user"].String(),
			"a verified bearer identity is the only per-person attribution available, so it must be recorded when present")
	})

	s.Run("the request id is recorded", func() {
		s.Require().Equal("req-77", logged.attrs["request_id"].String(),
			"the HTTP layer already logs a request id, and carrying it here is what joins an audit line to that access log")
	})
}
