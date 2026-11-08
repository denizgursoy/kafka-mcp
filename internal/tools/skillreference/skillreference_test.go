package skillreference_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/suite"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/denizgursoy/kafka-mcp/internal/domain/testenv"
	"github.com/denizgursoy/kafka-mcp/pkg/kafkamcp"
)

type SkillReferenceSuite struct {
	suite.Suite
	env *testenv.Environment
}

func TestSkillReferenceSuite(t *testing.T) { suite.Run(t, new(SkillReferenceSuite)) }

func (s *SkillReferenceSuite) SetupSuite()    { s.env = testenv.Start(s.T()) }
func (s *SkillReferenceSuite) TearDownSuite() { s.env.Stop() }

// connect exercises the public embedding API over HTTP, including the initialize
// instructions. A skill read must never acquire one of the host's Kafka readers.
func (s *SkillReferenceSuite) connect(disabled bool, versions ...string) *mcp.ClientSession {
	var readers atomic.Int32
	clusters, err := kafkamcp.NewClusters(kafkamcp.Connection{
		Name: "local", Kafka: s.env.Kafka(), Admin: s.env.Admin(), ReadOnly: true,
		NewReader: func(string) (*kgo.Client, error) {
			readers.Add(1)
			return nil, fmt.Errorf("skill reads must not open a Kafka reader")
		},
	})
	s.Require().NoError(err, "the real broker's clients must be accepted by the embedding API")
	server := mcp.NewServer(&mcp.Implementation{Name: "host", Version: "test"},
		&mcp.ServerOptions{Instructions: "Keep the host's instructions.", SupportedProtocolVersions: versions})
	s.Require().NoError(clusters.Register(server, "local", kafkamcp.Options{
		Tools: map[string]bool{"skill_reference": !disabled},
	}), "skill_reference must be a recognised, individually disableable tool on a read-only endpoint")
	host := httptest.NewServer(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, nil))
	s.T().Cleanup(host.Close)
	session, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil).
		Connect(s.T().Context(), &mcp.StreamableClientTransport{Endpoint: host.URL}, nil)
	s.Require().NoError(err, "a remote client must receive the skill bootstrap during initialize")
	s.T().Cleanup(func() {
		_ = session.Close()
		s.Require().Zero(readers.Load(), "reading built-in guidance must not open a Kafka consumer")
	})
	return session
}

type document struct {
	Skill   string   `json:"skill"`
	File    string   `json:"file"`
	Content string   `json:"content"`
	Files   []string `json:"files"`
}

type response struct {
	Results []struct {
		Index  int       `json:"index"`
		Result *document `json:"result"`
		Error  string    `json:"error"`
	} `json:"results"`
	Succeeded int  `json:"succeeded"`
	Failed    int  `json:"failed"`
	Applied   int  `json:"applied"`
	Atomic    bool `json:"atomic"`
}

func (s *SkillReferenceSuite) call(session *mcp.ClientSession, items []map[string]string) response {
	out, err := session.CallTool(s.T().Context(), &mcp.CallToolParams{
		Name: "skill_reference", Arguments: map[string]any{"items": items},
	})
	s.Require().NoError(err, "a valid batch must reach the tool over MCP")
	s.Require().False(out.IsError, "one invalid document must not hide the valid documents in the batch")
	encoded, err := json.Marshal(out.StructuredContent)
	s.Require().NoError(err, "the tool must expose structured batch results to MCP clients")
	var result response
	s.Require().NoError(json.Unmarshal(encoded, &result), "the response must retain the shared batch envelope")
	return result
}

func (s *SkillReferenceSuite) TestDefaultDocumentAndBootstrap() {
	session := s.connect(false)
	instructions := session.InitializeResult().Instructions
	s.Require().Contains(instructions, "Keep the host's instructions.", "adding guidance must not erase a host application's instructions")
	s.Require().Contains(instructions, `"items":[{"skill":"kafka-debugging"}]`, "initialize must show a callable bootstrap, not assume the client has local skill files")
	s.Require().Contains(instructions, "server_config", "reading guidance must not replace checking endpoint permissions before a write")

	out := s.call(session, []map[string]string{{"skill": "kafka-debugging"}})
	s.Require().Equal(1, out.Succeeded, "omitting file must load the umbrella skill instead of a nonexistent reference.md")
	s.Require().Len(out.Results, 1, "one requested document must produce one indexed result")
	s.Require().NotNil(out.Results[0].Result, "a successful skill read must carry its document")
	doc := out.Results[0].Result
	s.Require().Equal("SKILL.md", doc.File, "SKILL.md contains the scenario routing table the agent needs first")
	s.Require().Contains(doc.Content, "# Kafka debugging", "the response must contain actual Markdown, not a path on the server")
	s.Require().IsIncreasing(doc.Files, "available files must be sorted so repeated replies are deterministic")
	s.Require().Contains(doc.Files, "references/skip-poison-message.md", "the umbrella must let a client discover scenario guides")
	s.Require().Zero(out.Applied, "reading documentation must never count as an applied mutation")
	s.Require().False(out.Atomic, "the shared batch contract reports each independent result")
}

func (s *SkillReferenceSuite) TestLegacyInitializeAlsoIncludesBootstrap() {
	session := s.connect(false, "2024-11-05")
	s.Require().Contains(session.InitializeResult().Instructions, `"items":[{"skill":"kafka-debugging"}]`,
		"legacy initialize clients need the same bootstrap as server/discover clients")
	out := s.call(session, []map[string]string{{"skill": "kafka-debugging"}})
	s.Require().Equal(1, out.Succeeded, "the same built-in guidance must be available on older MCP protocols")
}

func (s *SkillReferenceSuite) TestEveryBundledDocumentMatchesItsSource() {
	session := s.connect(false)
	root := filepath.Join("..", "..", "..", "skills", "kafka-debugging")
	paths := []string{"SKILL.md"}
	guides, err := filepath.Glob(filepath.Join(root, "references", "*.md"))
	s.Require().NoError(err, "the source guides must be discoverable for the bundle freshness check")
	for _, guide := range guides {
		paths = append(paths, "references/"+filepath.Base(guide))
	}
	items := make([]map[string]string, 0, len(paths))
	for _, path := range paths {
		items = append(items, map[string]string{"skill": "kafka-debugging", "file": path})
	}
	out := s.call(session, items)
	s.Require().Equal(len(paths), out.Succeeded, "every source guide must ship in the binary, including newly added ones")
	s.Require().Len(out.Results, len(paths), "a document must not disappear from the batch")
	for index, path := range paths {
		source, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(path)))
		s.Require().NoError(err, "the canonical source for %s must be readable", path)
		s.Require().Equal(index, out.Results[index].Index, "the document's index must follow input order")
		s.Require().NotNil(out.Results[index].Result, "the existing guide %s must have a result", path)
		s.Require().Equal(string(source), out.Results[index].Result.Content, "go:embed must serve the current canonical Markdown for %s", path)
		s.Require().Equal(paths, out.Results[index].Result.Files, "available files must include every source and no obsolete guides")
	}
}

func (s *SkillReferenceSuite) TestInvalidDocumentsDoNotHideValidOnes() {
	session := s.connect(false)
	out := s.call(session, []map[string]string{
		{"skill": "unknown"},
		{"skill": "kafka-debugging", "file": "references/check-lag.md"},
		{"skill": "kafka-debugging", "file": "missing.md"},
	})
	s.Require().Equal(1, out.Succeeded, "a bad skill and file must not prevent the valid guide from being returned")
	s.Require().Equal(2, out.Failed, "unknown documents must be reported as failures, not empty successes")
	s.Require().Contains(out.Results[0].Error, "kafka-debugging", "unknown skill errors must list the available skills")
	s.Require().Contains(out.Results[2].Error, "SKILL.md", "unknown file errors must list the available documents")
	s.Require().NotNil(out.Results[1].Result, "an invalid neighbour must not remove the valid guide's result")
	s.Require().Contains(out.Results[1].Result.Content, "lag", "the successful item must contain the requested scenario")
}

func (s *SkillReferenceSuite) TestExactNamesOnly() {
	session := s.connect(false)
	s.Run("parent paths are not normalised into valid documents", func() {
		out := s.call(session, []map[string]string{{"skill": "kafka-debugging", "file": "references/../SKILL.md"}})
		s.Require().Equal(1, out.Failed, "only catalogued names may resolve; cleaning paths would widen the API")
	})
	s.Run("absolute disk paths cannot be read", func() {
		out := s.call(session, []map[string]string{{"skill": "kafka-debugging", "file": "/etc/passwd"}})
		s.Require().Equal(1, out.Failed, "a document name must never become a filesystem read")
	})
	s.Run("file names are case sensitive", func() {
		out := s.call(session, []map[string]string{{"skill": "kafka-debugging", "file": "skill.md"}})
		s.Require().Equal(1, out.Failed, "the API must use the exact names returned by the catalog")
	})
	s.Run("skill names are case sensitive", func() {
		out := s.call(session, []map[string]string{{"skill": "Kafka-Debugging"}})
		s.Require().Equal(1, out.Failed, "case folding must not turn an unknown skill into a different request")
	})
}

func (s *SkillReferenceSuite) TestEmptyAndOversizedBatchesAreRefused() {
	session := s.connect(false)
	s.Run("an empty batch is not a successful no-op", func() {
		out, err := session.CallTool(s.T().Context(), &mcp.CallToolParams{
			Name: "skill_reference", Arguments: map[string]any{"items": []any{}},
		})
		s.Require().NoError(err, "validation failures must be readable tool results")
		s.Require().True(out.IsError, "at least one document is required")
	})
	s.Run("an oversized batch is refused before reading documents", func() {
		items := make([]map[string]string, 1001)
		for index := range items {
			items[index] = map[string]string{"skill": "kafka-debugging"}
		}
		out, err := session.CallTool(s.T().Context(), &mcp.CallToolParams{
			Name: "skill_reference", Arguments: map[string]any{"items": items},
		})
		s.Require().NoError(err, "a bounded batch refusal must be delivered through MCP")
		s.Require().True(out.IsError, "documentation reads must obey the metadata batch limit")
	})
}

func (s *SkillReferenceSuite) TestReadOnlyAnnotationsAndDisabledBootstrap() {
	s.Run("read-only endpoints advertise safe document reads", func() {
		session := s.connect(false)
		listed, err := session.ListTools(s.T().Context(), nil)
		s.Require().NoError(err, "a read-only endpoint must advertise its available tools")
		var found *mcp.Tool
		for _, tool := range listed.Tools {
			if tool.Name == "skill_reference" {
				found = tool
			}
		}
		s.Require().NotNil(found, "read_only must not hide instructions for diagnosing a protected cluster")
		s.Require().NotNil(found.Annotations, "clients need annotations to distinguish a safe read from an external operation")
		s.Require().True(found.Annotations.ReadOnlyHint, "the tool reads only built-in Markdown")
		s.Require().NotNil(found.Annotations.OpenWorldHint, "closed-world behavior must be explicit, not the default open world")
		s.Require().False(*found.Annotations.OpenWorldHint, "reading a bundled guide must not access external entities")
		s.Require().NotNil(found.Annotations.DestructiveHint, "non-destructive behavior must be explicit")
		s.Require().False(*found.Annotations.DestructiveHint, "the tool must never mutate a cluster or file")
	})
	s.Run("a disabled tool leaves no dangling initialize pointer", func() {
		session := s.connect(true)
		s.Require().Equal("Keep the host's instructions.", session.InitializeResult().Instructions, "withholding the tool must preserve host instructions without adding an unusable bootstrap")
		listed, err := session.ListTools(s.T().Context(), nil)
		s.Require().NoError(err, "tool listing must still work when guidance is disabled")
		for _, tool := range listed.Tools {
			s.Require().NotEqual("skill_reference", tool.Name, "a disabled tool must not remain callable through discovery")
		}
		out, err := session.CallTool(s.T().Context(), &mcp.CallToolParams{Name: "server_config"})
		s.Require().NoError(err, "server_config must still describe the effective endpoint")
		encoded, err := json.Marshal(out.StructuredContent)
		s.Require().NoError(err, "the endpoint tool list must be serializable")
		s.Require().False(strings.Contains(string(encoded), "skill_reference"), "server_config must agree that the documentation tool was withheld")
	})
}
