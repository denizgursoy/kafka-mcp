package main

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/suite"

	"github.com/denizgursoy/kafka-mcp/internal/domain/config"
)

type HTTPRoutesSuite struct {
	suite.Suite
}

type StdioEndpointSuite struct {
	suite.Suite
}

func TestStdioEndpointSuite(t *testing.T) {
	suite.Run(t, new(StdioEndpointSuite))
}

func (s *StdioEndpointSuite) TestSelectsTheOnlyEndpointByDefault() {
	cfg := &config.Config{Endpoints: map[string]*config.Endpoint{
		"local": {Name: "local", Cluster: "local"},
	}}

	name, err := selectStdioEndpoint(cfg, "")

	s.Require().NoError(err,
		"stdio should need no endpoint flag when the configuration has only one unambiguous endpoint")
	s.Require().Equal("local", name,
		"stdio must bind its single session to the only configured endpoint")
}

func (s *StdioEndpointSuite) TestSelectsAnExplicitEndpoint() {
	cfg := &config.Config{Endpoints: map[string]*config.Endpoint{
		"prod-read": {Name: "prod-read", Cluster: "prod", ReadOnly: true},
		"preprod":   {Name: "preprod", Cluster: "preprod"},
	}}

	name, err := selectStdioEndpoint(cfg, "preprod")

	s.Require().NoError(err,
		"an explicit endpoint must let stdio select one policy from a multi-endpoint configuration")
	s.Require().Equal("preprod", name,
		"stdio must use the endpoint named by --endpoint rather than an arbitrary map entry")
}

func (s *StdioEndpointSuite) TestRefusesAnAmbiguousConfiguration() {
	cfg := &config.Config{Endpoints: map[string]*config.Endpoint{
		"prod-read": {Name: "prod-read", Cluster: "prod", ReadOnly: true},
		"preprod":   {Name: "preprod", Cluster: "preprod"},
	}}

	_, err := selectStdioEndpoint(cfg, "")

	s.Require().ErrorContains(err, "--endpoint",
		"stdio must ask for an endpoint when several policies exist instead of silently choosing the wrong cluster")
	s.Require().ErrorContains(err, "preprod, prod-read",
		"the selection error must list endpoint names in stable order so the operator can correct the command")
}

func (s *StdioEndpointSuite) TestRefusesAnUnknownExplicitEndpoint() {
	cfg := &config.Config{Endpoints: map[string]*config.Endpoint{
		"local": {Name: "local", Cluster: "local"},
	}}

	_, err := selectStdioEndpoint(cfg, "production")

	s.Require().ErrorContains(err, `endpoint "production" is not configured`,
		"an endpoint typo must stop startup rather than connecting the stdio client somewhere else")
	s.Require().ErrorContains(err, "local",
		"the unknown-endpoint error must name the configured alternative")
}

func (s *StdioEndpointSuite) TestTreatsDirectEOFAsANormalClientClose() {
	s.Require().True(isNormalStdioClose(io.EOF),
		"a stdio client closing its input is the normal end of a session and must not make the process fail")
}

func (s *StdioEndpointSuite) TestTreatsTheSDKClosingEOFAsANormalClientClose() {
	err := errors.New("server is closing: EOF")

	s.Require().True(isNormalStdioClose(err),
		"the MCP SDK's current unwrapped EOF form must still produce a successful stdio shutdown")
}

func (s *StdioEndpointSuite) TestDoesNotHideOtherStdioFailures() {
	err := errors.New("read stdin: permission denied")

	s.Require().False(isNormalStdioClose(err),
		"transport failures other than a client EOF must be returned so broken stdio sessions remain diagnosable")
}

func TestHTTPRoutesSuite(t *testing.T) {
	suite.Run(t, new(HTTPRoutesSuite))
}

func (s *HTTPRoutesSuite) TestConfiguredBasePathPrefixesEveryEndpoint() {
	server := newHTTPServer(
		&config.Config{
			HTTP: config.HTTP{BasePath: "/gateway/kafka"},
			Endpoints: map[string]*config.Endpoint{
				"local": {Name: "local", Cluster: "local", Path: "/mcp/local"},
			},
		},
		map[string]*mcp.Server{},
	)

	s.Run("health is served below the base path", func() {
		request := httptest.NewRequestWithContext(s.T().Context(), http.MethodGet, "/gateway/kafka/healthz", nil)
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		s.Require().Equal(http.StatusOK, response.Code,
			"a reverse proxy mounting the service below a path needs a liveness endpoint at that same mount point")
		s.Require().Equal("ok\n", response.Body.String(),
			"the prefixed health endpoint must retain the existing response contract")
	})

	s.Run("mcp is served below the base path", func() {
		request := httptest.NewRequestWithContext(s.T().Context(), http.MethodPost, "/gateway/kafka/mcp/unknown", nil)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Accept", "application/json, text/event-stream")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		s.Require().Equal(http.StatusNotFound, response.Code,
			"an unconfigured endpoint below the base path must not be captured by a broader writable or read-only route")
	})

	s.Run("unprefixed endpoints are not exposed", func() {
		request := httptest.NewRequestWithContext(s.T().Context(), http.MethodGet, "/healthz", nil)
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		s.Require().Equal(http.StatusNotFound, response.Code,
			"setting a base path must move the endpoint rather than exposing a second route that bypasses the proxy mount")
	})
}

func (s *HTTPRoutesSuite) TestEmptyBasePathKeepsExistingRoutes() {
	server := newHTTPServer(
		&config.Config{HTTP: config.HTTP{}},
		map[string]*mcp.Server{},
	)
	request := httptest.NewRequestWithContext(s.T().Context(), http.MethodGet, "/healthz", nil)
	response := httptest.NewRecorder()

	server.ServeHTTP(response, request)

	s.Require().Equal(http.StatusOK, response.Code,
		"deployments that do not configure a base path must keep their existing health URL")
}

func (s *HTTPRoutesSuite) TestConfiguredEndpointPathsAreExact() {
	readServer := mcp.NewServer(&mcp.Implementation{Name: "read", Version: "test"}, nil)
	writeServer := mcp.NewServer(&mcp.Implementation{Name: "write", Version: "test"}, nil)
	cfg := &config.Config{
		Endpoints: map[string]*config.Endpoint{
			"read":  {Name: "read", Cluster: "prod", Path: "/mcp", ReadOnly: true},
			"write": {Name: "write", Cluster: "prod", Path: "/mcp/rw"},
		},
	}

	server := newHTTPServer(cfg, map[string]*mcp.Server{
		"read":  readServer,
		"write": writeServer,
	})

	for _, endpointPath := range []string{"/mcp", "/mcp/rw"} {
		request := httptest.NewRequestWithContext(s.T().Context(), http.MethodPost, endpointPath, nil)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Accept", "application/json, text/event-stream")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		s.Require().NotEqual(http.StatusNotFound, response.Code,
			"each configured endpoint path must be mounted even when one path is a prefix of another")
	}

	request := httptest.NewRequestWithContext(s.T().Context(), http.MethodPost, "/mcp/other", nil)
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)

	s.Require().Equal(http.StatusNotFound, response.Code,
		"custom endpoint routing must use exact paths so a read endpoint cannot accidentally catch a nearby path")
}
