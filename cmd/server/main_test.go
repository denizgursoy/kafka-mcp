package main

import (
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

func TestHTTPRoutesSuite(t *testing.T) {
	suite.Run(t, new(HTTPRoutesSuite))
}

func (s *HTTPRoutesSuite) TestConfiguredBasePathPrefixesEveryEndpoint() {
	server := newHTTPServer(
		&config.Config{HTTP: config.HTTP{BasePath: "/gateway/kafka"}},
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

		s.Require().Equal(http.StatusBadRequest, response.Code,
			"the prefixed MCP wildcard must reach the SDK, which reports an unknown cluster as a bad request")
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
