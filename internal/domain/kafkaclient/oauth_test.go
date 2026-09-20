package kafkaclient

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/denizgursoy/kafka-mcp/internal/domain/config"
)

type OAuthSuite struct{ suite.Suite }

func TestOAuthSuite(t *testing.T) { suite.Run(t, new(OAuthSuite)) }

func (s *OAuthSuite) TestStaticToken() {
	mechanism, err := oauthMechanism(&config.SASLOAuth{Token: "static-secret", Zid: "delegate", Extensions: map[string]string{"tenant": "test"}})
	s.Require().NoError(err, "static token authentication must not need an identity provider")
	_, initial, err := mechanism.Authenticate(s.T().Context(), "broker")
	s.Require().NoError(err, "static tokens must generate an OAUTHBEARER handshake")
	s.Require().Contains(string(initial), "auth=Bearer static-secret", "the token must reach the Kafka authentication payload")
	s.Require().Contains(string(initial), "a=delegate", "OAuth must retain the configured authorization identity")
	s.Require().Contains(string(initial), "tenant=test", "OAuth extensions must reach the broker")
}

func (s *OAuthSuite) TestRefreshAndCache() {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != "client" || pass != "secret" || r.FormValue("grant_type") != "client_credentials" || r.FormValue("scope") != "kafka read" {
			http.Error(w, "invalid request", http.StatusUnauthorized)
			return
		}
		n := calls.Add(1)
		expires := 3600
		if n == 1 {
			expires = 1
		} // Inside oauth2's near-expiry refresh window.
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"access_token":"token-%d","token_type":"Bearer","expires_in":%d}`, n, expires)
	}))
	defer server.Close()
	mechanism, err := oauthMechanism(&config.SASLOAuth{TokenURL: server.URL, ClientID: "client", ClientSecret: "secret", Scopes: []string{"kafka", "read"}})
	s.Require().NoError(err, "client credentials must create a mechanism without fetching a token yet")
	s.Require().Zero(calls.Load(), "startup must not eagerly contact the identity provider")
	firstCtx, cancel := context.WithCancel(s.T().Context())
	_, initial, err := mechanism.Authenticate(firstCtx, "broker")
	cancel()
	s.Require().NoError(err, "the first authentication must fetch its token")
	s.Require().Contains(string(initial), "Bearer token-1", "the fetched token must be sent to Kafka")
	_, renewed, err := mechanism.Authenticate(s.T().Context(), "broker")
	s.Require().NoError(err, "renewal must use the new context, not the cancelled first reader context")
	s.Require().Contains(string(renewed), "Bearer token-2", "near-expiry tokens must be renewed before use")
	_, cached, err := mechanism.Authenticate(s.T().Context(), "broker")
	s.Require().NoError(err, "subsequent authentication must reuse a valid token")
	s.Require().Equal(renewed, cached, "independent connections must share the cached token")
	s.Require().Equal(int32(2), calls.Load(), "a still-valid token must not trigger another endpoint request")
}

func (s *OAuthSuite) TestConcurrentAuthentication() {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"access_token":"shared","token_type":"Bearer","expires_in":3600}`)
	}))
	defer server.Close()
	mechanism, err := oauthMechanism(&config.SASLOAuth{TokenURL: server.URL, ClientID: "client", ClientSecret: "secret"})
	s.Require().NoError(err, "concurrent readers need a shared authentication mechanism")
	ctx := s.T().Context()
	errors := make(chan error, 10)
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() { _, _, err := mechanism.Authenticate(ctx, "broker"); errors <- err })
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		s.Require().NoError(err, "simultaneous connections must all obtain authentication")
	}
	s.Require().Equal(int32(1), calls.Load(), "concurrent refreshes must collapse to one token request")
}

func (s *OAuthSuite) TestTokenEndpointFailures() {
	s.Run("timeout", func() {
		release := make(chan struct{})
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-release }))
		defer server.Close()
		defer close(release)
		mechanism, err := oauthMechanism(&config.SASLOAuth{TokenURL: server.URL, ClientID: "client", ClientSecret: "secret", Timeout: 50 * time.Millisecond})
		s.Require().NoError(err, "a positive timeout must be accepted")
		_, _, err = mechanism.Authenticate(s.T().Context(), "broker")
		s.Require().ErrorIs(err, context.DeadlineExceeded, "a stalled identity provider must not hang authentication")
	})
	s.Run("error body is redacted", func() {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "echoed-client-secret", http.StatusUnauthorized)
		}))
		defer server.Close()
		mechanism, err := oauthMechanism(&config.SASLOAuth{TokenURL: server.URL, ClientID: "client", ClientSecret: "secret"})
		s.Require().NoError(err, "endpoint errors happen during authentication")
		_, _, err = mechanism.Authenticate(s.T().Context(), "broker")
		s.Require().ErrorContains(err, "401", "the operator needs the identity provider's HTTP status")
		s.Require().NotContains(err.Error(), "echoed-client-secret", "identity provider response bodies must not leak secrets into MCP errors")
	})
}
