package kafkaclient

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/denizgursoy/kafka-mcp/internal/domain/config"
)

const gcpTestServiceAccount = "sa@proj.iam.gserviceaccount.com"

// gcpFake stands in for Google STS and the IAM credentials API. Both require
// the client certificate it was built with.
type gcpFake struct {
	server        *httptest.Server
	sts           atomic.Int32
	impersonation atomic.Int32
	fail          atomic.Bool
	expires       time.Time
}

func newGCPFake(t *testing.T, client *x509.Certificate) *gcpFake {
	t.Helper()
	fake := &gcpFake{expires: time.Now().Add(time.Hour)}
	pool := x509.NewCertPool()
	pool.AddCert(client)

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/token", func(w http.ResponseWriter, r *http.Request) {
		fake.sts.Add(1)
		if fake.fail.Load() {
			http.Error(w, "echoed-subject-token", http.StatusInternalServerError)
			return
		}
		var certs []string
		if r.FormValue("subject_token_type") != gcpTokenTypeMTLS ||
			r.FormValue("grant_type") != gcpGrantTypeTokenExchange ||
			r.FormValue("audience") != "//iam.googleapis.com/test" ||
			json.Unmarshal([]byte(r.FormValue("subject_token")), &certs) != nil || len(certs) != 1 {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		der, _ := base64.StdEncoding.DecodeString(certs[0])
		if string(r.TLS.PeerCertificates[0].Raw) != string(der) {
			http.Error(w, "certificate mismatch", http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "sts-token", "expires_in": 3600})
	})
	mux.HandleFunc("/v1/projects/-/serviceAccounts/"+gcpTestServiceAccount+":generateAccessToken",
		func(w http.ResponseWriter, r *http.Request) {
			fake.impersonation.Add(1)
			var body struct {
				Lifetime string `json:"lifetime"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			if r.Header.Get("Authorization") != "Bearer sts-token" || body.Lifetime != "3600s" {
				http.Error(w, "bad impersonation", http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"accessToken": "ya29.final",
				"expireTime":  fake.expires.UTC().Format(time.RFC3339),
			})
		})

	fake.server = httptest.NewUnstartedServer(mux)
	fake.server.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool}
	fake.server.StartTLS()
	t.Cleanup(fake.server.Close)

	return fake
}

type OAuthGCPSuite struct{ suite.Suite }

func TestOAuthGCPSuite(t *testing.T) { suite.Run(t, new(OAuthGCPSuite)) }

// settings returns GCP settings that authenticate against a fresh fake.
func (s *OAuthGCPSuite) settings() (config.SASLOAuthGCP, *gcpFake) {
	certPEM, keyPEM, cert := generateClientCert(s.T())
	fake := newGCPFake(s.T(), cert)
	credentials, err := json.Marshal(map[string]any{
		"type":                              "external_account",
		"audience":                          "//iam.googleapis.com/test",
		"subject_token_type":                gcpTokenTypeMTLS,
		"token_url":                         fake.server.URL + "/v1/token",
		"service_account_impersonation_url": fake.server.URL + "/v1/projects/-/serviceAccounts/" + gcpTestServiceAccount + ":generateAccessToken",
	})
	s.Require().NoError(err, "the credentials fixture must encode")

	return config.SASLOAuthGCP{
		Enabled:     true,
		Credentials: string(credentials),
		Cert:        certPEM,
		Key:         keyPEM,
		CA:          serverCAPEM(fake.server),
	}, fake
}

func (s *OAuthGCPSuite) TestSharedCachedToken() {
	settings, fake := s.settings()
	mechanism, err := oauthMechanism(&config.SASLOAuth{GCP: settings})
	s.Require().NoError(err, "GCP must build an OAUTHBEARER mechanism")
	s.Require().Equal("OAUTHBEARER", mechanism.Name(), "GCP tokens are presented to Kafka as OAUTHBEARER")
	s.Require().Zero(fake.sts.Load(), "startup must not contact Google before the first authentication")

	var wg sync.WaitGroup
	results := make(chan string, 8)
	for range 8 {
		wg.Go(func() {
			_, initial, err := mechanism.Authenticate(s.T().Context(), "broker")
			if err != nil {
				results <- "error: " + err.Error()
				return
			}
			results <- string(initial)
		})
	}
	wg.Wait()
	close(results)
	for initial := range results {
		s.Require().Contains(initial, "auth=Bearer ya29.final", "every connection must present the impersonated token")
	}
	s.Require().Equal(int32(1), fake.sts.Load(), "concurrent connections must share one STS exchange")
	s.Require().Equal(int32(1), fake.impersonation.Load(), "concurrent connections must share one impersonation")
}

func (s *OAuthGCPSuite) TestManagedKafkaFormat() {
	settings, _ := s.settings()
	settings.Format = config.GCPFormatManagedKafka
	source, err := newGCPTokenSource(settings, time.Second)
	s.Require().NoError(err, "managed_kafka must be accepted with an impersonated service account")
	token, err := source.Token(s.T().Context())
	s.Require().NoError(err, "the token must be fetched")
	parts := strings.Split(token, ".")
	s.Require().Len(parts, 3, "Managed Kafka expects a three-part JWT-like envelope")
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	s.Require().NoError(err, "the payload must be base64url")
	s.Require().Contains(string(payload), `"sub":"`+gcpTestServiceAccount+`"`, "the subject must be the impersonated service account")
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	s.Require().NoError(err, "the signature part must be base64url")
	s.Require().Equal("ya29.final", string(signature), "the Google access token travels in the signature position")
}

func (s *OAuthGCPSuite) TestManagedKafkaNeedsServiceAccount() {
	certPEM, keyPEM, _ := generateClientCert(s.T())
	_, err := newGCPTokenSource(config.SASLOAuthGCP{
		Enabled: true, Audience: "a", Cert: certPEM, Key: keyPEM, Format: config.GCPFormatManagedKafka,
	}, time.Second)
	s.Require().ErrorContains(err, "service_account_impersonation_url",
		"the managed_kafka envelope names a service account, so one must be configured")
}

func (s *OAuthGCPSuite) TestStaleTokenOnRefreshFailure() {
	settings, fake := s.settings()
	source, err := newGCPTokenSource(settings, time.Second)
	s.Require().NoError(err, "the token source must build")
	_, err = source.Token(s.T().Context())
	s.Require().NoError(err, "the first token must be fetched")

	fake.fail.Store(true)
	source.now = func() time.Time { return fake.expires.Add(-time.Minute) }
	token, err := source.Token(s.T().Context())
	s.Require().NoError(err, "a failed refresh must not break connections while the cached token is still valid")
	s.Require().Equal("ya29.final", token, "the still-valid cached token must be used")

	source.now = func() time.Time { return fake.expires.Add(time.Minute) }
	_, err = source.Token(s.T().Context())
	s.Require().ErrorContains(err, "sts exchange", "an expired token cannot be reused, so the refresh failure must surface")
	s.Require().ErrorContains(err, "500", "the operator needs Google's HTTP status")
	s.Require().NotContains(err.Error(), "echoed-subject-token", "Google response bodies must not leak into MCP errors or logs")
}

func (s *OAuthGCPSuite) TestFilesAndRotation() {
	settings, fake := s.settings()
	dir := s.T().TempDir()
	write := func(name, content string) string {
		path := filepath.Join(dir, name)
		s.Require().NoError(os.WriteFile(path, []byte(content), 0o600), "the fixture file must be written")
		return path
	}
	settings.CredentialsFile = write("credentials.json", settings.Credentials)
	settings.CertFile = write("cert.pem", settings.Cert)
	settings.KeyFile = write("key.pem", settings.Key)
	settings.CAFile = write("ca.pem", settings.CA)
	settings.Credentials, settings.Cert, settings.Key, settings.CA = "", "", "", ""

	source, err := newGCPTokenSource(settings, time.Second)
	s.Require().NoError(err, "every GCP value must be loadable from a file")
	_, err = source.Token(s.T().Context())
	s.Require().NoError(err, "file-based settings must authenticate")

	certPEM, keyPEM, _ := generateClientCert(s.T())
	write("cert.pem", certPEM)
	write("key.pem", keyPEM)
	source.cached = nil
	before := fake.sts.Load()
	_, err = source.Token(s.T().Context())
	s.Require().Error(err, "a rotated, untrusted certificate must be the one presented on the next refresh")
	s.Require().Equal(before, fake.sts.Load(), "the untrusted certificate must fail at the handshake, before STS")
}

func (s *OAuthGCPSuite) TestInsecureSkipVerify() {
	s.Run("verification is on by default", func() {
		settings, _ := s.settings()
		settings.CA = ""
		source, err := newGCPTokenSource(settings, time.Second)
		s.Require().NoError(err, "an untrusted endpoint is a runtime failure, not a configuration error")
		_, err = source.Token(s.T().Context())
		s.Require().Error(err, "Google endpoints with an untrusted certificate must be refused unless verification is disabled")
	})
	s.Run("skipping verification reaches both endpoints", func() {
		settings, fake := s.settings()
		settings.CA = ""
		settings.InsecureSkipVerify = true
		source, err := newGCPTokenSource(settings, time.Second)
		s.Require().NoError(err, "insecure_skip_verify must be accepted")
		token, err := source.Token(s.T().Context())
		s.Require().NoError(err, "insecure_skip_verify must accept the untrusted endpoints")
		s.Require().Equal("ya29.final", token, "the impersonated token must be returned")
		s.Require().Equal(int32(1), fake.impersonation.Load(), "impersonation must also skip verification")
	})
}

func (s *OAuthGCPSuite) TestProxy() {
	s.Run("gcp.proxy", func() {
		var tunnels atomic.Int32
		settings, fake := s.settings()
		settings.Proxy = connectProxy(s.T(), &tunnels).URL
		mechanism, err := oauthMechanism(&config.SASLOAuth{GCP: settings})
		s.Require().NoError(err, "a GCP proxy must be accepted")
		_, initial, err := mechanism.Authenticate(s.T().Context(), "broker")
		s.Require().NoError(err, "the Google endpoints must be reachable through the proxy")
		s.Require().Contains(string(initial), "auth=Bearer ya29.final", "the token must reach Kafka")
		s.Require().Positive(tunnels.Load(), "requests must be tunnelled through gcp.proxy")
		s.Require().Equal(int32(1), fake.sts.Load(), "STS must be reached once through the tunnel")
	})
	s.Run("falls back to oauth.proxy", func() {
		var tunnels atomic.Int32
		settings, _ := s.settings()
		mechanism, err := oauthMechanism(&config.SASLOAuth{GCP: settings, Proxy: connectProxy(s.T(), &tunnels).URL})
		s.Require().NoError(err, "oauth.proxy must be accepted with GCP")
		_, _, err = mechanism.Authenticate(s.T().Context(), "broker")
		s.Require().NoError(err, "the Google endpoints must be reachable through oauth.proxy")
		s.Require().Positive(tunnels.Load(), "without gcp.proxy, oauth.proxy must be used")
	})
}

func (s *OAuthGCPSuite) TestInvalidCredentials() {
	certPEM, keyPEM, _ := generateClientCert(s.T())

	s.Run("wrong credential type", func() {
		_, err := oauthMechanism(&config.SASLOAuth{GCP: config.SASLOAuthGCP{
			Enabled: true, Credentials: `{"type":"service_account"}`, Cert: certPEM, Key: keyPEM,
		}})
		s.Require().ErrorContains(err, "unsupported credentials type", "only external_account credentials federate an X.509 identity")
	})
	s.Run("wrong subject token type", func() {
		_, err := oauthMechanism(&config.SASLOAuth{GCP: config.SASLOAuthGCP{
			Enabled: true, Credentials: `{"audience":"a","subject_token_type":"urn:ietf:params:oauth:token-type:jwt"}`,
			Cert: certPEM, Key: keyPEM,
		}})
		s.Require().ErrorContains(err, "subject_token_type", "a pool expecting another subject type would reject every exchange")
	})
	s.Run("credentials are not JSON", func() {
		_, err := oauthMechanism(&config.SASLOAuth{GCP: config.SASLOAuthGCP{
			Enabled: true, Credentials: "not json", Cert: certPEM, Key: keyPEM,
		}})
		s.Require().ErrorContains(err, "parse credentials", "unreadable credentials must fail at startup")
	})
	s.Run("audience missing after reading credentials", func() {
		_, err := oauthMechanism(&config.SASLOAuth{GCP: config.SASLOAuthGCP{
			Enabled: true, Credentials: `{"type":"external_account"}`, Cert: certPEM, Key: keyPEM,
		}})
		s.Require().ErrorContains(err, "audience", "STS cannot exchange without a workload identity provider")
	})
}
