package kafkaclient

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/denizgursoy/kafka-mcp/internal/domain/config"
)

// generateClientCert returns a self-signed client certificate and key as PEM,
// plus the parsed certificate for a server to trust.
func generateClientCert(t *testing.T) (string, string, *x509.Certificate) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: "kafka-mcp-test-client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse certificate: %v", err)
	}

	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})), cert
}

func serverCAPEM(server *httptest.Server) string {
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}))
}

// connectProxy is an HTTP CONNECT proxy that counts tunnels it opens.
func connectProxy(t *testing.T, tunnels *atomic.Int32) *httptest.Server {
	t.Helper()
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect {
			http.Error(w, "connect only", http.StatusMethodNotAllowed)
			return
		}
		tunnels.Add(1)
		upstream, err := net.Dial("tcp", r.Host)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			upstream.Close()
			return
		}
		_, _ = conn.Write([]byte("HTTP/1.1 200 Connection established\r\n\r\n"))
		go func() {
			_, _ = io.Copy(upstream, conn)
			upstream.Close()
		}()
		_, _ = io.Copy(conn, upstream)
		conn.Close()
	}))
	t.Cleanup(proxy.Close)

	return proxy
}

type OAuthMTLSSuite struct{ suite.Suite }

func TestOAuthMTLSSuite(t *testing.T) { suite.Run(t, new(OAuthMTLSSuite)) }

// tokenServer requires a client certificate signed by client. With an empty
// secret it expects RFC 8705 tls_client_auth: client_id in the body and no
// basic authentication.
func (s *OAuthMTLSSuite) tokenServer(client *x509.Certificate, secret string, calls *atomic.Int32) *httptest.Server {
	pool := x509.NewCertPool()
	pool.AddCert(client)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, basic := r.BasicAuth()
		ok := r.FormValue("grant_type") == "client_credentials"
		if secret == "" {
			ok = ok && !basic && r.FormValue("client_id") == "worker" && r.FormValue("client_secret") == ""
		} else {
			ok = ok && basic && user == "worker" && pass == secret
		}
		if !ok {
			http.Error(w, `{"error":"invalid_client"}`, http.StatusUnauthorized)
			return
		}
		n := calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"access_token":"mtls-%d","token_type":"Bearer","expires_in":3600}`, n)
	}))
	server.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool}
	server.StartTLS()
	s.T().Cleanup(server.Close)

	return server
}

func (s *OAuthMTLSSuite) TestClientCertificateWithoutSecret() {
	var calls atomic.Int32
	certPEM, keyPEM, cert := generateClientCert(s.T())
	server := s.tokenServer(cert, "", &calls)
	mechanism, err := oauthMechanism(&config.SASLOAuth{
		TokenURL: server.URL, ClientID: "worker",
		TLS: config.OAuthTLS{Cert: certPEM, Key: keyPEM, CA: serverCAPEM(server)},
	})
	s.Require().NoError(err, "a client certificate must be enough to authenticate to the token endpoint")
	for range 3 {
		_, initial, err := mechanism.Authenticate(s.T().Context(), "broker")
		s.Require().NoError(err, "tls_client_auth must obtain a token without a client secret")
		s.Require().Contains(string(initial), "auth=Bearer mtls-1", "the token fetched over mTLS must reach Kafka")
	}
	s.Require().Equal(int32(1), calls.Load(), "the mTLS token must be cached like any client-credentials token")
}

func (s *OAuthMTLSSuite) TestClientCertificateWithSecret() {
	var calls atomic.Int32
	certPEM, keyPEM, cert := generateClientCert(s.T())
	server := s.tokenServer(cert, "secret", &calls)
	mechanism, err := oauthMechanism(&config.SASLOAuth{
		TokenURL: server.URL, ClientID: "worker", ClientSecret: "secret",
		TLS: config.OAuthTLS{Cert: certPEM, Key: keyPEM, CA: serverCAPEM(server)},
	})
	s.Require().NoError(err, "a certificate and a secret must be usable together")
	_, initial, err := mechanism.Authenticate(s.T().Context(), "broker")
	s.Require().NoError(err, "with a secret configured, basic authentication must still be sent over mTLS")
	s.Require().Contains(string(initial), "auth=Bearer mtls-1", "the token must reach Kafka")
}

func (s *OAuthMTLSSuite) TestCAWithoutClientCertificate() {
	var calls atomic.Int32
	_, _, cert := generateClientCert(s.T())
	server := s.tokenServer(cert, "secret", &calls)
	mechanism, err := oauthMechanism(&config.SASLOAuth{
		TokenURL: server.URL, ClientID: "worker", ClientSecret: "secret",
		TLS: config.OAuthTLS{CA: serverCAPEM(server)},
	})
	s.Require().NoError(err, "a custom CA alone is valid configuration")
	_, _, err = mechanism.Authenticate(s.T().Context(), "broker")
	s.Require().Error(err, "a server demanding a client certificate must refuse a client that has none")
	s.Require().Zero(calls.Load(), "the request must fail at the handshake, before the token handler")
}

func (s *OAuthMTLSSuite) TestInsecureSkipVerify() {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"access_token":"untrusted","token_type":"Bearer","expires_in":3600}`)
	}))
	s.T().Cleanup(server.Close)

	s.Run("verification is on by default", func() {
		mechanism, err := oauthMechanism(&config.SASLOAuth{TokenURL: server.URL, ClientID: "worker", ClientSecret: "secret"})
		s.Require().NoError(err, "a self-signed endpoint is a runtime failure, not a configuration error")
		_, _, err = mechanism.Authenticate(s.T().Context(), "broker")
		s.Require().Error(err, "a token endpoint with an untrusted certificate must be refused unless verification is disabled")
	})
	s.Run("skipping verification reaches the endpoint", func() {
		mechanism, err := oauthMechanism(&config.SASLOAuth{
			TokenURL: server.URL, ClientID: "worker", ClientSecret: "secret",
			TLS: config.OAuthTLS{InsecureSkipVerify: true},
		})
		s.Require().NoError(err, "insecure_skip_verify alone is valid token endpoint TLS")
		_, initial, err := mechanism.Authenticate(s.T().Context(), "broker")
		s.Require().NoError(err, "insecure_skip_verify must accept the self-signed endpoint")
		s.Require().Contains(string(initial), "auth=Bearer untrusted", "the token must reach Kafka")
	})
}

func (s *OAuthMTLSSuite) TestProxy() {
	s.Run("plain HTTP token endpoint", func() {
		var proxied atomic.Int32
		proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Host != "token.invalid" {
				http.Error(w, "unexpected host", http.StatusBadRequest)
				return
			}
			proxied.Add(1)
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"access_token":"via-proxy","token_type":"Bearer","expires_in":3600}`)
		}))
		s.T().Cleanup(proxy.Close)
		mechanism, err := oauthMechanism(&config.SASLOAuth{
			TokenURL: "http://token.invalid/token", ClientID: "client", ClientSecret: "secret", Proxy: proxy.URL,
		})
		s.Require().NoError(err, "a proxy must be accepted for client credentials")
		_, initial, err := mechanism.Authenticate(s.T().Context(), "broker")
		s.Require().NoError(err, "a host only the proxy can resolve must be reached through it")
		s.Require().Contains(string(initial), "auth=Bearer via-proxy", "the token fetched through the proxy must reach Kafka")
		s.Require().Equal(int32(1), proxied.Load(), "the token request must go through the configured proxy")
	})
	s.Run("mTLS through a CONNECT tunnel", func() {
		var calls, tunnels atomic.Int32
		certPEM, keyPEM, cert := generateClientCert(s.T())
		server := s.tokenServer(cert, "", &calls)
		proxy := connectProxy(s.T(), &tunnels)
		mechanism, err := oauthMechanism(&config.SASLOAuth{
			TokenURL: server.URL, ClientID: "worker", Proxy: proxy.URL,
			TLS: config.OAuthTLS{Cert: certPEM, Key: keyPEM, CA: serverCAPEM(server)},
		})
		s.Require().NoError(err, "a proxy and a client certificate must be usable together")
		_, initial, err := mechanism.Authenticate(s.T().Context(), "broker")
		s.Require().NoError(err, "the client certificate must survive the proxy tunnel")
		s.Require().Contains(string(initial), "auth=Bearer mtls-1", "the token must reach Kafka")
		s.Require().Positive(tunnels.Load(), "the HTTPS token request must be tunnelled through the proxy")
	})
}

func (s *OAuthMTLSSuite) TestCertificateRotation() {
	certPEM, keyPEM, _ := generateClientCert(s.T())
	dir := s.T().TempDir()
	write := func(name, content string) string {
		path := filepath.Join(dir, name)
		s.Require().NoError(os.WriteFile(path, []byte(content), 0o600), "the certificate fixture must be written")
		return path
	}
	settings := config.OAuthTLS{CertFile: write("cert.pem", certPEM), KeyFile: write("key.pem", keyPEM)}
	client, err := newMTLSClient(settings, "")
	s.Require().NoError(err, "certificate files must load")
	first, err := client.Client()
	s.Require().NoError(err, "the first client must build")
	again, err := client.Client()
	s.Require().NoError(err, "an unchanged certificate must not fail")
	s.Require().Same(first, again, "unchanged files must reuse the client and its connections")

	newCert, newKey, _ := generateClientCert(s.T())
	write("cert.pem", newCert)
	write("key.pem", newKey)
	rotated, err := client.Client()
	s.Require().NoError(err, "a rotated certificate must load")
	s.Require().NotSame(first, rotated, "rotated files must rebuild the client, so cert-manager rotation needs no restart")
	s.Require().Equal(newCert, client.CertPEM(), "the rotated certificate must be the one presented")
}

func (s *OAuthMTLSSuite) TestInvalidCertificates() {
	certPEM, _, _ := generateClientCert(s.T())

	s.Run("key does not parse", func() {
		_, err := oauthMechanism(&config.SASLOAuth{
			TokenURL: "https://idp/token", ClientID: "worker",
			TLS: config.OAuthTLS{Cert: certPEM, Key: "not a key"},
		})
		s.Require().ErrorContains(err, "cert/key", "an unusable key pair must fail at startup rather than on first authentication")
	})
	s.Run("certificate file is missing", func() {
		_, err := oauthMechanism(&config.SASLOAuth{
			TokenURL: "https://idp/token", ClientID: "worker",
			TLS: config.OAuthTLS{CertFile: filepath.Join(s.T().TempDir(), "absent.pem"), Key: "k"},
		})
		s.Require().Error(err, "a missing certificate file must not silently fall back to no client certificate")
	})
	s.Run("CA holds no certificate", func() {
		_, err := oauthMechanism(&config.SASLOAuth{
			TokenURL: "https://idp/token", ClientID: "worker", ClientSecret: "secret",
			TLS: config.OAuthTLS{CA: "not a certificate"},
		})
		s.Require().ErrorContains(err, "ca", "an empty custom trust store must fail rather than trust nothing silently")
	})
}
