package kafkaclient

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"github.com/twmb/tlscfg"

	"github.com/denizgursoy/kafka-mcp/internal/domain/config"
)

type SecuritySuite struct{ suite.Suite }

func TestSecuritySuite(t *testing.T) { suite.Run(t, new(SecuritySuite)) }

func (s *SecuritySuite) TestTLSDefaults() {
	settings, err := tlsConfig(&config.TLS{Enabled: true})
	s.Require().NoError(err, "TLS with system trust must work without custom certificate files")
	s.Require().Equal(uint16(tls.VersionTLS12), settings.MinVersion, "the helper migration must preserve the TLS 1.2 minimum")
	s.Require().Equal(tlscfg.CipherSuites(), settings.CipherSuites, "cipher selection must use tlscfg's recommended list")
	s.Require().Nil(settings.RootCAs, "without a custom CA, TLS must use the system trust store by default")
	s.Require().False(settings.InsecureSkipVerify, "TLS must continue verifying broker certificates and hostnames")
}

func (s *SecuritySuite) TestMutualTLSHandshake() {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	s.Require().NoError(err, "a fresh key is required to exercise actual certificate loading")
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"},
		DNSNames: []string{"localhost"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage:    x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, public, private)
	s.Require().NoError(err, "the fixture certificate must support both sides of a mutual TLS handshake")
	key, err := x509.MarshalPKCS8PrivateKey(private)
	s.Require().NoError(err, "the private key must be encoded as a loadable PEM")
	certPath := filepath.Join(s.T().TempDir(), "cert.pem")
	keyPath := filepath.Join(s.T().TempDir(), "key.pem")
	s.Require().NoError(os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600), "the CA and certificate fixture must be available on disk")
	s.Require().NoError(os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}), 0600), "the private key fixture must be available on disk")
	settings, err := tlsConfig(&config.TLS{Enabled: true, CAFile: certPath, CertFile: certPath, KeyFile: keyPath})
	s.Require().NoError(err, "custom CA and client credentials must load together")
	settings.ServerName = "localhost"
	serverConfig := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: settings.Certificates,
		ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: settings.RootCAs}
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	s.Require().NoError(left.SetDeadline(time.Now().Add(5*time.Second)), "a broken TLS handshake must not hang the suite")
	s.Require().NoError(right.SetDeadline(time.Now().Add(5*time.Second)), "both ends need a bounded handshake")
	server := tls.Server(left, serverConfig)
	result := make(chan error, 1)
	go func() { result <- server.HandshakeContext(s.T().Context()) }()
	client := tls.Client(right, settings)
	s.Require().NoError(client.HandshakeContext(s.T().Context()), "the client must trust the configured CA and send its certificate")
	s.Require().NoError(<-result, "the server must verify the client certificate, not merely accept encrypted transport")
	s.Require().Len(server.ConnectionState().VerifiedChains, 1, "mTLS must produce a verified client identity")
}

func (s *SecuritySuite) TestRejectsBadTLSSettings() {
	s.Run("certificate without key", func() {
		_, err := tlsConfig(&config.TLS{CertFile: "client.pem"})
		s.Require().ErrorContains(err, "both cert and key paths must be specified", "incomplete mTLS must fail before a connection is opened")
	})
	s.Run("missing certificate files", func() {
		_, err := tlsConfig(&config.TLS{CertFile: filepath.Join(s.T().TempDir(), "absent.pem"), KeyFile: "absent.key"})
		s.Require().ErrorContains(err, "unable to read cert", "missing client credentials must not silently disable mTLS")
	})
	s.Run("invalid CA", func() {
		path := filepath.Join(s.T().TempDir(), "ca.pem")
		s.Require().NoError(os.WriteFile(path, []byte("not a certificate"), 0600), "the invalid CA fixture must exist")
		_, err := tlsConfig(&config.TLS{CAFile: path})
		s.Require().ErrorContains(err, "no cert could be found", "invalid custom trust must fail even when system roots exist")
	})
}

func (s *SecuritySuite) TestSASLAuthorizationIdentity() {
	s.Run("PLAIN sends zid", func() {
		mechanism, err := saslMechanism(&config.SASL{Mechanism: config.MechanismPlain, Zid: "delegate", User: "alice", Password: "secret"})
		s.Require().NoError(err, "PLAIN must support a separate authorization identity")
		_, initial, err := mechanism.Authenticate(s.T().Context(), "broker")
		s.Require().NoError(err, "PLAIN must create the initial authentication payload")
		s.Require().Equal("delegate\x00alice\x00secret", string(initial), "zid must reach the wire rather than being discarded during configuration")
	})
	s.Run("SCRAM sends token flag", func() {
		mechanism, err := saslMechanism(&config.SASL{Mechanism: config.MechanismScramSHA512, Zid: "delegate", User: "token", Password: "secret", IsToken: true})
		s.Require().NoError(err, "SCRAM must support delegation token credentials")
		_, initial, err := mechanism.Authenticate(s.T().Context(), "broker")
		s.Require().NoError(err, "SCRAM must create its initial authentication payload")
		s.Require().Contains(string(initial), "a=delegate", "SCRAM authorization identity must reach the handshake")
		s.Require().Contains(string(initial), "tokenauth=true", "delegation tokens require the token authentication attribute")
	})
}
