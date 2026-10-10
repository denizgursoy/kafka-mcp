package kafkaclient

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"

	"github.com/denizgursoy/kafka-mcp/internal/domain/config"
)

// mtlsClient builds the HTTP client used for token endpoints, presenting a
// client certificate when one is configured. Files are re-read on every call
// to Client, and the client is rebuilt only when their content changed, so a
// rotated certificate applies without a restart.
//
// It is not safe for concurrent use; the token sources serialize access.
type mtlsClient struct {
	cfg    config.OAuthTLS
	proxy  *url.URL
	client *http.Client
}

// newMTLSClient builds the client. An empty proxy uses the HTTPS_PROXY,
// HTTP_PROXY and NO_PROXY environment variables.
func newMTLSClient(settings config.OAuthTLS, proxy string) (*mtlsClient, error) {
	for _, pair := range []struct{ name, inline, file string }{
		{"cert", settings.Cert, settings.CertFile},
		{"key", settings.Key, settings.KeyFile},
		{"ca", settings.CA, settings.CAFile},
	} {
		if pair.inline != "" && pair.file != "" {
			return nil, fmt.Errorf("%s and %s_file cannot be used together", pair.name, pair.name)
		}
	}

	m := &mtlsClient{cfg: settings}

	if proxy != "" {
		u, err := url.Parse(proxy)
		if err != nil || u.Host == "" ||
			(u.Scheme != "http" && u.Scheme != "https" && u.Scheme != "socks5" && u.Scheme != "socks5h") {
			return nil, errors.New("proxy must be an absolute http, https, socks5 or socks5h URL")
		}
		m.proxy = u
	}

	if _, err := m.reload(); err != nil {
		return nil, err
	}
	if (m.cfg.Cert == "") != (m.cfg.Key == "") {
		return nil, errors.New("cert and key must be provided together")
	}
	if err := m.build(); err != nil {
		return nil, err
	}

	return m, nil
}

// reload re-reads the configured files and reports whether content changed.
func (m *mtlsClient) reload() (bool, error) {
	changed := false

	for _, file := range []struct {
		path string
		dst  *string
	}{
		{m.cfg.CertFile, &m.cfg.Cert},
		{m.cfg.KeyFile, &m.cfg.Key},
		{m.cfg.CAFile, &m.cfg.CA},
	} {
		if file.path == "" {
			continue
		}

		contents, err := os.ReadFile(filepath.Clean(file.path))
		if err != nil {
			return false, err
		}

		if string(contents) != *file.dst {
			*file.dst = string(contents)
			changed = true
		}
	}

	return changed, nil
}

func (m *mtlsClient) build() error {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: m.cfg.InsecureSkipVerify}

	if m.cfg.Cert != "" {
		cert, err := tls.X509KeyPair([]byte(m.cfg.Cert), []byte(m.cfg.Key))
		if err != nil {
			return fmt.Errorf("load cert/key: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	if m.cfg.CA != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(m.cfg.CA)) {
			return errors.New("no valid certificate in ca")
		}
		tlsConfig.RootCAs = pool
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsConfig
	if m.proxy != nil {
		transport.Proxy = http.ProxyURL(m.proxy)
	}

	if m.client != nil {
		m.client.CloseIdleConnections()
	}
	m.client = &http.Client{Transport: transport}

	return nil
}

// Client returns the current HTTP client, rebuilding it if files changed.
func (m *mtlsClient) Client() (*http.Client, error) {
	changed, err := m.reload()
	if err != nil {
		return nil, err
	}

	if changed {
		if err := m.build(); err != nil {
			return nil, err
		}
	}

	return m.client, nil
}

// CertPEM returns the current client certificate PEM.
func (m *mtlsClient) CertPEM() string {
	return m.cfg.Cert
}
