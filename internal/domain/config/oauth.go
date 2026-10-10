package config

import (
	"fmt"
	"net/url"
	"time"
)

// GCP token formats.
const (
	// GCPFormatRaw sends the Google access token as-is.
	GCPFormatRaw = "raw"
	// GCPFormatManagedKafka wraps the token in the GOOG_OAUTH2_TOKEN envelope
	// expected by Google Managed Service for Apache Kafka.
	GCPFormatManagedKafka = "managed_kafka"
)

// SASLOAuth follows wkafka's file-based OAuth settings. Tokens, client
// secrets, inline keys and GCP credentials support the same {env:VAR}
// substitution as SASL passwords.
//
// Exactly one token source is configured: a static token, client credentials
// (optionally authenticated with a client certificate), or GCP workload
// identity federation.
type SASLOAuth struct {
	Enabled      bool              `cfg:"enabled"`
	Zid          string            `cfg:"zid"`
	Extensions   map[string]string `cfg:"extensions"`
	Token        string            `cfg:"token" log:"false"`
	TokenURL     string            `cfg:"token_url"`
	ClientID     string            `cfg:"client_id"`
	ClientSecret string            `cfg:"client_secret" log:"false"`
	Scopes       []string          `cfg:"scopes"`
	Timeout      time.Duration     `cfg:"timeout"`

	// TLS is the client certificate and trust used for the token endpoint in
	// client-credentials mode. With a client certificate, client_secret is
	// optional (RFC 8705 tls_client_auth). It never applies to the brokers.
	TLS OAuthTLS `cfg:"tls"`

	// Proxy is used for the token endpoint: http, https, socks5 or socks5h.
	// Empty means the HTTPS_PROXY, HTTP_PROXY and NO_PROXY environment.
	Proxy string `cfg:"proxy" log:"false"`

	// GCP fetches tokens from Google Cloud with X.509 workload identity
	// federation.
	GCP SASLOAuthGCP `cfg:"gcp"`
}

// OAuthTLS is the client certificate presented to a token endpoint. Each
// value is given inline as PEM or as a file, not both. Files are re-read on
// every token refresh, so rotated certificates apply without a restart.
type OAuthTLS struct {
	// InsecureSkipVerify disables the token endpoint's certificate and
	// hostname verification. Unsafe outside a test environment.
	InsecureSkipVerify bool   `cfg:"insecure_skip_verify"`
	Cert               string `cfg:"cert"`
	CertFile           string `cfg:"cert_file"`
	Key                string `cfg:"key" log:"false"`
	KeyFile            string `cfg:"key_file"`
	CA                 string `cfg:"ca"`
	CAFile             string `cfg:"ca_file"`
}

// IsSet reports whether any TLS option is configured.
func (t OAuthTLS) IsSet() bool {
	return t.InsecureSkipVerify || t.Cert != "" || t.CertFile != "" || t.Key != "" ||
		t.KeyFile != "" || t.CA != "" || t.CAFile != ""
}

// HasCertificate reports whether a client certificate is configured in
// either form.
func (t OAuthTLS) HasCertificate() bool {
	return t.Cert != "" || t.CertFile != ""
}

func (t OAuthTLS) validate() error {
	if err := inlineOrFile("cert", t.Cert, t.CertFile); err != nil {
		return err
	}
	if err := inlineOrFile("key", t.Key, t.KeyFile); err != nil {
		return err
	}
	if err := inlineOrFile("ca", t.CA, t.CAFile); err != nil {
		return err
	}
	if t.HasCertificate() != (t.Key != "" || t.KeyFile != "") {
		return fmt.Errorf("cert and key must be provided together")
	}

	return nil
}

// SASLOAuthGCP obtains Google Cloud access tokens with workload identity
// federation using an X.509 client certificate, optionally impersonating a
// service account.
type SASLOAuthGCP struct {
	Enabled bool `cfg:"enabled"`

	// Credentials is the "external_account" JSON. Its credential_source is
	// ignored, because the certificate below is used instead.
	Credentials     string `cfg:"credentials" log:"false"`
	CredentialsFile string `cfg:"credentials_file"`

	// Audience, TokenURL and ServiceAccountImpersonationURL override the
	// credentials.
	Audience                       string `cfg:"audience"`
	TokenURL                       string `cfg:"token_url"`
	ServiceAccountImpersonationURL string `cfg:"service_account_impersonation_url"`

	// Cert is the PEM client certificate, chain allowed with the leaf first.
	Cert     string `cfg:"cert"`
	CertFile string `cfg:"cert_file"`
	Key      string `cfg:"key" log:"false"`
	KeyFile  string `cfg:"key_file"`
	// CA is optional PEM roots for the Google endpoints; system roots if empty.
	CA     string `cfg:"ca"`
	CAFile string `cfg:"ca_file"`

	// InsecureSkipVerify disables certificate and hostname verification of
	// the Google endpoints. Unsafe outside a test environment.
	InsecureSkipVerify bool `cfg:"insecure_skip_verify"`

	// Proxy for the Google endpoints. Empty falls back to oauth.proxy, then to
	// the proxy environment variables.
	Proxy string `cfg:"proxy" log:"false"`

	// Scopes default to cloud-platform.
	Scopes []string `cfg:"scopes"`
	// Lifetime of the impersonated token, default 1h.
	Lifetime time.Duration `cfg:"lifetime"`
	// RefreshBefore refreshes this long before expiry, default 5m.
	RefreshBefore time.Duration `cfg:"refresh_before"`
	// Format is "raw" (default) or "managed_kafka".
	Format string `cfg:"format"`
}

// TLS returns the certificate settings used to reach the Google endpoints.
func (g SASLOAuthGCP) TLS() OAuthTLS {
	return OAuthTLS{
		InsecureSkipVerify: g.InsecureSkipVerify,
		Cert:               g.Cert, CertFile: g.CertFile,
		Key: g.Key, KeyFile: g.KeyFile,
		CA: g.CA, CAFile: g.CAFile,
	}
}

func (g SASLOAuthGCP) validate() error {
	if err := inlineOrFile("credentials", g.Credentials, g.CredentialsFile); err != nil {
		return fmt.Errorf("gcp: %w", err)
	}
	if g.Credentials == "" && g.CredentialsFile == "" && g.Audience == "" {
		return fmt.Errorf("gcp: audience or credentials is required")
	}
	tls := g.TLS()
	if err := tls.validate(); err != nil {
		return fmt.Errorf("gcp: %w", err)
	}
	if !tls.HasCertificate() {
		return fmt.Errorf("gcp: cert and key are required")
	}
	for _, u := range []string{g.TokenURL, g.ServiceAccountImpersonationURL} {
		if u != "" && !httpURL(u) {
			return fmt.Errorf("gcp: %q must be an absolute HTTP or HTTPS URL", u)
		}
	}
	if g.Lifetime < 0 || g.RefreshBefore < 0 {
		return fmt.Errorf("gcp: lifetime and refresh_before must not be negative")
	}
	switch g.Format {
	case "", GCPFormatRaw, GCPFormatManagedKafka:
	default:
		return fmt.Errorf("gcp: unknown format %q, must be %s or %s", g.Format, GCPFormatRaw, GCPFormatManagedKafka)
	}
	if err := validateProxy(g.Proxy); err != nil {
		return fmt.Errorf("gcp: %w", err)
	}

	return nil
}

// Validate rejects ambiguous or incomplete OAuth settings before connecting.
// Files are not read here; that happens when the mechanism is built.
func (o *SASLOAuth) Validate() error {
	if o.Timeout < 0 {
		return fmt.Errorf("oauth: timeout must not be negative")
	}
	credentials := o.TokenURL != "" || o.ClientID != "" || o.ClientSecret != "" || len(o.Scopes) > 0 || o.TLS.IsSet()
	modes := 0
	for _, set := range []bool{o.Token != "", credentials, o.GCP.Enabled} {
		if set {
			modes++
		}
	}
	if modes != 1 {
		return fmt.Errorf("oauth: configure exactly one of token, client credentials or gcp")
	}
	if err := validateProxy(o.Proxy); err != nil {
		return fmt.Errorf("oauth: %w", err)
	}
	if o.Token != "" {
		return nil
	}
	if o.GCP.Enabled {
		if err := o.GCP.validate(); err != nil {
			return fmt.Errorf("oauth: %w", err)
		}

		return nil
	}
	if err := o.TLS.validate(); err != nil {
		return fmt.Errorf("oauth: tls: %w", err)
	}
	if o.TokenURL == "" || o.ClientID == "" || (o.ClientSecret == "" && !o.TLS.HasCertificate()) {
		return fmt.Errorf("oauth: token_url, client_id and client_secret (or a tls client certificate) are required")
	}
	if !httpURL(o.TokenURL) {
		return fmt.Errorf("oauth: token_url must be an absolute HTTP or HTTPS URL")
	}

	return nil
}

func resolveOAuth(o *SASLOAuth, name string) error {
	for _, secret := range []*string{&o.Token, &o.ClientSecret, &o.TLS.Key, &o.GCP.Credentials, &o.GCP.Key} {
		resolved, err := interpolate(*secret, name)
		if err != nil {
			return err
		}
		*secret = resolved
	}

	return o.Validate()
}

func inlineOrFile(name, inline, file string) error {
	if inline != "" && file != "" {
		return fmt.Errorf("%s and %s_file cannot be used together", name, name)
	}

	return nil
}

func httpURL(value string) bool {
	u, err := url.Parse(value)

	return err == nil && u.Host != "" && (u.Scheme == "https" || u.Scheme == "http")
}

func validateProxy(value string) error {
	if value == "" {
		return nil
	}
	u, err := url.Parse(value)
	if err != nil || u.Host == "" ||
		(u.Scheme != "http" && u.Scheme != "https" && u.Scheme != "socks5" && u.Scheme != "socks5h") {
		return fmt.Errorf("proxy must be an absolute http, https, socks5 or socks5h URL")
	}

	return nil
}
