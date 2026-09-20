package config

import (
	"fmt"
	"net/url"
	"time"
)

// SASLOAuth follows wkafka's file-based OAuth settings. Tokens and client
// secrets support the same {env:VAR} substitution as SASL passwords.
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
}

// Validate rejects ambiguous or incomplete OAuth settings before connecting.
func (o *SASLOAuth) Validate() error {
	if o.Timeout < 0 {
		return fmt.Errorf("oauth: timeout must not be negative")
	}
	credentials := o.TokenURL != "" || o.ClientID != "" || o.ClientSecret != "" || len(o.Scopes) > 0
	if (o.Token != "") == credentials {
		return fmt.Errorf("oauth: configure exactly one of token or client credentials")
	}
	if o.Token != "" {
		return nil
	}
	if o.TokenURL == "" || o.ClientID == "" || o.ClientSecret == "" {
		return fmt.Errorf("oauth: token_url, client_id and client_secret are required")
	}
	u, err := url.Parse(o.TokenURL)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
		return fmt.Errorf("oauth: token_url must be an absolute HTTP or HTTPS URL")
	}
	return nil
}

func resolveOAuth(o *SASLOAuth, name string) error {
	var err error
	if o.Token, err = interpolate(o.Token, name); err != nil {
		return err
	}
	if o.ClientSecret, err = interpolate(o.ClientSecret, name); err != nil {
		return err
	}
	return o.Validate()
}
