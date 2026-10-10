package kafkaclient

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/denizgursoy/kafka-mcp/internal/domain/config"
)

const (
	gcpGrantTypeTokenExchange = "urn:ietf:params:oauth:grant-type:token-exchange"
	gcpTokenTypeAccessToken   = "urn:ietf:params:oauth:token-type:access_token"
	gcpTokenTypeMTLS          = "urn:ietf:params:oauth:token-type:mtls"
	gcpScopeCloudPlatform     = "https://www.googleapis.com/auth/cloud-platform"
	gcpDefaultTokenURL        = "https://sts.mtls.googleapis.com/v1/token"
)

type gcpExternalAccount struct {
	Type                           string `json:"type"`
	Audience                       string `json:"audience"`
	SubjectTokenType               string `json:"subject_token_type"`
	TokenURL                       string `json:"token_url"`
	ServiceAccountImpersonationURL string `json:"service_account_impersonation_url"`
}

type gcpToken struct {
	accessToken string
	expiry      time.Time
}

// gcpTokenSource exchanges an X.509 client certificate for a Google access
// token through STS, then optionally impersonates a service account. Tokens
// are cached and shared by every connection to the cluster.
type gcpTokenSource struct {
	cfg     config.SASLOAuthGCP
	timeout time.Duration
	now     func() time.Time

	mu     sync.Mutex
	mtls   *mtlsClient
	client *http.Client
	cached *gcpToken
}

func newGCPTokenSource(settings config.SASLOAuthGCP, timeout time.Duration) (*gcpTokenSource, error) {
	if settings.Credentials != "" && settings.CredentialsFile != "" {
		return nil, errors.New("gcp: credentials and credentials_file cannot be used together")
	}

	if settings.CredentialsFile != "" {
		contents, err := os.ReadFile(filepath.Clean(settings.CredentialsFile))
		if err != nil {
			return nil, fmt.Errorf("gcp: %w", err)
		}
		settings.Credentials = string(contents)
	}

	if settings.Credentials != "" {
		var account gcpExternalAccount
		if err := json.Unmarshal([]byte(settings.Credentials), &account); err != nil {
			return nil, fmt.Errorf("gcp: parse credentials: %w", err)
		}
		if account.Type != "" && account.Type != "external_account" {
			return nil, fmt.Errorf("gcp: unsupported credentials type %q", account.Type)
		}
		if account.SubjectTokenType != "" && account.SubjectTokenType != gcpTokenTypeMTLS {
			return nil, fmt.Errorf("gcp: unsupported subject_token_type %q, only %s",
				account.SubjectTokenType, gcpTokenTypeMTLS)
		}
		settings.Audience = firstNonEmpty(settings.Audience, account.Audience)
		settings.TokenURL = firstNonEmpty(settings.TokenURL, account.TokenURL)
		settings.ServiceAccountImpersonationURL = firstNonEmpty(
			settings.ServiceAccountImpersonationURL, account.ServiceAccountImpersonationURL)
	}

	if settings.Audience == "" {
		return nil, errors.New("gcp: audience is required")
	}

	settings.TokenURL = firstNonEmpty(settings.TokenURL, gcpDefaultTokenURL)

	for _, endpoint := range []string{settings.TokenURL, settings.ServiceAccountImpersonationURL} {
		if endpoint == "" {
			continue
		}
		if u, err := url.Parse(endpoint); err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
			return nil, fmt.Errorf("gcp: %q must be an absolute HTTP or HTTPS URL", endpoint)
		}
	}

	if len(settings.Scopes) == 0 {
		settings.Scopes = []string{gcpScopeCloudPlatform}
	}
	if settings.Lifetime < 0 || settings.RefreshBefore < 0 {
		return nil, errors.New("gcp: lifetime and refresh_before must not be negative")
	}
	if settings.Lifetime == 0 {
		settings.Lifetime = time.Hour
	}
	if settings.RefreshBefore == 0 {
		settings.RefreshBefore = 5 * time.Minute
	}

	switch settings.Format {
	case "":
		settings.Format = config.GCPFormatRaw
	case config.GCPFormatRaw:
	case config.GCPFormatManagedKafka:
		if gcpServiceAccountEmail(settings.ServiceAccountImpersonationURL) == "" {
			return nil, errors.New("gcp: managed_kafka format requires service_account_impersonation_url")
		}
	default:
		return nil, fmt.Errorf("gcp: unknown format %q", settings.Format)
	}

	client, err := newMTLSClient(settings.TLS(), settings.Proxy)
	if err != nil {
		return nil, fmt.Errorf("gcp: %w", err)
	}
	if client.CertPEM() == "" {
		return nil, errors.New("gcp: cert and key are required")
	}

	source := &gcpTokenSource{cfg: settings, timeout: timeout, now: time.Now, mtls: client}
	if _, err := source.subjectToken(); err != nil {
		return nil, err
	}

	return source, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}

	return ""
}

// subjectToken is a JSON array of base64 DER certificates, leaf first.
func (s *gcpTokenSource) subjectToken() (string, error) {
	var certs []string

	rest := []byte(s.mtls.CertPEM())
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type == "CERTIFICATE" {
			certs = append(certs, base64.StdEncoding.EncodeToString(block.Bytes))
		}
	}

	if len(certs) == 0 {
		return "", errors.New("gcp: no certificate found in cert")
	}

	encoded, err := json.Marshal(certs)

	return string(encoded), err
}

func gcpServiceAccountEmail(impersonationURL string) string {
	const marker = "serviceAccounts/"

	i := strings.LastIndex(impersonationURL, marker)
	if i < 0 {
		return ""
	}

	email := impersonationURL[i+len(marker):]
	if j := strings.Index(email, ":"); j >= 0 {
		email = email[:j]
	}

	return email
}

// Token returns the formatted token, refreshing it when close to expiry. A
// failed refresh keeps using the cached token until it actually expires, so
// a brief Google outage does not break new connections.
func (s *gcpTokenSource) Token(ctx context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return "", err
	}

	if s.cached == nil || !s.now().Add(s.cfg.RefreshBefore).Before(s.cached.expiry) {
		fetchCtx, cancel := context.WithTimeout(ctx, s.timeout)
		token, err := s.fetch(fetchCtx)
		cancel()

		switch {
		case err == nil:
			s.cached = token
		case s.cached != nil && s.now().Before(s.cached.expiry):
			slog.Warn("gcp token refresh failed, using cached token", "error", err)
		default:
			return "", err
		}
	}

	return s.format(s.cached)
}

func (s *gcpTokenSource) fetch(ctx context.Context) (*gcpToken, error) {
	client, err := s.mtls.Client()
	if err != nil {
		return nil, fmt.Errorf("gcp: %w", err)
	}
	s.client = client

	subject, err := s.subjectToken()
	if err != nil {
		return nil, err
	}

	form := url.Values{
		"grant_type":           {gcpGrantTypeTokenExchange},
		"audience":             {s.cfg.Audience},
		"scope":                {strings.Join(s.cfg.Scopes, " ")},
		"requested_token_type": {gcpTokenTypeAccessToken},
		"subject_token":        {subject},
		"subject_token_type":   {gcpTokenTypeMTLS},
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.TokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	var sts struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
	}
	if err := s.do(request, &sts); err != nil {
		return nil, fmt.Errorf("gcp: sts exchange: %w", err)
	}
	if sts.AccessToken == "" {
		return nil, errors.New("gcp: sts exchange: empty access_token")
	}

	if s.cfg.ServiceAccountImpersonationURL == "" {
		return &gcpToken{
			accessToken: sts.AccessToken,
			expiry:      s.now().Add(time.Duration(sts.ExpiresIn) * time.Second),
		}, nil
	}

	body, err := json.Marshal(map[string]any{
		"scope":    s.cfg.Scopes,
		"lifetime": strconv.FormatInt(int64(s.cfg.Lifetime.Seconds()), 10) + "s",
	})
	if err != nil {
		return nil, err
	}

	request, err = http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.ServiceAccountImpersonationURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+sts.AccessToken)

	var impersonated struct {
		AccessToken string    `json:"accessToken"`
		ExpireTime  time.Time `json:"expireTime"`
	}
	if err := s.do(request, &impersonated); err != nil {
		return nil, fmt.Errorf("gcp: impersonation: %w", err)
	}
	if impersonated.AccessToken == "" {
		return nil, errors.New("gcp: impersonation: empty accessToken")
	}

	return &gcpToken{accessToken: impersonated.AccessToken, expiry: impersonated.ExpireTime}, nil
}

func (s *gcpTokenSource) do(request *http.Request, v any) error {
	response, err := s.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return err
	}

	// Response bodies can echo the subject token or the access token, so only
	// the status is reported; it is what reaches tool errors and server logs.
	if response.StatusCode < 200 || response.StatusCode > 299 {
		return fmt.Errorf("endpoint returned HTTP %d", response.StatusCode)
	}

	if err := json.Unmarshal(body, v); err != nil {
		return errors.New("endpoint returned a response that is not valid JSON")
	}

	return nil
}

func (s *gcpTokenSource) format(token *gcpToken) (string, error) {
	if s.cfg.Format != config.GCPFormatManagedKafka {
		return token.accessToken, nil
	}

	encoding := base64.RawURLEncoding

	header, err := json.Marshal(map[string]string{"typ": "JWT", "alg": "GOOG_OAUTH2_TOKEN"})
	if err != nil {
		return "", err
	}

	payload, err := json.Marshal(map[string]any{
		"exp":   token.expiry.Unix(),
		"iat":   s.now().Unix(),
		"iss":   "Google",
		"scope": "kafka",
		"sub":   gcpServiceAccountEmail(s.cfg.ServiceAccountImpersonationURL),
	})
	if err != nil {
		return "", err
	}

	return encoding.EncodeToString(header) + "." + encoding.EncodeToString(payload) + "." +
		encoding.EncodeToString([]byte(token.accessToken)), nil
}
