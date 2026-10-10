package kafkaclient

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"

	"github.com/denizgursoy/kafka-mcp/internal/domain/config"
)

const defaultOAuthTimeout = 10 * time.Second

func oauthMechanism(settings *config.SASLOAuth) (sasl.Mechanism, error) {
	if err := settings.Validate(); err != nil {
		return nil, err
	}
	zid, extensions := settings.Zid, maps.Clone(settings.Extensions)
	if settings.Token != "" {
		return (oauth.Auth{Token: settings.Token, Zid: zid, Extensions: extensions}).AsMechanism(), nil
	}
	timeout := settings.Timeout
	if timeout == 0 {
		timeout = defaultOAuthTimeout
	}

	if settings.GCP.Enabled {
		gcp := settings.GCP
		if gcp.Proxy == "" {
			gcp.Proxy = settings.Proxy
		}
		source, err := newGCPTokenSource(gcp, timeout)
		if err != nil {
			return nil, fmt.Errorf("oauth: %w", err)
		}
		return oauth.Oauth(func(ctx context.Context) (oauth.Auth, error) {
			token, err := source.Token(ctx)
			if err != nil {
				return oauth.Auth{}, fmt.Errorf("oauth: %w", err)
			}
			return oauth.Auth{Token: token, Zid: zid, Extensions: extensions}, nil
		}), nil
	}

	var httpClient *mtlsClient
	if settings.TLS.IsSet() || settings.Proxy != "" {
		var err error
		if httpClient, err = newMTLSClient(settings.TLS, settings.Proxy); err != nil {
			return nil, fmt.Errorf("oauth: tls: %w", err)
		}
	}

	credentials := clientcredentials.Config{
		TokenURL: settings.TokenURL, ClientID: settings.ClientID, ClientSecret: settings.ClientSecret,
		Scopes: append([]string(nil), settings.Scopes...),
	}
	if settings.ClientSecret == "" {
		// RFC 8705 tls_client_auth: the certificate authenticates the client,
		// and client_id travels in the body.
		credentials.AuthStyle = oauth2.AuthStyleInParams
	}

	// The mechanism is shared by the main client and independent readers.
	// Serialize refreshes while allowing waiting authentications to cancel.
	gate := make(chan struct{}, 1)
	var cached *oauth2.Token
	return oauth.Oauth(func(ctx context.Context) (oauth.Auth, error) {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		select {
		case gate <- struct{}{}:
			defer func() { <-gate }()
		case <-ctx.Done():
			return oauth.Auth{}, ctx.Err()
		}
		if err := ctx.Err(); err != nil {
			return oauth.Auth{}, err
		}
		if !cached.Valid() {
			if httpClient != nil {
				client, err := httpClient.Client()
				if err != nil {
					return oauth.Auth{}, fmt.Errorf("oauth: tls: %w", err)
				}
				ctx = context.WithValue(ctx, oauth2.HTTPClient, client)
			}
			token, err := credentials.Token(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return oauth.Auth{}, ctx.Err()
				}
				// Identity-provider error bodies can echo credentials or tokens;
				// never propagate those bodies into tool errors or server logs.
				var response *oauth2.RetrieveError
				if errors.As(err, &response) && response.Response != nil {
					return oauth.Auth{}, fmt.Errorf("oauth: token endpoint returned HTTP %d", response.Response.StatusCode)
				}
				return oauth.Auth{}, fmt.Errorf("oauth: token request failed")
			}
			cached = token
		}
		return oauth.Auth{Token: cached.AccessToken, Zid: zid, Extensions: extensions}, nil
	}), nil
}
