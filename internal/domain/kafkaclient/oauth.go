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

func oauthMechanism(settings *config.SASLOAuth) (sasl.Mechanism, error) {
	if err := settings.Validate(); err != nil {
		return nil, err
	}
	zid, extensions := settings.Zid, maps.Clone(settings.Extensions)
	if settings.Token != "" {
		return (oauth.Auth{Token: settings.Token, Zid: zid, Extensions: extensions}).AsMechanism(), nil
	}
	credentials := clientcredentials.Config{
		TokenURL: settings.TokenURL, ClientID: settings.ClientID, ClientSecret: settings.ClientSecret,
		Scopes: append([]string(nil), settings.Scopes...),
	}
	timeout := settings.Timeout
	if timeout == 0 {
		timeout = 10 * time.Second
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
