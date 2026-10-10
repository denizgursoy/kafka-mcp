package serde_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/denizgursoy/kafka-mcp/internal/domain/config"
	"github.com/denizgursoy/kafka-mcp/internal/domain/serde"
)

type RegistryTLSSuite struct {
	suite.Suite

	server *httptest.Server
}

func TestRegistryTLSSuite(t *testing.T) {
	suite.Run(t, new(RegistryTLSSuite))
}

// SetupSuite starts a registry behind a self-signed certificate that no trust
// store holds, serving one subject with one version.
func (s *RegistryTLSSuite) SetupSuite() {
	mux := http.NewServeMux()
	mux.HandleFunc("/subjects/orders-value/versions/latest", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
		fmt.Fprint(w, `{"subject":"orders-value","version":1,"id":7,"schema":"{\"type\":\"string\"}"}`)
	})
	mux.HandleFunc("/subjects/orders-value/versions", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
		fmt.Fprint(w, `[1]`)
	})
	s.server = httptest.NewTLSServer(mux)
}

func (s *RegistryTLSSuite) TearDownSuite() {
	s.server.Close()
}

func (s *RegistryTLSSuite) codec(insecure bool) *serde.Codec {
	codec, err := serde.New(&config.Cluster{
		Name: "tls",
		SchemaRegistry: &config.SchemaRegistry{
			URLs: []string{s.server.URL},
			TLS:  &config.TLS{Enabled: true, InsecureSkipVerify: insecure},
		},
	})
	s.Require().NoError(err, "a registry with TLS must build without certificate files")

	return codec
}

func (s *RegistryTLSSuite) TestVerifiesByDefault() {
	_, err := s.codec(false).LookupSubject(s.T().Context(), "orders-value", 0)
	s.Require().Error(err, "a registry with an untrusted certificate must be refused unless verification is disabled")
}

func (s *RegistryTLSSuite) TestInsecureSkipVerify() {
	found, err := s.codec(true).LookupSubject(s.T().Context(), "orders-value", 0)
	s.Require().NoError(err, "insecure_skip_verify must let the registry client accept a self-signed certificate")
	s.Require().Equal(7, found.ID, "the schema must be read from the registry it reached")
}
