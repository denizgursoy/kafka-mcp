package kafkaclient_test

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/denizgursoy/kafka-mcp/internal/domain/config"
	"github.com/denizgursoy/kafka-mcp/internal/domain/kafkaclient"
	"github.com/denizgursoy/kafka-mcp/internal/domain/testenv"
)

type RegistrySuite struct {
	suite.Suite

	env *testenv.Environment
}

func TestRegistrySuite(t *testing.T) {
	suite.Run(t, new(RegistrySuite))
}

func (s *RegistrySuite) SetupSuite() {
	s.env = testenv.Start(s.T())
}

func (s *RegistrySuite) TearDownSuite() {
	s.env.Stop()
}

func (s *RegistrySuite) TestHoldsEveryConfiguredCluster() {
	registry, err := kafkaclient.NewRegistry(&config.Config{
		Clusters: map[string]*config.Cluster{
			"live":    {Name: "live", Brokers: []string{s.env.Broker()}},
			"archive": {Name: "archive", Brokers: []string{s.env.Broker()}, ReadOnly: true},
		},
	})

	s.Require().NoError(err, "building clients for reachable clusters must succeed")

	s.T().Cleanup(registry.Close)

	s.Run("each cluster is reachable by name", func() {
		s.Require().NotNil(registry.Get("live"),
			"a configured cluster must be retrievable, because its endpoint depends on it")
		s.Require().NotNil(registry.Get("archive"),
			"every configured cluster must be present, not only the first")
	})

	s.Run("each client carries its own settings", func() {
		s.Require().False(registry.Get("live").Config().ReadOnly,
			"one cluster's read_only must not leak into another, or a writable cluster would refuse writes")
		s.Require().True(registry.Get("archive").Config().ReadOnly,
			"a read-only cluster must stay read-only, since that is the only protection on a cluster without ACLs")
	})

	s.Run("names are sorted", func() {
		s.Require().Equal([]string{"archive", "live"}, registry.Names(),
			"names must be sorted, because Go map order is random and list_clusters would otherwise vary between calls")
	})
}

func (s *RegistrySuite) TestUnknownClusterIsNil() {
	registry, err := kafkaclient.NewRegistry(&config.Config{
		Clusters: map[string]*config.Cluster{
			"live": {Name: "live", Brokers: []string{s.env.Broker()}},
		},
	})

	s.Require().NoError(err, "the registry must build")

	s.T().Cleanup(registry.Close)

	s.Require().Nil(registry.Get("never-configured"),
		"an unknown name must return nothing rather than a client for some other cluster, because the HTTP router turns a nil server into a 400 and a tool turns it into a clear error")
}

func (s *RegistrySuite) TestScopesPermissionsPerEndpoint() {
	registry, err := kafkaclient.NewRegistry(&config.Config{
		Clusters: map[string]*config.Cluster{
			"prod": {Name: "prod", Brokers: []string{s.env.Broker()}},
		},
		Endpoints: map[string]*config.Endpoint{
			"read":  {Name: "read", Cluster: "prod", Path: "/mcp", ReadOnly: true},
			"write": {Name: "write", Cluster: "prod", Path: "/mcp/rw"},
		},
	})
	s.Require().NoError(err,
		"one physical Kafka connection must support several endpoint policies")
	s.T().Cleanup(registry.Close)

	s.Run("read endpoint refuses", func() {
		client := registry.Endpoint("read")
		s.Require().NotNil(client,
			"the read endpoint must resolve to its cluster connection")
		s.Require().Error(client.RequireWritable("test write"),
			"endpoint read_only must be enforced at the mutation gate, not only by hiding tools")
	})

	s.Run("write endpoint permits", func() {
		client := registry.Endpoint("write")
		s.Require().NotNil(client,
			"the write endpoint must reuse the same configured cluster")
		s.Require().NoError(client.RequireWritable("test write"),
			"a writable endpoint for the same cluster must not inherit the read endpoint's narrower policy")
	})
}

func (s *RegistrySuite) TestDestinationsArePerEndpoint() {
	registry, err := kafkaclient.NewRegistry(&config.Config{
		Clusters: map[string]*config.Cluster{
			"prod":    {Name: "prod", Brokers: []string{s.env.Broker()}},
			"preprod": {Name: "preprod", Brokers: []string{s.env.Broker()}},
			"locked":  {Name: "locked", Brokers: []string{s.env.Broker()}, ReadOnly: true},
			"hidden":  {Name: "hidden", Brokers: []string{s.env.Broker()}},
		},
		Endpoints: map[string]*config.Endpoint{
			"prod-read":  {Name: "prod-read", Cluster: "prod", ReadOnly: true, Destinations: []string{"preprod", "locked"}},
			"prod-write": {Name: "prod-write", Cluster: "prod"},
			"preprod":    {Name: "preprod", Cluster: "preprod"},
			"locked":     {Name: "locked", Cluster: "locked"},
		},
	})
	s.Require().NoError(err, "the registry must build")
	s.T().Cleanup(registry.Close)

	s.Run("a listed destination is writable", func() {
		client, err := registry.Destination("prod-read", "preprod")
		s.Require().NoError(err, "a cluster the endpoint lists is one the operator chose to let it write to")
		s.Require().NoError(client.RequireWritable("copy"),
			"read_only protects the cluster written to, so a read-only endpoint may still seed a listed writable cluster")
	})
	s.Run("an unlisted destination is refused even when another endpoint can write to it", func() {
		_, err := registry.Destination("preprod", "prod")
		s.Require().ErrorContains(err, "destinations",
			"prod has a writable endpoint, but that is a different session's permission; this endpoint never listed prod")
	})
	s.Run("cluster read_only still wins over a listing", func() {
		client, err := registry.Destination("prod-read", "locked")
		s.Require().NoError(err, "a listed cluster resolves")
		s.Require().ErrorContains(client.RequireWritable("copy"), "read-only",
			"a cluster configured read_only is protected from every endpoint, whatever they list")
	})
	s.Run("an endpoint with no destinations writes nowhere else", func() {
		_, err := registry.Destination("prod-write", "preprod")
		s.Require().ErrorContains(err, "destinations",
			"absence of a list means no cross-cluster writes, so a new endpoint is narrow until widened")
	})
	s.Run("an unknown cluster is reported by name", func() {
		_, err := registry.Destination("prod-read", "nowhere")
		s.Require().ErrorContains(err, "nowhere", "a typo is the likeliest cause and must be quoted")
	})
	s.Run("writability is reported per endpoint", func() {
		s.Require().True(registry.Writable("prod-read", "preprod"), "a listed writable cluster is writable from here")
		s.Require().False(registry.Writable("prod-read", "prod"), "the endpoint's own cluster follows its read_only")
		s.Require().True(registry.Writable("prod-write", "prod"), "a writable endpoint may write its own cluster")
		s.Require().False(registry.Writable("preprod", "prod"), "an unlisted cluster is not writable from here")
		s.Require().False(registry.Writable("prod-read", "locked"), "a read_only cluster is never writable")
	})
	s.Run("a cluster no endpoint exposes is unreachable", func() {
		s.Require().Nil(registry.Exposed("hidden"),
			"a cluster defined but never given an endpoint must not be readable through another endpoint's cross-cluster tools")
		s.Require().NotNil(registry.Exposed("preprod"), "an exposed cluster stays reachable")
		s.Require().NotContains(registry.ExposedNames(), "hidden", "the roster must not reveal a cluster nobody exposed")
	})
}

func (s *RegistrySuite) TestKeepsAnUnreachableCluster() {
	registry, err := kafkaclient.NewRegistry(&config.Config{
		Clusters: map[string]*config.Cluster{
			"live": {Name: "live", Brokers: []string{s.env.Broker()}},
			"down": {Name: "down", Brokers: []string{"127.0.0.1:1"}},
		},
	})

	s.Require().NoError(err,
		"one unreachable cluster must not stop the server starting, or a single outage would block debugging every other cluster")

	s.T().Cleanup(registry.Close)

	s.Require().NotNil(registry.Get("down"),
		"an unreachable cluster must still be served, so a caller gets a connection error naming it rather than a 404 suggesting it was never configured")
	s.Require().NotNil(registry.Get("live"),
		"the reachable cluster must be unaffected by its neighbour being down")
}

func (s *RegistrySuite) TestReportsConnectivity() {
	registry, err := kafkaclient.NewRegistry(&config.Config{
		Clusters: map[string]*config.Cluster{
			"live": {Name: "live", Brokers: []string{s.env.Broker()}},
			"down": {Name: "down", Brokers: []string{"127.0.0.1:1"}},
		},
	})

	s.Require().NoError(err, "the registry must build")

	s.T().Cleanup(registry.Close)

	s.Run("a reachable cluster is connected", func() {
		s.Require().True(registry.Get("live").Connected(s.T().Context()),
			"a broker that answers must be reported as connected, or an operator cannot tell a configuration problem from an outage")
	})

	s.Run("an unreachable cluster is not", func() {
		s.Require().False(registry.Get("down").Connected(s.T().Context()),
			"a broker that cannot be reached must be reported honestly: claiming it is up is precisely wrong when someone is asking why a call failed")
	})
}

func (s *RegistrySuite) TestErrorsOnAMisconfiguredCluster() {
	_, err := kafkaclient.NewRegistry(&config.Config{
		Clusters: map[string]*config.Cluster{
			"broken": {
				Name:    "broken",
				Brokers: []string{s.env.Broker()},
				SASL:    []*config.SASL{{Mechanism: "not-a-mechanism", User: "x", Password: "y"}},
			},
		},
	})

	s.Require().Error(err,
		"a cluster that cannot even be constructed is a configuration mistake, not an outage, and must stop the server rather than fail on the first tool call")
}
