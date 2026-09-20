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
				SASL:    &config.SASL{Mechanism: "not-a-mechanism", User: "x", Password: "y"},
			},
		},
	})

	s.Require().Error(err,
		"a cluster that cannot even be constructed is a configuration mistake, not an outage, and must stop the server rather than fail on the first tool call")
}
