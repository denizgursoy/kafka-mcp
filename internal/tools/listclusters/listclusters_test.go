package listclusters_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/denizgursoy/kafka-mcp/internal/domain/config"
	"github.com/denizgursoy/kafka-mcp/internal/domain/kafkaclient"
	"github.com/denizgursoy/kafka-mcp/internal/domain/testenv"
	"github.com/denizgursoy/kafka-mcp/internal/tools/listclusters"
)

type ListClustersSuite struct {
	suite.Suite

	env *testenv.Environment
}

func TestListClustersSuite(t *testing.T) {
	suite.Run(t, new(ListClustersSuite))
}

func (s *ListClustersSuite) SetupSuite() {
	s.env = testenv.Start(s.T())
}

func (s *ListClustersSuite) TearDownSuite() {
	s.env.Stop()
}

// registry builds a roster with one reachable cluster and one that is not.
func (s *ListClustersSuite) registry() *kafkaclient.Registry {
	s.T().Helper()

	registry, err := kafkaclient.NewRegistry(&config.Config{
		Clusters: map[string]*config.Cluster{
			"prod":    {Name: "prod", Brokers: []string{s.env.Broker()}, ReadOnly: true},
			"preprod": {Name: "preprod", Brokers: []string{s.env.Broker()}},
			"down":    {Name: "down", Brokers: []string{"127.0.0.1:1"}},
		},
	})
	s.Require().NoError(err, "building the registry must succeed")

	s.T().Cleanup(registry.Close)

	return registry
}

// byName indexes the result so a case can assert on one cluster.
func (s *ListClustersSuite) byName(out listclusters.Output) map[string]listclusters.Cluster {
	s.T().Helper()

	indexed := make(map[string]listclusters.Cluster, len(out.Clusters))

	for _, cluster := range out.Clusters {
		indexed[cluster.Name] = cluster
	}

	return indexed
}

func (s *ListClustersSuite) TestReportsEveryCluster() {
	out, err := listclusters.Run(s.T().Context(), s.registry())

	s.Require().NoError(err, "listing clusters must succeed")

	s.Run("the whole roster is returned", func() {
		s.Require().Len(out.Clusters, 3,
			"every configured cluster must be listed, because this is what makes a copy destination discoverable")
		s.Require().Equal(3, out.Count,
			"count must agree with the list it describes")
	})

	s.Run("names are sorted", func() {
		names := make([]string, 0, len(out.Clusters))
		for _, cluster := range out.Clusters {
			names = append(names, cluster.Name)
		}

		s.Require().IsIncreasing(names,
			"clusters must be sorted, because Go map order is random and an unstable list confuses a caller comparing two calls")
	})
}

func (s *ListClustersSuite) TestReportsConnectivity() {
	out, err := listclusters.Run(s.T().Context(), s.registry())

	s.Require().NoError(err, "listing clusters must succeed")

	indexed := s.byName(out)

	s.Run("a reachable cluster is connected", func() {
		s.Require().True(indexed["prod"].Connected,
			"a broker that answers must be reported as connected, or a caller cannot tell a configuration problem from an outage")
	})

	s.Run("an unreachable cluster is not", func() {
		s.Require().False(indexed["down"].Connected,
			"an unreachable cluster must be reported honestly: claiming it is up is precisely wrong when someone is asking why a call failed")
	})
}

func (s *ListClustersSuite) TestReportsWhetherWritesAreAllowed() {
	out, err := listclusters.Run(s.T().Context(), s.registry())

	s.Require().NoError(err, "listing clusters must succeed")

	indexed := s.byName(out)

	s.Require().True(indexed["prod"].ReadOnly,
		"a read-only cluster must say so, so a caller does not choose it as a copy destination and meet a refusal")
	s.Require().False(indexed["preprod"].ReadOnly,
		"a writable cluster must say so, because that is what makes it usable as a copy destination")
}

func (s *ListClustersSuite) TestRevealsNoConnectionDetails() {
	out, err := listclusters.Run(s.T().Context(), s.registry())

	s.Require().NoError(err, "listing clusters must succeed")

	// Every endpoint exposes this tool, so a session bound to one cluster can
	// see that the others exist. It must learn nothing about how to reach them.
	rendered := s.render(out)

	s.Require().NotContains(rendered, s.env.Broker(),
		"broker addresses must never be reported: this tool is reachable from every endpoint, so it must not hand one cluster's session the means to reach another")
	s.Require().NotContains(rendered, "sasl",
		"no credential or principal may appear, for the same reason")
}

// render serialises the output the way an MCP client receives it, so a case
// can assert on everything that crosses the wire rather than field by field.
func (s *ListClustersSuite) render(out listclusters.Output) string {
	s.T().Helper()

	encoded, err := json.Marshal(out)
	s.Require().NoError(err, "the output must be serialisable, since it is returned over MCP as JSON")

	return strings.ToLower(string(encoded))
}
