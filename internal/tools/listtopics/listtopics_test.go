package listtopics_test

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/denizgursoy/kafka-mcp/internal/domain/testenv"
	"github.com/denizgursoy/kafka-mcp/internal/tools/listtopics"
)

type ListTopicsSuite struct {
	suite.Suite

	env *testenv.Environment
}

func TestListTopicsSuite(t *testing.T) {
	suite.Run(t, new(ListTopicsSuite))
}

func (s *ListTopicsSuite) SetupSuite() {
	s.env = testenv.Start(s.T())
}

func (s *ListTopicsSuite) TearDownSuite() {
	s.env.Stop()
}

func (s *ListTopicsSuite) TestReturnsAllTopics() {
	topics := s.env.CreateTopics(s.T(), "all-orders", "all-payments")

	out, err := listtopics.Run(
		s.T().Context(),
		s.env.Admin(),
		listtopics.Input{},
	)

	s.Require().NoError(err, "listing every topic on a healthy broker must succeed")
	s.Require().Equal(len(out.Topics), out.Count,
		"count must report how many topics were returned")
	s.Require().Contains(out.Topics, topics[0],
		"an unfiltered listing must include every created topic")
	s.Require().Contains(out.Topics, topics[1],
		"an unfiltered listing must include every created topic")
	s.Require().IsIncreasing(out.Topics,
		"topics must be sorted, because kadm returns a map and Go map order is random")
}

func (s *ListTopicsSuite) TestFiltersBySearchCaseInsensitively() {
	topics := s.env.CreateTopics(s.T(), "filter-ORDERS-created", "filter-payments-created")

	out, err := listtopics.Run(
		s.T().Context(),
		s.env.Admin(),
		listtopics.Input{Search: "orders"},
	)

	s.Require().NoError(err, "filtering topics on a healthy broker must succeed")
	s.Require().Contains(out.Topics, topics[0],
		"search must match regardless of case, so lowercase 'orders' must match 'ORDERS'")
	s.Require().NotContains(out.Topics, topics[1],
		"topics that do not contain the search text must be filtered out")
	s.Require().Equal(len(out.Topics), out.Count,
		"count must report how many topics were returned")
}

func (s *ListTopicsSuite) TestReturnsEmptySliceWhenNothingMatches() {
	s.env.CreateTopic(s.T(), "empty-orders")

	out, err := listtopics.Run(
		s.T().Context(),
		s.env.Admin(),
		listtopics.Input{Search: "no-such-topic-anywhere"},
	)

	s.Require().NoError(err, "a search matching nothing is not an error")
	s.Require().NotNil(out.Topics,
		"topics must be an empty slice, not nil, so the JSON output is [] and not null")
	s.Require().Empty(out.Topics,
		"no topic contains the search text, so none may be returned")
	s.Require().Zero(out.Count,
		"count must be zero when no topic matches")
}

func (s *ListTopicsSuite) TestReturnsErrorWhenBrokerUnreachable() {
	client, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
	s.Require().NoError(err, "building a client against a dead address must not fail yet")

	s.T().Cleanup(client.Close)

	_, err = listtopics.Run(s.T().Context(), kadm.NewClient(client), listtopics.Input{})

	s.Require().Error(err,
		"an unreachable broker must surface as an error, not as an empty topic list")
}
