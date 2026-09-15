package listconsumergroups_test

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/denizgursoy/kafka-mcp/internal/domain/testenv"
	"github.com/denizgursoy/kafka-mcp/internal/tools/listconsumergroups"
)

type ListConsumerGroupsSuite struct {
	suite.Suite

	env *testenv.Environment
}

func TestListConsumerGroupsSuite(t *testing.T) {
	suite.Run(t, new(ListConsumerGroupsSuite))
}

func (s *ListConsumerGroupsSuite) SetupSuite() {
	s.env = testenv.Start(s.T())
}

func (s *ListConsumerGroupsSuite) TearDownSuite() {
	s.env.Stop()
}

func (s *ListConsumerGroupsSuite) names(out listconsumergroups.Output) []string {
	s.T().Helper()

	names := make([]string, 0, len(out.Groups))

	for _, group := range out.Groups {
		names = append(names, group.Group)
	}

	return names
}

func (s *ListConsumerGroupsSuite) TestListsGroupsThatCommittedOffsets() {
	topic := s.env.CreateTopic(s.T(), "groups-list")

	s.env.Produce(s.T(), topic,
		testenv.Message{Value: "one"},
		testenv.Message{Value: "two"},
	)

	group := s.env.UniqueName("group-list")
	s.env.ConsumeAndCommit(s.T(), topic, group, 1)

	out, err := listconsumergroups.Run(
		s.T().Context(),
		s.env.Admin(),
		listconsumergroups.Input{},
	)

	s.Require().NoError(err, "listing groups on a healthy cluster must succeed")
	s.Require().Contains(s.names(out), group,
		"a group that has committed offsets must be listed, because that is what makes its lag measurable")
	s.Require().Equal(len(out.Groups), out.Count,
		"count must report how many groups were returned")
}

func (s *ListConsumerGroupsSuite) TestReportsGroupStateAndTopics() {
	topic := s.env.CreateTopic(s.T(), "groups-state")

	s.env.Produce(s.T(), topic, testenv.Message{Value: "one"})

	group := s.env.UniqueName("group-state")
	s.env.ConsumeAndCommit(s.T(), topic, group, 1)

	out, err := listconsumergroups.Run(
		s.T().Context(),
		s.env.Admin(),
		listconsumergroups.Input{Topic: topic},
	)

	s.Require().NoError(err, "listing groups for one topic must succeed")
	s.Require().Len(out.Groups, 1,
		"exactly the one group consuming this topic must be returned")

	found := out.Groups[0]

	s.Run("the group is named", func() {
		s.Require().Equal(group, found.Group,
			"the returned group must be the one that consumed this topic")
	})

	s.Run("the state is reported", func() {
		s.Require().Equal("Empty", found.State,
			"a group whose consumers have stopped is Empty, and a caller must see that before blaming a slow consumer for lag")
	})

	s.Run("the consumed topic is reported", func() {
		s.Require().Contains(found.Topics, topic,
			"the topics a group has commits for must be listed, since that is how a caller confirms it is the right group")
	})
}

func (s *ListConsumerGroupsSuite) TestFiltersByTopic() {
	wanted := s.env.CreateTopic(s.T(), "groups-wanted")
	other := s.env.CreateTopic(s.T(), "groups-other")

	s.env.Produce(s.T(), wanted, testenv.Message{Value: "one"})
	s.env.Produce(s.T(), other, testenv.Message{Value: "one"})

	wantedGroup := s.env.UniqueName("group-wanted")
	otherGroup := s.env.UniqueName("group-other")

	s.env.ConsumeAndCommit(s.T(), wanted, wantedGroup, 1)
	s.env.ConsumeAndCommit(s.T(), other, otherGroup, 1)

	out, err := listconsumergroups.Run(
		s.T().Context(),
		s.env.Admin(),
		listconsumergroups.Input{Topic: wanted},
	)

	s.Require().NoError(err, "filtering groups by topic must succeed")
	s.Require().Contains(s.names(out), wantedGroup,
		"the group consuming the requested topic must be returned")
	s.Require().NotContains(s.names(out), otherGroup,
		"a group consuming a different topic must be excluded, or the caller cannot tell who is behind on this topic")
}

func (s *ListConsumerGroupsSuite) TestGroupsAreSorted() {
	topic := s.env.CreateTopic(s.T(), "groups-sorted")

	s.env.Produce(s.T(), topic, testenv.Message{Value: "one"})

	for _, prefix := range []string{"group-c", "group-a", "group-b"} {
		s.env.ConsumeAndCommit(s.T(), topic, s.env.UniqueName(prefix), 1)
	}

	out, err := listconsumergroups.Run(
		s.T().Context(),
		s.env.Admin(),
		listconsumergroups.Input{Topic: topic},
	)

	s.Require().NoError(err, "listing several groups must succeed")
	s.Require().IsIncreasing(s.names(out),
		"groups must be sorted, because kadm returns a map and Go map order is random")
}

func (s *ListConsumerGroupsSuite) TestTopicWithNoGroupsReturnsEmptyList() {
	topic := s.env.CreateTopic(s.T(), "groups-none")

	out, err := listconsumergroups.Run(
		s.T().Context(),
		s.env.Admin(),
		listconsumergroups.Input{Topic: topic},
	)

	s.Require().NoError(err,
		"a topic nobody consumes is a normal case, not an error")
	s.Require().NotNil(out.Groups,
		"groups must be an empty slice, not nil, so the JSON output is [] and not null")
	s.Require().Empty(out.Groups,
		"no group consumes this topic, so none may be reported")
	s.Require().Zero(out.Count,
		"the count must agree with the empty list")
}

func (s *ListConsumerGroupsSuite) TestErrorsWhenBrokerUnreachable() {
	client, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
	s.Require().NoError(err, "building a client against a dead address must not fail yet")

	s.T().Cleanup(client.Close)

	_, err = listconsumergroups.Run(
		s.T().Context(),
		kadm.NewClient(client),
		listconsumergroups.Input{},
	)

	s.Require().Error(err,
		"an unreachable broker must surface as an error, not as a cluster with no consumer groups")
}
