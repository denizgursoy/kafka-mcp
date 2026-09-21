package commitoffset_test

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/denizgursoy/kafka-mcp/internal/domain/config"
	"github.com/denizgursoy/kafka-mcp/internal/domain/kafkaclient"
	"github.com/denizgursoy/kafka-mcp/internal/domain/testenv"
	"github.com/denizgursoy/kafka-mcp/internal/tools/commitoffset"
)

type CommitOffsetSuite struct {
	suite.Suite

	env *testenv.Environment
}

func TestCommitOffsetSuite(t *testing.T) {
	suite.Run(t, new(CommitOffsetSuite))
}

func (s *CommitOffsetSuite) SetupSuite() {
	s.env = testenv.Start(s.T())
}

func (s *CommitOffsetSuite) TearDownSuite() {
	s.env.Stop()
}

// committed reads the group's committed offset straight from the broker, so a
// test proves what actually happened rather than trusting the tool's report.
func (s *CommitOffsetSuite) committed(group string, topic string) int64 {
	s.T().Helper()

	offsets, err := s.env.Admin().FetchOffsets(s.T().Context(), group)
	s.Require().NoError(err, "reading the committed offset back must succeed")

	offset, ok := offsets.Lookup(topic, 0)
	s.Require().True(ok, "the group must have a committed offset for this topic")

	return offset.At
}

// stuckGroup produces messages and leaves a group committed partway through,
// which is the state a consumer blocked on a bad message is in.
func (s *CommitOffsetSuite) stuckGroup(prefix string, produce int, consume int) (string, string) {
	s.T().Helper()

	topic := s.env.CreateTopic(s.T(), prefix)

	messages := make([]testenv.Message, 0, produce)
	for i := 0; i < produce; i++ {
		messages = append(messages, testenv.Message{Value: "message"})
	}

	s.env.Produce(s.T(), topic, messages...)

	group := s.env.UniqueName(prefix + "-group")
	s.env.ConsumeAndCommit(s.T(), topic, group, consume)

	return topic, group
}

func (s *CommitOffsetSuite) TestDryRunDoesNotMoveTheOffset() {
	topic, group := s.stuckGroup("commit-dry-run", 10, 4)

	out, err := commitoffset.Run(
		s.T().Context(),
		s.env.ClusterClient(s.T(), false),
		commitoffset.Input{Topic: topic, Group: group, Partition: 0, Offset: 8},
	)

	s.Require().NoError(err, "a dry run against a valid request must succeed")

	s.Run("the committed offset is untouched", func() {
		s.Require().EqualValues(4, s.committed(group, topic),
			"a dry run must change nothing: moving an offset skips messages permanently, so a preview that applied would be the worst possible bug")
	})

	s.Run("the result reports what would happen", func() {
		s.Require().False(out.Applied,
			"the caller must be able to tell a preview from a change that happened")
		s.Require().EqualValues(4, out.CurrentOffset,
			"the starting point must be reported so the caller can judge the move")
		s.Require().EqualValues(8, out.RequestedOffset,
			"the target must be echoed back so a mistyped offset is visible before confirming")
		s.Require().EqualValues(4, out.SkippedMessages,
			"moving from 4 to 8 passes over four messages, and the caller must see that count before agreeing to lose them")
	})
}

func (s *CommitOffsetSuite) TestConfirmMovesTheOffsetForward() {
	topic, group := s.stuckGroup("commit-forward", 10, 3)

	out, err := commitoffset.Run(
		s.T().Context(),
		s.env.ClusterClient(s.T(), false),
		commitoffset.Input{
			Topic:     topic,
			Group:     group,
			Partition: 0,
			Offset:    7,
			Confirm:   true,
		},
	)

	s.Require().NoError(err, "a confirmed commit must succeed")
	s.Require().True(out.Applied, "a change that happened must be reported as applied")
	s.Require().EqualValues(7, s.committed(group, topic),
		"the broker must now report the new offset, which is the only proof the commit took effect")
	s.Require().EqualValues(7, out.ResultingOffset,
		"the resulting offset must be re-read from the broker rather than assumed from the request")
}

func (s *CommitOffsetSuite) TestConfirmMovesTheOffsetBackward() {
	topic, group := s.stuckGroup("commit-backward", 10, 8)

	out, err := commitoffset.Run(
		s.T().Context(),
		s.env.ClusterClient(s.T(), false),
		commitoffset.Input{
			Topic:     topic,
			Group:     group,
			Partition: 0,
			Offset:    2,
			Confirm:   true,
		},
	)

	s.Require().NoError(err,
		"moving an offset backwards is how messages are replayed, and must be allowed")
	s.Require().EqualValues(2, s.committed(group, topic),
		"the group must now re-read from the earlier offset")
	s.Require().EqualValues(6, out.ReplayedMessages,
		"moving from 8 back to 2 means six messages are processed again, and the caller must see that duplicates are coming")
}

func (s *CommitOffsetSuite) TestRefusesAnOffsetBeyondTheEnd() {
	topic, group := s.stuckGroup("commit-past-end", 5, 2)

	_, err := commitoffset.Run(
		s.T().Context(),
		s.env.ClusterClient(s.T(), false),
		commitoffset.Input{
			Topic:     topic,
			Group:     group,
			Partition: 0,
			Offset:    99,
			Confirm:   true,
		},
	)

	s.Require().Error(err,
		"an offset past the end of the partition must be refused: the group would sit ahead of the data and appear caught up while consuming nothing")
	s.Require().EqualValues(2, s.committed(group, topic),
		"a refused request must leave the committed offset exactly as it was")
}

func (s *CommitOffsetSuite) TestRefusesANegativeOffset() {
	topic, group := s.stuckGroup("commit-negative", 5, 2)

	_, err := commitoffset.Run(
		s.T().Context(),
		s.env.ClusterClient(s.T(), false),
		commitoffset.Input{
			Topic:     topic,
			Group:     group,
			Partition: 0,
			Offset:    -5,
			Confirm:   true,
		},
	)

	s.Require().Error(err,
		"a negative offset is never valid, and Kafka would interpret some negative values as special positions rather than rejecting them")
	s.Require().EqualValues(2, s.committed(group, topic),
		"a refused request must leave the committed offset as it was")
}

func (s *CommitOffsetSuite) TestReadOnlyRefusesTheCommit() {
	topic, group := s.stuckGroup("commit-read-only", 10, 3)

	_, err := commitoffset.Run(
		s.T().Context(),
		s.env.ClusterClient(s.T(), true),
		commitoffset.Input{
			Topic:     topic,
			Group:     group,
			Partition: 0,
			Offset:    7,
			Confirm:   true,
		},
	)

	s.Require().Error(err,
		"a read-only server must refuse to move an offset: this protects clusters that have no ACLs of their own, so it cannot be left to the broker")
	s.Require().Contains(err.Error(), "read-only",
		"the error must name the reason, so the operator looks at the configuration rather than at Kafka permissions")
	s.Require().EqualValues(3, s.committed(group, topic),
		"nothing may have moved: a refusal that still changed the offset would be worse than no protection at all")
}

func (s *CommitOffsetSuite) TestReadOnlyStillAllowsADryRun() {
	topic, group := s.stuckGroup("commit-read-only-dry", 10, 3)

	out, err := commitoffset.Run(
		s.T().Context(),
		s.env.ClusterClient(s.T(), true),
		commitoffset.Input{Topic: topic, Group: group, Partition: 0, Offset: 7},
	)

	s.Require().NoError(err,
		"a preview only reads the current offset, so a read-only server can still answer what a move would do")
	s.Require().False(out.Applied, "a preview must never be reported as applied")
	s.Require().EqualValues(3, s.committed(group, topic),
		"the committed offset must be untouched by a preview")
}

func (s *CommitOffsetSuite) TestErrorsOnUnknownGroup() {
	topic := s.env.CreateTopic(s.T(), "commit-unknown-group")

	s.env.Produce(s.T(), topic, testenv.Message{Value: "one"})

	_, err := commitoffset.Run(
		s.T().Context(),
		s.env.ClusterClient(s.T(), false),
		commitoffset.Input{
			Topic:     topic,
			Group:     s.env.UniqueName("never-existed"),
			Partition: 0,
			Offset:    1,
			Confirm:   true,
		},
	)

	s.Require().Error(err,
		"committing for a group that does not exist must fail: it would otherwise create a group out of a typo and appear to have worked")
}

func (s *CommitOffsetSuite) TestErrorsOnUnknownTopic() {
	_, err := commitoffset.Run(
		s.T().Context(),
		s.env.ClusterClient(s.T(), false),
		commitoffset.Input{
			Topic:     s.env.UniqueName("missing"),
			Group:     s.env.UniqueName("group"),
			Partition: 0,
			Offset:    1,
		},
	)

	s.Require().Error(err,
		"a topic that does not exist must fail rather than appear to accept an offset for it")
}

func (s *CommitOffsetSuite) TestErrorsWhenBrokerUnreachable() {
	client, err := kafkaclient.New(&config.Cluster{Name: "test", Brokers: []string{"127.0.0.1:1"}})
	s.Require().NoError(err, "building a client against a dead address must not fail yet")

	s.T().Cleanup(client.Close)

	_, err = commitoffset.Run(
		s.T().Context(),
		client,
		commitoffset.Input{Topic: "anything", Group: "anything", Partition: 0, Offset: 1},
	)

	s.Require().Error(err,
		"an unreachable broker must surface as an error, not as a commit that appears to have succeeded")
}
