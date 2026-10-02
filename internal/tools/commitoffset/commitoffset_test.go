package commitoffset_test

import (
	"errors"
	"testing"
	"time"

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

// commitOne moves one offset and returns that item's result.
//
// Every call is a batch, so a single move is an items array of length one, and a
// failure for it arrives as the item's error rather than as an error for the
// call. Structural problems, such as an empty items array, still fail the call.
func (s *CommitOffsetSuite) commitOne(
	kafka *kafkaclient.Client,
	confirm bool,
	item commitoffset.Item,
) (commitoffset.Output, error) {
	s.T().Helper()

	out, err := commitoffset.Run(s.T().Context(), kafka, commitoffset.Input{
		Items:   []commitoffset.Item{item},
		Confirm: confirm,
	})
	if err != nil {
		return commitoffset.Output{}, err
	}

	s.Require().Len(out.Results, 1,
		"one item in must produce exactly one result out, or results cannot be matched to inputs by position")

	if out.Results[0].Error != "" {
		return commitoffset.Output{}, errors.New(out.Results[0].Error)
	}

	return *out.Results[0].Result, nil
}

func (s *CommitOffsetSuite) TestDryRunDoesNotMoveTheOffset() {
	topic, group := s.stuckGroup("commit-dry-run", 10, 4)

	out, err := s.commitOne(s.env.ClusterClient(s.T(), false), false, commitoffset.Item{Topic: topic, Group: group, Partition: ptr(int32(0)), Offset: ptr(int64(8))})

	s.Require().NoError(err, "a dry run against a valid request must succeed")

	s.Run("the committed offset is untouched", func() {
		s.Require().EqualValues(4, s.committed(group, topic),
			"a dry run must change nothing: moving an offset skips messages permanently, so a preview that applied would be the worst possible bug")
	})

	s.Run("the result reports what would happen", func() {
		s.Require().False(out.Applied,
			"the caller must be able to tell a preview from a change that happened")
		s.Require().Len(out.Partitions, 1,
			"an item naming one partition must report exactly that partition")
		s.Require().EqualValues(4, out.Partitions[0].CurrentOffset,
			"the starting point must be reported so the caller can judge the move")
		s.Require().EqualValues(8, out.Partitions[0].TargetOffset,
			"the target must be echoed back so a mistyped offset is visible before confirming")
		s.Require().EqualValues(4, out.SkippedMessages,
			"moving from 4 to 8 passes over four messages, and the caller must see that count before agreeing to lose them")
	})
}

func (s *CommitOffsetSuite) TestConfirmMovesTheOffsetForward() {
	topic, group := s.stuckGroup("commit-forward", 10, 3)

	out, err := s.commitOne(s.env.ClusterClient(s.T(), false), true, commitoffset.Item{
		Topic:     topic,
		Group:     group,
		Partition: ptr(int32(0)),
		Offset:    ptr(int64(7)),
	})

	s.Require().NoError(err, "a confirmed commit must succeed")
	s.Require().True(out.Applied, "a change that happened must be reported as applied")
	s.Require().EqualValues(7, s.committed(group, topic),
		"the broker must now report the new offset, which is the only proof the commit took effect")
	s.Require().NotNil(out.Partitions[0].ResultingOffset, "an applied move must report where the broker left the group")
	s.Require().EqualValues(7, *out.Partitions[0].ResultingOffset,
		"the resulting offset must be re-read from the broker rather than assumed from the request")
}

func (s *CommitOffsetSuite) TestConfirmMovesTheOffsetBackward() {
	topic, group := s.stuckGroup("commit-backward", 10, 8)

	out, err := s.commitOne(s.env.ClusterClient(s.T(), false), true, commitoffset.Item{
		Topic:     topic,
		Group:     group,
		Partition: ptr(int32(0)),
		Offset:    ptr(int64(2)),
	})

	s.Require().NoError(err,
		"moving an offset backwards is how messages are replayed, and must be allowed")
	s.Require().EqualValues(2, s.committed(group, topic),
		"the group must now re-read from the earlier offset")
	s.Require().EqualValues(6, out.ReplayedMessages,
		"moving from 8 back to 2 means six messages are processed again, and the caller must see that duplicates are coming")
}

func (s *CommitOffsetSuite) TestRefusesAnOffsetBeyondTheEnd() {
	topic, group := s.stuckGroup("commit-past-end", 5, 2)

	_, err := s.commitOne(s.env.ClusterClient(s.T(), false), true, commitoffset.Item{
		Topic:     topic,
		Group:     group,
		Partition: ptr(int32(0)),
		Offset:    ptr(int64(99)),
	})

	s.Require().Error(err,
		"an offset past the end of the partition must be refused: the group would sit ahead of the data and appear caught up while consuming nothing")
	s.Require().EqualValues(2, s.committed(group, topic),
		"a refused request must leave the committed offset exactly as it was")
}

func (s *CommitOffsetSuite) TestRefusesANegativeOffset() {
	topic, group := s.stuckGroup("commit-negative", 5, 2)

	_, err := s.commitOne(s.env.ClusterClient(s.T(), false), true, commitoffset.Item{
		Topic:     topic,
		Group:     group,
		Partition: ptr(int32(0)),
		Offset:    ptr(int64(-5)),
	})

	s.Require().Error(err,
		"a negative offset is never valid, and Kafka would interpret some negative values as special positions rather than rejecting them")
	s.Require().EqualValues(2, s.committed(group, topic),
		"a refused request must leave the committed offset as it was")
}

func (s *CommitOffsetSuite) TestReadOnlyRefusesTheCommit() {
	topic, group := s.stuckGroup("commit-read-only", 10, 3)

	_, err := s.commitOne(s.env.ClusterClient(s.T(), true), true, commitoffset.Item{
		Topic:     topic,
		Group:     group,
		Partition: ptr(int32(0)),
		Offset:    ptr(int64(7)),
	})

	s.Require().Error(err,
		"a read-only server must refuse to move an offset: this protects clusters that have no ACLs of their own, so it cannot be left to the broker")
	s.Require().Contains(err.Error(), "read-only",
		"the error must name the reason, so the operator looks at the configuration rather than at Kafka permissions")
	s.Require().EqualValues(3, s.committed(group, topic),
		"nothing may have moved: a refusal that still changed the offset would be worse than no protection at all")
}

func (s *CommitOffsetSuite) TestReadOnlyStillAllowsADryRun() {
	topic, group := s.stuckGroup("commit-read-only-dry", 10, 3)

	out, err := s.commitOne(s.env.ClusterClient(s.T(), true), false, commitoffset.Item{Topic: topic, Group: group, Partition: ptr(int32(0)), Offset: ptr(int64(7))})

	s.Require().NoError(err,
		"a preview only reads the current offset, so a read-only server can still answer what a move would do")
	s.Require().False(out.Applied, "a preview must never be reported as applied")
	s.Require().EqualValues(3, s.committed(group, topic),
		"the committed offset must be untouched by a preview")
}

func (s *CommitOffsetSuite) TestErrorsOnUnknownGroup() {
	topic := s.env.CreateTopic(s.T(), "commit-unknown-group")

	s.env.Produce(s.T(), topic, testenv.Message{Value: "one"})

	_, err := s.commitOne(s.env.ClusterClient(s.T(), false), true, commitoffset.Item{
		Topic:     topic,
		Group:     s.env.UniqueName("never-existed"),
		Partition: ptr(int32(0)),
		Offset:    ptr(int64(1)),
	})

	s.Require().Error(err,
		"committing for a group that does not exist must fail: it would otherwise create a group out of a typo and appear to have worked")
}

func (s *CommitOffsetSuite) TestErrorsOnUnknownTopic() {
	_, err := s.commitOne(s.env.ClusterClient(s.T(), false), false, commitoffset.Item{
		Topic:     s.env.UniqueName("missing"),
		Group:     s.env.UniqueName("group"),
		Partition: ptr(int32(0)),
		Offset:    ptr(int64(1)),
	})

	s.Require().Error(err,
		"a topic that does not exist must fail rather than appear to accept an offset for it")
}

func (s *CommitOffsetSuite) TestErrorsWhenBrokerUnreachable() {
	client, err := kafkaclient.New(&config.Cluster{Name: "test", Brokers: []string{"127.0.0.1:1"}})
	s.Require().NoError(err, "building a client against a dead address must not fail yet")

	s.T().Cleanup(client.Close)

	_, err = s.commitOne(client, false, commitoffset.Item{
		Topic: "anything", Group: "anything", Partition: ptr(int32(0)), Offset: ptr(int64(1)),
	})

	s.Require().Error(err,
		"an unreachable broker must surface as an error, not as a commit that appears to have succeeded")
}

func (s *CommitOffsetSuite) TestBatchCommitsSeveralOffsetsAndReportsItemErrors() {
	firstTopic, firstGroup := s.stuckGroup("commit-batch-first", 8, 2)
	secondTopic, secondGroup := s.stuckGroup("commit-batch-second", 8, 3)

	out, err := commitoffset.Run(
		s.T().Context(),
		s.env.ClusterClient(s.T(), false),
		commitoffset.Input{
			Items: []commitoffset.Item{
				{Topic: firstTopic, Group: firstGroup, Partition: ptr(int32(0)), Offset: ptr(int64(6))},
				{Topic: secondTopic, Group: secondGroup, Partition: ptr(int32(0)), Offset: ptr(int64(5))},
				{Topic: secondTopic, Group: secondGroup, Partition: ptr(int32(99)), Offset: ptr(int64(5))},
			},
			Confirm: true,
		},
	)

	s.Require().NoError(err, "a structurally valid batch must return per-item results even when one item is invalid")
	s.Require().Len(out.Results, 3, "every requested offset must have a result in input order")
	s.Require().Equal(2, out.Succeeded, "the two valid offsets must be committed")
	s.Require().Equal(1, out.Failed, "the invalid partition must be isolated as one failed item")
	s.Require().EqualValues(6, s.committed(firstGroup, firstTopic), "the first valid offset must be applied")
	s.Require().EqualValues(5, s.committed(secondGroup, secondTopic), "the second valid offset must be applied")
	s.Require().Contains(out.Results[2].Error, "partition 99", "the failed item must explain which partition was invalid")
}

func ptr[T any](value T) *T {
	return &value
}

// replayGroup produces messages with known timestamps across two partitions and
// commits the group to the end of both, which is the state a group is in when a
// bug fix ships and everything since some moment has to be processed again.
func (s *CommitOffsetSuite) replayGroup(prefix string, base time.Time) (string, string) {
	s.T().Helper()

	topic := s.env.CreateTopicWithPartitions(s.T(), prefix, 2)

	messages := make([]testenv.Message, 0, 8)
	for partition := int32(0); partition < 2; partition++ {
		for i := 0; i < 4; i++ {
			messages = append(messages, testenv.Message{
				Value:     "message",
				Partition: partition,
				Timestamp: base.Add(time.Duration(i) * time.Hour),
			})
		}
	}

	s.env.Produce(s.T(), topic, messages...)

	group := s.env.UniqueName(prefix + "-group")
	s.env.ConsumeAndCommit(s.T(), topic, group, 8)

	return topic, group
}

func (s *CommitOffsetSuite) committedOn(group string, topic string, partition int32) int64 {
	s.T().Helper()

	offsets, err := s.env.Admin().FetchOffsets(s.T().Context(), group)
	s.Require().NoError(err, "reading the committed offset back must succeed")

	offset, ok := offsets.Lookup(topic, partition)
	s.Require().True(ok, "the group must have a committed offset for partition %d", partition)

	return offset.At
}

func (s *CommitOffsetSuite) TestTimestampMovesEveryPartitionWhenPartitionIsOmitted() {
	base := time.Now().Add(-48 * time.Hour).Truncate(time.Second)
	topic, group := s.replayGroup("commit-timestamp-all", base)

	// Two hours after the first message is offset 2 on both partitions.
	out, err := s.commitOne(s.env.ClusterClient(s.T(), false), true, commitoffset.Item{
		Topic:     topic,
		Group:     group,
		Timestamp: base.Add(2 * time.Hour).Format(time.RFC3339),
	})

	s.Require().NoError(err, "replaying from a time across every partition must succeed")

	s.Run("every partition is reported", func() {
		s.Require().Len(out.Partitions, 2,
			"omitting partition means the whole topic, so both partitions must be in the result")
		s.Require().EqualValues(0, out.Partitions[0].Partition,
			"partitions must be sorted so the report is stable between calls")
		s.Require().EqualValues(1, out.Partitions[1].Partition,
			"partitions must be sorted so the report is stable between calls")
	})

	s.Run("each partition moves to the first message at or after the time", func() {
		s.Require().EqualValues(2, s.committedOn(group, topic, 0),
			"partition 0 holds its third message at that time, so the group must read it next")
		s.Require().EqualValues(2, s.committedOn(group, topic, 1),
			"partition 1 must move independently to its own offset for that time")
	})

	s.Run("the replay is counted across partitions", func() {
		s.Require().EqualValues(4, out.ReplayedMessages,
			"two messages per partition will be processed again, and the caller must see the total before agreeing to duplicates")
		s.Require().True(out.Applied, "a confirmed move must be reported as applied")
	})
}

func (s *CommitOffsetSuite) TestTimestampPreviewChangesNothing() {
	base := time.Now().Add(-48 * time.Hour).Truncate(time.Second)
	topic, group := s.replayGroup("commit-timestamp-dry", base)

	out, err := s.commitOne(s.env.ClusterClient(s.T(), false), false, commitoffset.Item{
		Topic:     topic,
		Group:     group,
		Timestamp: base.Format(time.RFC3339),
	})

	s.Require().NoError(err, "previewing a replay must succeed")
	s.Require().False(out.Applied, "a preview must never be reported as applied")
	s.Require().EqualValues(8, out.ReplayedMessages,
		"replaying from the first message replays all eight, which the preview must say before anything moves")
	s.Require().EqualValues(4, s.committedOn(group, topic, 0),
		"a preview must leave the committed offset where it was")
}

func (s *CommitOffsetSuite) TestTimestampAfterTheLastMessageGoesToTheEnd() {
	base := time.Now().Add(-48 * time.Hour).Truncate(time.Second)
	topic, group := s.replayGroup("commit-timestamp-future", base)

	out, err := s.commitOne(s.env.ClusterClient(s.T(), false), false, commitoffset.Item{
		Topic:     topic,
		Group:     group,
		Partition: ptr(int32(0)),
		Timestamp: base.Add(24 * time.Hour).Format(time.RFC3339),
	})

	s.Require().NoError(err, "a time after every message is valid: it means nothing to replay")
	s.Require().EqualValues(4, out.Partitions[0].TargetOffset,
		"with no message at or after the time, the target is the end of the partition, as Kafka reports it")
	s.Require().NotEmpty(out.Partitions[0].Note,
		"the caller must be told no message matched, or a mistyped date looks like a successful replay of nothing")
}

func (s *CommitOffsetSuite) TestPositionEarliestReplaysEverything() {
	topic, group := s.stuckGroup("commit-earliest", 6, 6)

	out, err := s.commitOne(s.env.ClusterClient(s.T(), false), true, commitoffset.Item{
		Topic:    topic,
		Group:    group,
		Position: "earliest",
	})

	s.Require().NoError(err, "moving to earliest must succeed")
	s.Require().EqualValues(0, s.committed(group, topic),
		"earliest means the first offset the partition still holds")
	s.Require().EqualValues(6, out.ReplayedMessages,
		"every consumed message will be processed again, and the caller must see how many")
}

func (s *CommitOffsetSuite) TestPositionLatestSkipsTheBacklog() {
	topic, group := s.stuckGroup("commit-latest", 9, 2)

	out, err := s.commitOne(s.env.ClusterClient(s.T(), false), true, commitoffset.Item{
		Topic:     topic,
		Group:     group,
		Partition: ptr(int32(0)),
		Position:  "latest",
	})

	s.Require().NoError(err, "moving to latest must succeed")
	s.Require().EqualValues(9, s.committed(group, topic),
		"latest means the end of the partition, so the group reads only new messages")
	s.Require().EqualValues(7, out.SkippedMessages,
		"the seven unread messages will never be processed, and the caller must see that before agreeing")
}

func (s *CommitOffsetSuite) TestRefusesMoreThanOneTarget() {
	topic, group := s.stuckGroup("commit-two-targets", 5, 2)

	_, err := s.commitOne(s.env.ClusterClient(s.T(), false), false, commitoffset.Item{
		Topic:     topic,
		Group:     group,
		Partition: ptr(int32(0)),
		Offset:    ptr(int64(3)),
		Position:  "earliest",
	})

	s.Require().Error(err,
		"offset, timestamp and position each name a different place, so giving two leaves the caller's intent ambiguous")
}

func (s *CommitOffsetSuite) TestRefusesNoTarget() {
	topic, group := s.stuckGroup("commit-no-target", 5, 2)

	_, err := s.commitOne(s.env.ClusterClient(s.T(), false), false, commitoffset.Item{
		Topic: topic,
		Group: group,
	})

	s.Require().Error(err, "an item that says nowhere to move cannot be guessed at")
}

func (s *CommitOffsetSuite) TestRefusesAnOffsetWithoutAPartition() {
	topic, group := s.stuckGroup("commit-offset-no-partition", 5, 2)

	_, err := s.commitOne(s.env.ClusterClient(s.T(), false), false, commitoffset.Item{
		Topic:  topic,
		Group:  group,
		Offset: ptr(int64(3)),
	})

	s.Require().Error(err,
		"an exact offset means one place in one partition; applying it to every partition would point most of them at unrelated messages")
}

func (s *CommitOffsetSuite) TestRefusesAnUnknownPosition() {
	topic, group := s.stuckGroup("commit-bad-position", 5, 2)

	_, err := s.commitOne(s.env.ClusterClient(s.T(), false), false, commitoffset.Item{
		Topic:    topic,
		Group:    group,
		Position: "beginning",
	})

	s.Require().Error(err, "only earliest and latest are positions; anything else must be refused rather than guessed")
}

func (s *CommitOffsetSuite) TestRefusesAWholeTopicItemOverlappingAPartitionItem() {
	topic, group := s.stuckGroup("commit-overlap", 5, 2)

	_, err := commitoffset.Run(s.T().Context(), s.env.ClusterClient(s.T(), false), commitoffset.Input{
		Items: []commitoffset.Item{
			{Topic: topic, Group: group, Position: "earliest"},
			{Topic: topic, Group: group, Partition: ptr(int32(0)), Offset: ptr(int64(3))},
		},
		Confirm: true,
	})

	s.Require().Error(err,
		"a whole-topic move and a partition move for the same group both target partition 0, and which one wins would depend on order")
	s.Require().EqualValues(2, s.committed(group, topic), "a refused batch must change nothing")
}
