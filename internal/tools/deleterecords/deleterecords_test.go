package deleterecords_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/denizgursoy/kafka-mcp/internal/domain/testenv"
	"github.com/denizgursoy/kafka-mcp/internal/tools/deleterecords"
)

type DeleteRecordsSuite struct {
	suite.Suite

	env *testenv.Environment
}

func TestDeleteRecordsSuite(t *testing.T) {
	suite.Run(t, new(DeleteRecordsSuite))
}

func (s *DeleteRecordsSuite) SetupSuite() {
	s.env = testenv.Start(s.T())
}

func (s *DeleteRecordsSuite) TearDownSuite() {
	s.env.Stop()
}

func (s *DeleteRecordsSuite) deleteOne(
	readOnly bool,
	confirm bool,
	item deleterecords.Item,
) (deleterecords.Output, error) {
	s.T().Helper()

	out, err := deleterecords.Run(
		s.T().Context(),
		s.env.ClusterClient(s.T(), readOnly),
		deleterecords.Input{Items: []deleterecords.Item{item}, Confirm: confirm},
	)
	if err != nil {
		return deleterecords.Output{}, err
	}

	s.Require().Len(out.Results, 1,
		"one item in must produce exactly one result out, or results cannot be matched to inputs by position")

	if out.Results[0].Error != "" {
		return deleterecords.Output{}, errors.New(out.Results[0].Error)
	}

	return *out.Results[0].Result, nil
}

// startOffset reads the partition's first readable offset from the broker, so
// a test proves what was deleted rather than trusting the tool's report.
func (s *DeleteRecordsSuite) startOffset(topic string, partition int32) int64 {
	s.T().Helper()

	starts, err := s.env.Admin().ListStartOffsets(s.T().Context(), topic)
	s.Require().NoError(err, "reading the start offset must succeed")

	start, ok := starts.Lookup(topic, partition)
	s.Require().True(ok, "the partition must report a start offset")
	s.Require().NoError(start.Err, "the start offset must be readable")

	return start.Offset
}

func (s *DeleteRecordsSuite) topicWith(prefix string, count int) string {
	s.T().Helper()

	topic := s.env.CreateTopic(s.T(), prefix)

	messages := make([]testenv.Message, 0, count)
	for range count {
		messages = append(messages, testenv.Message{Value: "message"})
	}

	s.env.Produce(s.T(), topic, messages...)

	return topic
}

func (s *DeleteRecordsSuite) TestPreviewDeletesNothing() {
	topic := s.topicWith("delete-records-preview", 10)

	out, err := s.deleteOne(false, false, deleterecords.Item{Topic: topic, Partition: 0, BeforeOffset: 6})
	s.Require().NoError(err, "previewing a deletion must succeed")

	s.Require().EqualValues(0, s.startOffset(topic, 0),
		"a preview must change nothing: deleted records cannot be restored")
	s.Require().False(out.Deleted, "a preview must be told apart from a deletion that happened")
	s.Require().EqualValues(6, out.MessagesDeleted,
		"offsets 0 to 5 would go, and the caller must see how many before agreeing")
	s.Require().EqualValues(0, out.StartOffset, "the current start offset is reported so the range is visible")
	s.Require().EqualValues(10, out.EndOffset, "the end offset is reported so the caller sees what remains")
}

func (s *DeleteRecordsSuite) TestConfirmMovesTheStartOffset() {
	topic := s.topicWith("delete-records-confirm", 10)

	out, err := s.deleteOne(false, true, deleterecords.Item{
		Topic: topic, Partition: 0, BeforeOffset: 6, AcknowledgeDataLoss: true,
	})
	s.Require().NoError(err, "an acknowledged deletion must succeed")
	s.Require().True(out.Deleted, "a deletion that happened must be reported as such")
	s.Require().EqualValues(6, s.startOffset(topic, 0),
		"the broker must now start the partition at offset 6, which is the only proof the records are gone")
	s.Require().EqualValues(6, out.ResultingStartOffset, "the resulting start offset is what the broker reported")
}

func (s *DeleteRecordsSuite) TestRefusesWithoutAcknowledgement() {
	topic := s.topicWith("delete-records-no-ack", 4)

	_, err := s.deleteOne(false, true, deleterecords.Item{Topic: topic, Partition: 0, BeforeOffset: 2})
	s.Require().Error(err,
		"deleting messages is irreversible, so confirm alone is not enough: the item must acknowledge the loss")
	s.Require().EqualValues(0, s.startOffset(topic, 0), "a refused deletion must leave every message readable")
}

func (s *DeleteRecordsSuite) TestWarnsAboutGroupsThatHaveNotReachedTheCut() {
	topic := s.topicWith("delete-records-groups", 10)

	behind := s.env.UniqueName("delete-records-behind")
	s.env.ConsumeAndCommit(s.T(), topic, behind, 2)

	ahead := s.env.UniqueName("delete-records-ahead")
	s.env.ConsumeAndCommit(s.T(), topic, ahead, 8)

	out, err := s.deleteOne(false, false, deleterecords.Item{Topic: topic, Partition: 0, BeforeOffset: 6})
	s.Require().NoError(err, "previewing must succeed")

	s.Require().Len(out.AffectedGroups, 1,
		"only the group that has not read up to the cut loses messages it never processed")
	s.Require().Equal(behind, out.AffectedGroups[0].Group, "the group that is behind must be named")
	s.Require().EqualValues(4, out.AffectedGroups[0].UnprocessedLost,
		"the group committed at 2, so offsets 2 to 5 are deleted before it reads them")
}

func (s *DeleteRecordsSuite) TestRefusesAnOffsetBeyondTheEnd() {
	topic := s.topicWith("delete-records-past-end", 3)

	_, err := s.deleteOne(false, false, deleterecords.Item{Topic: topic, Partition: 0, BeforeOffset: 99})
	s.Require().Error(err, "a cut past the end would claim to delete messages that do not exist")
}

func (s *DeleteRecordsSuite) TestRefusesAnOffsetAlreadyDeleted() {
	topic := s.topicWith("delete-records-behind-start", 6)

	_, err := s.deleteOne(false, true, deleterecords.Item{
		Topic: topic, Partition: 0, BeforeOffset: 4, AcknowledgeDataLoss: true,
	})
	s.Require().NoError(err, "the first deletion must succeed")

	_, err = s.deleteOne(false, false, deleterecords.Item{Topic: topic, Partition: 0, BeforeOffset: 2})
	s.Require().Error(err,
		"offsets before the current start are already gone, so a cut there deletes nothing and is almost certainly a mistake")
}

func (s *DeleteRecordsSuite) TestErrorsOnUnknownPartition() {
	topic := s.topicWith("delete-records-bad-partition", 2)

	_, err := s.deleteOne(false, false, deleterecords.Item{Topic: topic, Partition: 7, BeforeOffset: 1})
	s.Require().Error(err, "a partition the topic does not have must fail rather than appear to delete")
}

func (s *DeleteRecordsSuite) TestErrorsOnUnknownTopic() {
	_, err := s.deleteOne(false, false, deleterecords.Item{
		Topic: s.env.UniqueName("missing"), Partition: 0, BeforeOffset: 1,
	})
	s.Require().Error(err, "a topic that does not exist must fail")
}

func (s *DeleteRecordsSuite) TestReadOnlyRefusesEvenThePreview() {
	topic := s.topicWith("delete-records-read-only", 4)

	_, err := s.deleteOne(true, false, deleterecords.Item{Topic: topic, Partition: 0, BeforeOffset: 2})
	s.Require().Error(err, "deleting is this tool's only purpose, so a read-only server refuses the preview too")
	s.Require().Contains(err.Error(), "read-only", "the error must name the reason")
}

func (s *DeleteRecordsSuite) TestBatchAppliesValidItemsAndReportsTheRest() {
	topic := s.env.CreateTopicWithPartitions(s.T(), "delete-records-batch", 2)
	s.env.Produce(s.T(), topic,
		testenv.Message{Value: "a", Partition: 0}, testenv.Message{Value: "b", Partition: 0},
		testenv.Message{Value: "c", Partition: 1}, testenv.Message{Value: "d", Partition: 1},
	)

	out, err := deleterecords.Run(s.T().Context(), s.env.ClusterClient(s.T(), false), deleterecords.Input{
		Items: []deleterecords.Item{
			{Topic: topic, Partition: 0, BeforeOffset: 1, AcknowledgeDataLoss: true},
			{Topic: topic, Partition: 5, BeforeOffset: 1, AcknowledgeDataLoss: true},
			{Topic: topic, Partition: 1, BeforeOffset: 2, AcknowledgeDataLoss: true},
		},
		Confirm: true,
	})

	s.Require().NoError(err, "a structurally valid batch must return per-item results")
	s.Require().Equal(2, out.Succeeded, "both valid partitions must be truncated")
	s.Require().Equal(1, out.Failed, "the missing partition must be one failed item")
	s.Require().EqualValues(1, s.startOffset(topic, 0), "partition 0 must start at the requested cut")
	s.Require().EqualValues(2, s.startOffset(topic, 1), "the item after a failed one must still apply")
}

func (s *DeleteRecordsSuite) timedTopic(prefix string, partitions int32) (string, time.Time) {
	topic := s.env.CreateTopicWithPartitions(s.T(), prefix, partitions)
	cut := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	before, after := cut.Add(-time.Minute), cut.Add(time.Minute)

	messages := []testenv.Message{}
	for partition := int32(0); partition < partitions; partition++ {
		messages = append(messages,
			testenv.Message{Value: "old", Partition: partition, Timestamp: before},
			testenv.Message{Value: "old", Partition: partition, Timestamp: before},
			testenv.Message{Value: "new", Partition: partition, Timestamp: after},
		)
	}
	// Partition 1 gets an extra old message, so its cut differs from 0's.
	if partitions > 1 {
		messages = append([]testenv.Message{{Value: "old", Partition: 1, Timestamp: before}}, messages...)
	}
	s.env.Produce(s.T(), topic, messages...)

	return topic, cut
}

func (s *DeleteRecordsSuite) TestCutsAtATimestamp() {
	topic, cut := s.timedTopic("delete-records-time", 1)

	out, err := s.deleteOne(false, true, deleterecords.Item{
		Topic: topic, Partition: 0, BeforeTimestamp: &cut, AcknowledgeDataLoss: true,
	})

	s.Require().NoError(err, "a cut by time must resolve and apply")
	s.Require().EqualValues(2, out.BeforeOffset,
		"the cut is the first offset at or after the moment, which keeps the message written then")
	s.Require().EqualValues(2, s.startOffset(topic, 0), "the broker must agree the older messages are gone")
}

func (s *DeleteRecordsSuite) TestCutsEveryPartitionAtATimestamp() {
	topic, cut := s.timedTopic("delete-records-time-all", 2)

	out, err := s.deleteOne(false, false, deleterecords.Item{Topic: topic, AllPartitions: true, BeforeTimestamp: &cut})
	s.Require().NoError(err, "a preview over every partition must succeed")

	s.Run("each partition gets its own cut", func() {
		s.Require().Len(out.Partitions, 2, "both partitions must be reported, sorted")
		s.Require().EqualValues(2, out.Partitions[0].BeforeOffset, "partition 0 has two older messages")
		s.Require().EqualValues(3, out.Partitions[1].BeforeOffset,
			"partition 1 has three, so copying one partition's cut to another would be wrong; resolving per partition is the point")
	})
	s.Run("the item is marked as covering every partition", func() {
		s.Require().True(out.AllPartitions, "a reader must not take the top-level offsets for one partition's cut")
		s.Require().EqualValues(-1, out.Partition, "no single partition is meant, and 0 would name a real one")
	})
	s.Run("the preview totals the loss", func() {
		s.Require().EqualValues(5, out.MessagesDeleted, "two plus three older messages would go")
		s.Require().EqualValues(0, s.startOffset(topic, 1), "a preview deletes nothing")
	})

	applied, err := s.deleteOne(false, true, deleterecords.Item{
		Topic: topic, AllPartitions: true, BeforeTimestamp: &cut, AcknowledgeDataLoss: true,
	})
	s.Require().NoError(err, "applying a cut over every partition must succeed")
	s.Require().True(applied.Deleted, "the result must say the deletion happened")
	s.Require().EqualValues(2, s.startOffset(topic, 0), "partition 0 must start at its cut")
	s.Require().EqualValues(3, s.startOffset(topic, 1), "partition 1 must start at its own cut")
}

func (s *DeleteRecordsSuite) TestTimestampCutSkipsPartitionsWithNothingOlder() {
	topic, _ := s.timedTopic("delete-records-time-nothing", 2)
	early := time.Now().Add(-48 * time.Hour)

	_, err := s.deleteOne(false, false, deleterecords.Item{Topic: topic, AllPartitions: true, BeforeTimestamp: &early})

	s.Require().ErrorContains(err, "nothing",
		"a moment older than every message deletes nothing anywhere, and must say so rather than report an empty success")
}

func (s *DeleteRecordsSuite) TestRefusesAmbiguousCuts() {
	topic := s.topicWith("delete-records-ambiguous", 3)
	at := time.Now()

	_, err := s.deleteOne(false, false, deleterecords.Item{Topic: topic, BeforeOffset: 2, BeforeTimestamp: &at})
	s.Require().ErrorContains(err, "before_offset", "an offset and a time are two different cuts; one must be chosen")

	_, err = s.deleteOne(false, false, deleterecords.Item{Topic: topic, AllPartitions: true, BeforeOffset: 2})
	s.Require().ErrorContains(err, "all_partitions",
		"an offset means nothing across partitions, so all_partitions needs before_timestamp")
}

func (s *DeleteRecordsSuite) TestRefusesDuplicatePartitions() {
	topic := s.topicWith("delete-records-duplicate", 4)

	_, err := deleterecords.Run(s.T().Context(), s.env.ClusterClient(s.T(), false), deleterecords.Input{
		Items: []deleterecords.Item{
			{Topic: topic, Partition: 0, BeforeOffset: 1, AcknowledgeDataLoss: true},
			{Topic: topic, Partition: 0, BeforeOffset: 3, AcknowledgeDataLoss: true},
		},
		Confirm: true,
	})

	s.Require().Error(err, "two cuts on one partition leave the result depending on order")
	s.Require().EqualValues(0, s.startOffset(topic, 0), "a refused batch must delete nothing")
}
