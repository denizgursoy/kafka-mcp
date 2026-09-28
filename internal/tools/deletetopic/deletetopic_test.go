package deletetopic_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/denizgursoy/kafka-mcp/internal/domain/testenv"
	"github.com/denizgursoy/kafka-mcp/internal/tools/deletetopic"
)

type DeleteTopicSuite struct {
	suite.Suite

	env *testenv.Environment
}

func TestDeleteTopicSuite(t *testing.T) {
	suite.Run(t, new(DeleteTopicSuite))
}

func (s *DeleteTopicSuite) SetupSuite() {
	s.env = testenv.Start(s.T())
}

func (s *DeleteTopicSuite) TearDownSuite() {
	s.env.Stop()
}

// delete deletes one topic and returns that item's result.
//
// Every call is a batch, so a single topic is an items array of length one, and
// a failure for it arrives as the item's error rather than as an error for the
// call. Structural problems, such as an empty items array, still fail the call.
func (s *DeleteTopicSuite) delete(
	readOnly bool,
	confirm bool,
	item deletetopic.Item,
) (deletetopic.Output, error) {
	s.T().Helper()

	out, err := deletetopic.Run(
		s.T().Context(),
		s.env.ClusterClient(s.T(), readOnly),
		deletetopic.Input{Items: []deletetopic.Item{item}, Confirm: confirm},
	)
	if err != nil {
		return deletetopic.Output{}, err
	}

	s.Require().Len(out.Results, 1,
		"one item in must produce exactly one result out, or results cannot be matched to inputs by position")

	if out.Results[0].Error != "" {
		return deletetopic.Output{}, errors.New(out.Results[0].Error)
	}

	return *out.Results[0].Result, nil
}

func (s *DeleteTopicSuite) TestDeletesAnEmptyTopic() {
	topic := s.env.CreateTopic(s.T(), "delete-empty")

	out, err := s.delete(false, true, deletetopic.Item{Topic: topic})

	s.Require().NoError(err, "deleting an existing empty topic must succeed")

	s.Run("the deletion is reported", func() {
		s.Require().True(out.Deleted,
			"a deletion that happened must say so, because there is no way to check afterwards by reading the topic")
	})

	s.Run("the topic is gone from the broker", func() {
		s.Require().False(s.env.TopicExists(s.T(), topic),
			"the broker must no longer hold the topic, or the tool reported a change it did not make")
	})
}

func (s *DeleteTopicSuite) TestPreviewDeletesNothing() {
	topic := s.env.CreateTopic(s.T(), "delete-preview")

	out, err := s.delete(false, false, deletetopic.Item{Topic: topic})

	s.Require().NoError(err, "a preview must succeed rather than error")

	s.Run("nothing was deleted", func() {
		s.Require().True(s.env.TopicExists(s.T(), topic),
			"a topic cannot be brought back, so omitting confirm must delete nothing at all")
		s.Require().False(out.Deleted, "a preview must not claim to have deleted anything")
	})

	s.Run("the preview says what would happen", func() {
		s.Require().True(out.WouldDelete,
			"the preview must state the topic would go, since that is the decision the caller is about to confirm")
		s.Require().NotEmpty(out.Note,
			"the response must tell the caller how to actually perform the deletion")
	})
}

func (s *DeleteTopicSuite) TestPreviewReportsWhatWouldBeLost() {
	topic := s.env.CreateTopicWithPartitions(s.T(), "delete-reports-loss", 2)

	s.env.Produce(s.T(), topic,
		testenv.Message{Value: "first", Partition: 0},
		testenv.Message{Value: "second", Partition: 0},
		testenv.Message{Value: "third", Partition: 1},
	)

	out, err := s.delete(false, false, deletetopic.Item{Topic: topic})

	s.Require().NoError(err, "previewing a topic that holds messages must succeed")

	s.Run("the message count is reported", func() {
		s.Require().EqualValues(3, out.MessageCount,
			"how much data is about to be destroyed is the single most important fact in this preview")
	})

	s.Run("the partition count is reported", func() {
		s.Require().Equal(2, out.Partitions,
			"the shape is reported so a caller who deletes by mistake knows what to recreate")
	})
}

func (s *DeleteTopicSuite) TestRefusesATopicHoldingMessagesWithoutAcknowledgement() {
	topic := s.env.CreateTopic(s.T(), "delete-holds-messages")

	s.env.Produce(s.T(), topic, testenv.Message{Value: "data that would be destroyed"})

	_, err := s.delete(false, true, deletetopic.Item{Topic: topic})

	s.Require().Error(err,
		"deleting a topic that still holds messages destroys them irrecoverably, so it must take a second, explicit acknowledgement")
	s.Require().True(s.env.TopicExists(s.T(), topic),
		"a refused deletion must leave the topic untouched")
}

func (s *DeleteTopicSuite) TestDeletesATopicHoldingMessagesWhenAcknowledged() {
	topic := s.env.CreateTopic(s.T(), "delete-acknowledged")

	s.env.Produce(s.T(), topic, testenv.Message{Value: "data the caller agreed to lose"})

	out, err := s.delete(false, true, deletetopic.Item{
		Topic:               topic,
		AcknowledgeDataLoss: true,
	})

	s.Require().NoError(err,
		"an explicit acknowledgement must allow the deletion: the tool warns, it does not forbid")
	s.Require().True(out.Deleted, "the deletion must have been applied")
	s.Require().False(s.env.TopicExists(s.T(), topic), "the topic must be gone")
}

func (s *DeleteTopicSuite) TestPreviewOfATopicWithMessagesDoesNotRequireAcknowledgement() {
	topic := s.env.CreateTopic(s.T(), "delete-preview-messages")

	s.env.Produce(s.T(), topic, testenv.Message{Value: "still here"})

	out, err := s.delete(false, false, deletetopic.Item{Topic: topic})

	s.Require().NoError(err,
		"a preview changes nothing, so it must be allowed to report the loss rather than refusing before the caller can see it")
	s.Require().NotEmpty(out.Warnings,
		"the preview must warn that messages would be destroyed, which is what the caller then acknowledges")
	s.Require().True(s.env.TopicExists(s.T(), topic), "previewing must not delete")
}

func (s *DeleteTopicSuite) TestReportsConsumerGroupsThatWouldBreak() {
	topic := s.env.CreateTopic(s.T(), "delete-with-consumers")

	s.env.Produce(s.T(), topic, testenv.Message{Value: "consumed"})

	group := s.env.UniqueName("delete-consumer-group")
	s.env.ConsumeAndCommit(s.T(), topic, group, 1)

	out, err := s.delete(false, false, deletetopic.Item{Topic: topic})

	s.Require().NoError(err, "previewing must succeed")
	s.Require().Contains(out.ConsumerGroups, group,
		"a group committed to this topic breaks when it disappears, and naming it is what lets a caller warn the team that owns it")
}

func (s *DeleteTopicSuite) TestErrorsWhenTheTopicDoesNotExist() {
	_, err := s.delete(false, true, deletetopic.Item{
		Topic: s.env.UniqueName("delete-absent"),
	})

	s.Require().Error(err,
		"deleting a topic that does not exist must be refused rather than reported as success, because a typo would otherwise read as a completed deletion")
	s.Require().Contains(err.Error(), "does not exist",
		"the caller must be told the topic is missing in words they can act on, not handed a broker error code")
}

func (s *DeleteTopicSuite) TestRefusesAnInternalTopic() {
	// __consumer_offsets holds every group's committed position. Deleting it
	// breaks every consumer on the cluster at once.
	_, err := s.delete(false, true, deletetopic.Item{
		Topic:               "__consumer_offsets",
		AcknowledgeDataLoss: true,
	})

	s.Require().Error(err,
		"an internal topic is Kafka's own state rather than a caller's data, and deleting one breaks the cluster in ways no acknowledgement should permit through a debugging tool")
}

func (s *DeleteTopicSuite) TestReadOnlyRefusesEvenAPreview() {
	topic := s.env.CreateTopic(s.T(), "delete-read-only")

	_, err := s.delete(true, false, deletetopic.Item{Topic: topic})

	s.Require().Error(err,
		"deleting is all this tool does, so a read-only endpoint must refuse the preview too rather than describing a capability it does not have")
	s.Require().True(s.env.TopicExists(s.T(), topic),
		"a refused call must leave the topic untouched")
}

func (s *DeleteTopicSuite) TestErrorsOnMissingTopicName() {
	_, err := s.delete(false, true, deletetopic.Item{})

	s.Require().Error(err,
		"an empty name must be refused here rather than sent to the broker, which would answer with an error no caller can act on")
}

func (s *DeleteTopicSuite) TestBatchDeletesSeveralTopics() {
	first := s.env.CreateTopic(s.T(), "delete-batch-first")
	second := s.env.CreateTopic(s.T(), "delete-batch-second")

	out, err := deletetopic.Run(
		s.T().Context(),
		s.env.ClusterClient(s.T(), false),
		deletetopic.Input{
			Items:   []deletetopic.Item{{Topic: first}, {Topic: second}},
			Confirm: true,
		},
	)

	s.Require().NoError(err, "deleting a valid batch must succeed")

	s.Run("every item was deleted", func() {
		s.Require().Equal(2, out.Succeeded, "both topics must be deleted")
		s.Require().False(s.env.TopicExists(s.T(), first), "the first topic must be gone")
		s.Require().False(s.env.TopicExists(s.T(), second), "the second topic must be gone")
	})

	s.Run("the batch reports it is not atomic", func() {
		s.Require().False(out.Atomic,
			"Kafka cannot restore a deleted topic, so a caller must never believe a partial batch was rolled back")
	})
}

func (s *DeleteTopicSuite) TestBatchReportsItemErrorsWithoutHidingSuccesses() {
	topic := s.env.CreateTopic(s.T(), "delete-batch-partial")

	out, err := deletetopic.Run(
		s.T().Context(),
		s.env.ClusterClient(s.T(), false),
		deletetopic.Input{
			Items: []deletetopic.Item{
				{Topic: topic},
				{Topic: s.env.UniqueName("delete-batch-absent")},
			},
			Confirm: true,
		},
	)

	s.Require().NoError(err,
		"one bad item is data in the response, not a failure of the whole call")

	s.Run("the valid item was deleted", func() {
		s.Require().Equal(1, out.Succeeded, "the existing topic must still be deleted")
		s.Require().False(s.env.TopicExists(s.T(), topic), "the valid topic must be gone")
	})

	s.Run("the invalid item is reported in place", func() {
		s.Require().Equal(1, out.Failed, "the missing topic must be counted as failed")
		s.Require().NotEmpty(out.Results[1].Error,
			"an item error must be attached to the item, so it cannot be mistaken for a different one")
	})
}

func (s *DeleteTopicSuite) TestBatchPreviewDeletesNothing() {
	first := s.env.CreateTopic(s.T(), "delete-batch-preview-first")
	second := s.env.CreateTopic(s.T(), "delete-batch-preview-second")

	out, err := deletetopic.Run(
		s.T().Context(),
		s.env.ClusterClient(s.T(), false),
		deletetopic.Input{Items: []deletetopic.Item{{Topic: first}, {Topic: second}}},
	)

	s.Require().NoError(err, "previewing a batch must succeed")
	s.Require().Zero(out.Applied, "a preview must apply nothing")
	s.Require().True(s.env.TopicExists(s.T(), first),
		"one confirm covers the whole batch, so without it not a single topic may be deleted")
	s.Require().True(s.env.TopicExists(s.T(), second),
		"the second topic must also survive a preview")
}

func (s *DeleteTopicSuite) TestRefusesDuplicateTargets() {
	topic := s.env.CreateTopic(s.T(), "delete-duplicate")

	_, err := deletetopic.Run(
		s.T().Context(),
		s.env.ClusterClient(s.T(), false),
		deletetopic.Input{
			Items:   []deletetopic.Item{{Topic: topic}, {Topic: topic}},
			Confirm: true,
		},
	)

	s.Require().Error(err,
		"naming one topic twice in a delete batch means the caller has lost track of what they are removing, so it must be refused before anything is destroyed")
	s.Require().True(s.env.TopicExists(s.T(), topic),
		"a refused batch must leave every topic untouched")
}

func (s *DeleteTopicSuite) TestRejectsAnEmptyItemList() {
	_, err := deletetopic.Run(
		s.T().Context(),
		s.env.ClusterClient(s.T(), false),
		deletetopic.Input{Items: []deletetopic.Item{}, Confirm: true},
	)

	s.Require().Error(err,
		"an empty items array means the intended deletions were lost before the call, which is a caller mistake rather than a no-op")
}
