package altertopicconfig_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/denizgursoy/kafka-mcp/internal/domain/testenv"
	"github.com/denizgursoy/kafka-mcp/internal/tools/altertopicconfig"
)

type AlterTopicConfigSuite struct {
	suite.Suite

	env *testenv.Environment
}

func TestAlterTopicConfigSuite(t *testing.T) {
	suite.Run(t, new(AlterTopicConfigSuite))
}

func (s *AlterTopicConfigSuite) SetupSuite() {
	s.env = testenv.Start(s.T())
}

func (s *AlterTopicConfigSuite) TearDownSuite() {
	s.env.Stop()
}

// alter runs one item and returns its result, so a failure for it arrives as
// the item's error rather than as an error for the call.
func (s *AlterTopicConfigSuite) alter(
	readOnly bool,
	confirm bool,
	item altertopicconfig.Item,
) (altertopicconfig.Output, error) {
	s.T().Helper()

	out, err := altertopicconfig.Run(
		s.T().Context(),
		s.env.ClusterClient(s.T(), readOnly),
		altertopicconfig.Input{Items: []altertopicconfig.Item{item}, Confirm: confirm},
	)
	if err != nil {
		return altertopicconfig.Output{}, err
	}

	s.Require().Len(out.Results, 1,
		"one item in must produce exactly one result out, or results cannot be matched to inputs by position")

	if out.Results[0].Error != "" {
		return altertopicconfig.Output{}, errors.New(out.Results[0].Error)
	}

	return *out.Results[0].Result, nil
}

func (s *AlterTopicConfigSuite) change(out altertopicconfig.Output, key string) altertopicconfig.Change {
	s.T().Helper()

	for _, change := range out.Changes {
		if change.Key == key {
			return change
		}
	}

	s.FailNow("missing change", "the result must report the change to %s", key)

	return altertopicconfig.Change{}
}

func (s *AlterTopicConfigSuite) TestPreviewChangesNothing() {
	topic := s.env.CreateTopicWithConfig(s.T(), "alter-preview", map[string]string{"retention.ms": "86400000"})

	out, err := s.alter(false, false, altertopicconfig.Item{
		Topic: topic,
		Set:   map[string]string{"retention.ms": "3600000"},
	})

	s.Require().NoError(err, "previewing a valid change must succeed")

	s.Run("the broker still holds the old value", func() {
		s.Require().Equal("86400000", s.env.TopicConfig(s.T(), topic, "retention.ms"),
			"a preview must change nothing: a retention change deletes data, so a preview that applied would destroy it")
	})

	s.Run("the result shows the before and after", func() {
		s.Require().False(out.Applied, "a preview must be told apart from a change that happened")
		change := s.change(out, "retention.ms")
		s.Require().Equal("86400000", change.Current, "the current value must be shown so the caller sees what is replaced")
		s.Require().Equal("3600000", change.Requested, "the requested value must be echoed so a typo is visible before confirming")
		s.Require().Equal("DYNAMIC_TOPIC_CONFIG", change.CurrentSource,
			"the source says whether the current value was a deliberate topic setting or inherited")
	})
}

func (s *AlterTopicConfigSuite) TestConfirmSetsAndKeepsOtherOverrides() {
	topic := s.env.CreateTopicWithConfig(s.T(), "alter-set", map[string]string{
		"retention.ms":      "86400000",
		"max.message.bytes": "2000000",
	})

	out, err := s.alter(false, true, altertopicconfig.Item{
		Topic: topic,
		Set:   map[string]string{"retention.ms": "172800000"},
	})

	s.Require().NoError(err, "a confirmed change must succeed")
	s.Require().True(out.Applied, "a change that happened must be reported as applied")
	s.Require().Equal("172800000", s.env.TopicConfig(s.T(), topic, "retention.ms"),
		"the broker must now report the new value, which is the only proof the change took effect")
	s.Require().Equal("2000000", s.env.TopicConfig(s.T(), topic, "max.message.bytes"),
		"an incremental change must leave every other override alone; a full replace would silently reset them")
	s.Require().Equal("172800000", s.change(out, "retention.ms").Resulting,
		"the resulting value must be re-read from the broker rather than assumed from the request")
}

func (s *AlterTopicConfigSuite) TestDeleteRestoresTheInheritedValue() {
	topic := s.env.CreateTopicWithConfig(s.T(), "alter-delete", map[string]string{"retention.ms": "3600000"})

	out, err := s.alter(false, true, altertopicconfig.Item{
		Topic:  topic,
		Delete: []string{"retention.ms"},
	})

	s.Require().NoError(err, "removing an override must succeed")
	change := s.change(out, "retention.ms")
	s.Require().NotEqual("DYNAMIC_TOPIC_CONFIG", change.ResultingSource,
		"after removing the override the value must be inherited again, or the delete did nothing")
	s.Require().NotEqual("3600000", change.Resulting,
		"the topic must no longer carry the override that was removed")
}

func (s *AlterTopicConfigSuite) TestWarnsAboutMessagesARetentionCutWouldExpire() {
	topic := s.env.CreateTopic(s.T(), "alter-retention-cut")

	now := time.Now()
	s.env.Produce(s.T(), topic,
		testenv.Message{Value: "old", Timestamp: now.Add(-5 * time.Hour)},
		testenv.Message{Value: "old", Timestamp: now.Add(-4 * time.Hour)},
		testenv.Message{Value: "old", Timestamp: now.Add(-3 * time.Hour)},
		testenv.Message{Value: "new", Timestamp: now.Add(-10 * time.Minute)},
	)

	out, err := s.alter(false, false, altertopicconfig.Item{
		Topic: topic,
		Set:   map[string]string{"retention.ms": "7200000"},
	})

	s.Require().NoError(err, "previewing a retention cut must succeed")
	s.Require().EqualValues(3, out.MessagesPastRetention,
		"three messages are older than two hours, and the caller must know they become eligible for deletion before agreeing")
	s.Require().NotEmpty(out.Warnings, "a change that can delete data must say so in words, not only as a number")
}

func (s *AlterTopicConfigSuite) TestWarnsWhenCleanupPolicyChanges() {
	topic := s.env.CreateTopic(s.T(), "alter-cleanup")

	out, err := s.alter(false, false, altertopicconfig.Item{
		Topic: topic,
		Set:   map[string]string{"cleanup.policy": "compact"},
	})

	s.Require().NoError(err, "previewing a cleanup policy change must succeed")
	s.Require().NotEmpty(out.Warnings,
		"switching to compaction keeps only the latest value per key and drops unkeyed messages, which a caller must be told")
}

func (s *AlterTopicConfigSuite) TestBrokerRejectsAnUnknownKeyInThePreview() {
	topic := s.env.CreateTopic(s.T(), "alter-unknown-key")

	_, err := s.alter(false, false, altertopicconfig.Item{
		Topic: topic,
		Set:   map[string]string{"retention.forever": "yes"},
	})

	s.Require().Error(err,
		"the preview must ask the broker, so a key it does not know is refused before confirm rather than after")
}

func (s *AlterTopicConfigSuite) TestRefusesTheSameKeyInSetAndDelete() {
	topic := s.env.CreateTopic(s.T(), "alter-set-and-delete")

	_, err := s.alter(false, false, altertopicconfig.Item{
		Topic:  topic,
		Set:    map[string]string{"retention.ms": "3600000"},
		Delete: []string{"retention.ms"},
	})

	s.Require().Error(err, "setting and removing the same key at once has no single meaning and must be refused")
}

func (s *AlterTopicConfigSuite) TestRefusesAnEmptyChange() {
	topic := s.env.CreateTopic(s.T(), "alter-empty")

	_, err := s.alter(false, false, altertopicconfig.Item{Topic: topic})

	s.Require().Error(err, "an item that changes nothing is a mistake in the request, not a successful no-op")
}

func (s *AlterTopicConfigSuite) TestErrorsOnUnknownTopic() {
	_, err := s.alter(false, false, altertopicconfig.Item{
		Topic: s.env.UniqueName("missing"),
		Set:   map[string]string{"retention.ms": "3600000"},
	})

	s.Require().Error(err, "a topic that does not exist must fail rather than appear to accept a config for it")
}

func (s *AlterTopicConfigSuite) TestReadOnlyRefusesEvenThePreview() {
	topic := s.env.CreateTopicWithConfig(s.T(), "alter-read-only", map[string]string{"retention.ms": "86400000"})

	_, err := s.alter(true, false, altertopicconfig.Item{
		Topic: topic,
		Set:   map[string]string{"retention.ms": "3600000"},
	})

	s.Require().Error(err,
		"changing config is this tool's only purpose, so a read-only server refuses the preview too rather than describe a change it can never make")
	s.Require().Contains(err.Error(), "read-only", "the error must name the reason, so the operator checks the configuration")
	s.Require().Equal("86400000", s.env.TopicConfig(s.T(), topic, "retention.ms"), "nothing may have changed")
}

func (s *AlterTopicConfigSuite) TestBatchReportsItemErrorsAndAppliesTheRest() {
	topics := s.env.CreateTopics(s.T(), "alter-batch-a", "alter-batch-b")

	out, err := altertopicconfig.Run(s.T().Context(), s.env.ClusterClient(s.T(), false), altertopicconfig.Input{
		Items: []altertopicconfig.Item{
			{Topic: topics[0], Set: map[string]string{"retention.ms": "7200000"}},
			{Topic: s.env.UniqueName("missing"), Set: map[string]string{"retention.ms": "7200000"}},
			{Topic: topics[1], Set: map[string]string{"retention.ms": "10800000"}},
		},
		Confirm: true,
	})

	s.Require().NoError(err, "a structurally valid batch must return per-item results even when one item fails")
	s.Require().Len(out.Results, 3, "every item must have a result in input order")
	s.Require().Equal(2, out.Succeeded, "the two valid topics must be changed")
	s.Require().Equal(1, out.Failed, "the missing topic must be one failed item, not a failed call")
	s.Require().Equal("7200000", s.env.TopicConfig(s.T(), topics[0], "retention.ms"), "the first valid change must apply")
	s.Require().Equal("10800000", s.env.TopicConfig(s.T(), topics[1], "retention.ms"), "the change after a failed item must still apply")
}

func (s *AlterTopicConfigSuite) TestRefusesDuplicateTopics() {
	topic := s.env.CreateTopic(s.T(), "alter-duplicate")

	_, err := altertopicconfig.Run(s.T().Context(), s.env.ClusterClient(s.T(), false), altertopicconfig.Input{
		Items: []altertopicconfig.Item{
			{Topic: topic, Set: map[string]string{"retention.ms": "7200000"}},
			{Topic: topic, Set: map[string]string{"retention.ms": "3600000"}},
		},
		Confirm: true,
	})

	s.Require().Error(err, "two items for one topic would leave the result depending on order, so the batch is refused")
}
