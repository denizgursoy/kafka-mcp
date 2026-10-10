package consumerlag_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/denizgursoy/kafka-mcp/internal/domain/testenv"
	"github.com/denizgursoy/kafka-mcp/internal/tools/consumerlag"
)

type ConsumerLagSuite struct {
	suite.Suite

	env *testenv.Environment
}

func TestConsumerLagSuite(t *testing.T) {
	suite.Run(t, new(ConsumerLagSuite))
}

func (s *ConsumerLagSuite) SetupSuite() {
	s.env = testenv.Start(s.T())
}

func (s *ConsumerLagSuite) TearDownSuite() {
	s.env.Stop()
}

// lag measures one item and returns that item's result.
//
// Every call is a batch, so a single measurement is an items array of length
// one, and a failure for it arrives as the item's error rather than as an error
// for the call.
func (s *ConsumerLagSuite) lag(item consumerlag.Item) (consumerlag.Output, error) {
	s.T().Helper()

	return s.lagOn(s.env.Admin(), item)
}

func (s *ConsumerLagSuite) lagOn(
	admin *kadm.Client,
	item consumerlag.Item,
) (consumerlag.Output, error) {
	s.T().Helper()

	out, err := consumerlag.Run(
		s.T().Context(), admin, s.env.Reader(),
		consumerlag.Input{Items: []consumerlag.Item{item}},
	)
	if err != nil {
		return consumerlag.Output{}, err
	}

	s.Require().Len(out.Results, 1,
		"one item in must produce exactly one result out, or results cannot be matched to inputs by position")

	if out.Results[0].Error != "" {
		return consumerlag.Output{}, errors.New(out.Results[0].Error)
	}

	return *out.Results[0].Result, nil
}

// messages returns n simple messages, so a test states only how many it needs.
func (s *ConsumerLagSuite) messages(n int) []testenv.Message {
	s.T().Helper()

	out := make([]testenv.Message, 0, n)

	for i := 0; i < n; i++ {
		out = append(out, testenv.Message{Value: "message"})
	}

	return out
}

func (s *ConsumerLagSuite) TestReportsLagOfAPartiallyConsumedTopic() {
	topic := s.env.CreateTopic(s.T(), "lag-partial")

	s.env.Produce(s.T(), topic, s.messages(10)...)

	group := s.env.UniqueName("lag-partial-group")
	s.env.ConsumeAndCommit(s.T(), topic, group, 4)

	out, err := s.lag(consumerlag.Item{Topic: topic, Group: group, SkipConsumeRate: true})

	s.Require().NoError(err, "measuring lag for an existing group must succeed")
	s.Require().Len(out.Groups, 1,
		"exactly the requested group must be reported")

	measured := out.Groups[0]

	s.Run("total lag is what remains unconsumed", func() {
		s.Require().EqualValues(6, measured.Lag,
			"ten messages were produced and four committed, so six remain: this is the number the whole tool exists to report")
	})

	s.Run("the partition offsets explain the lag", func() {
		s.Require().Len(measured.Partitions, 1,
			"the single partition must be reported so a caller can see where the lag sits")

		partition := measured.Partitions[0]

		s.Require().EqualValues(4, partition.CommittedOffset,
			"the committed offset must be where the consumer stopped, which is what lag is measured from")
		s.Require().EqualValues(10, partition.EndOffset,
			"the end offset must be the topic's end, which is what lag is measured to")
		s.Require().EqualValues(6, partition.Lag,
			"the partition lag must be the difference between the two offsets reported beside it")
	})
}

func (s *ConsumerLagSuite) TestCaughtUpGroupReportsZeroLag() {
	topic := s.env.CreateTopic(s.T(), "lag-caught-up")

	s.env.Produce(s.T(), topic, s.messages(3)...)

	group := s.env.UniqueName("lag-caught-up-group")
	s.env.ConsumeAndCommit(s.T(), topic, group, 3)

	out, err := s.lag(consumerlag.Item{Topic: topic, Group: group, SkipConsumeRate: true})

	s.Require().NoError(err, "measuring a caught-up group must succeed")
	s.Require().Zero(out.Groups[0].Lag,
		"every message was consumed, so there is nothing left to be behind on")
	s.Require().Equal("caught_up", out.Groups[0].Status,
		"a caught-up group must be named as such rather than given an estimate of zero seconds")
	s.Require().Nil(out.Groups[0].ETASeconds,
		"there is nothing to wait for when the lag is zero, so no estimate may be offered")
}

func (s *ConsumerLagSuite) TestGroupWithNoActiveMembersIsReportedAsSuch() {
	topic := s.env.CreateTopic(s.T(), "lag-empty-group")

	s.env.Produce(s.T(), topic, s.messages(10)...)

	group := s.env.UniqueName("lag-empty-group")
	s.env.ConsumeAndCommit(s.T(), topic, group, 2)

	out, err := s.lag(consumerlag.Item{Topic: topic, Group: group, SampleSeconds: 1})

	s.Require().NoError(err, "measuring a group whose consumers stopped must succeed")

	measured := out.Groups[0]

	s.Run("the lag is still reported", func() {
		s.Require().EqualValues(8, measured.Lag,
			"committed offsets outlive the consumers that made them, so the lag is real even with nobody consuming")
	})

	s.Run("the state explains why nothing is draining", func() {
		s.Require().Equal("Empty", measured.State,
			"a group whose members have left is Empty, which is what explains a lag that never shrinks")
		s.Require().Equal("no_active_consumers", measured.Status,
			"with no members there is nothing to drain the lag, and saying so is more useful than an impossible estimate")
	})

	s.Run("no completion time is invented", func() {
		s.Require().Nil(measured.ETASeconds,
			"dividing by a consume rate of zero would be infinite, so no estimate may be reported at all")
	})
}

func (s *ConsumerLagSuite) TestReportsProduceRateOverRealWindows() {
	topic := s.env.CreateTopic(s.T(), "lag-produce-rate")

	s.env.Produce(s.T(), topic, s.messages(20)...)

	group := s.env.UniqueName("lag-produce-rate-group")
	s.env.ConsumeAndCommit(s.T(), topic, group, 5)

	out, err := s.lag(consumerlag.Item{Topic: topic, Group: group, SkipConsumeRate: true})

	s.Require().NoError(err, "measuring produce rate must succeed")

	s.Run("the last hour counts every message", func() {
		s.Require().EqualValues(20, out.ProduceRate.LastHour.Messages,
			"all twenty messages were produced within the last hour, so the hourly window must account for them")
	})

	s.Run("rates are reported per second, minute and hour", func() {
		s.Require().Positive(out.ProduceRate.LastHour.PerHour,
			"an hourly rate must be reported, since that is the window a caller judges sustained load by")
		s.Require().Positive(out.ProduceRate.LastMinute.PerMinute,
			"a per-minute rate must be reported, which is what shows a recent burst against a calm hourly average")
	})

	s.Run("a window longer than the topic is flagged", func() {
		s.Require().True(out.ProduceRate.LastHour.WindowTruncated,
			"this topic is seconds old, so an hourly rate divided by a full hour would understate it by orders of magnitude and must be marked")
	})
}

func (s *ConsumerLagSuite) TestConsumeRateOfAStoppedGroupIsZero() {
	topic := s.env.CreateTopic(s.T(), "lag-consume-rate")

	s.env.Produce(s.T(), topic, s.messages(10)...)

	group := s.env.UniqueName("lag-consume-rate-group")
	s.env.ConsumeAndCommit(s.T(), topic, group, 3)

	out, err := s.lag(consumerlag.Item{Topic: topic, Group: group, SampleSeconds: 1})

	s.Require().NoError(err, "sampling the consume rate must succeed")

	measured := out.Groups[0]

	s.Require().NotNil(measured.ConsumeRate,
		"a consume rate must be reported whenever it was sampled, even when it turns out to be zero")
	s.Require().Zero(measured.ConsumeRate.PerSecond,
		"no consumer is running, so no messages were consumed during the sample")
	s.Require().EqualValues(1, measured.ConsumeRate.SampledSeconds,
		"the sample window must be reported, because a rate means nothing without the window it was measured over")
	s.Require().True(measured.ConsumeRate.SampleInconclusive,
		"nothing moved during the sample, and that must be distinguished from a measured rate of genuinely zero throughput")
}

func (s *ConsumerLagSuite) TestSkippingTheSampleReturnsNoConsumeRate() {
	topic := s.env.CreateTopic(s.T(), "lag-skip-sample")

	s.env.Produce(s.T(), topic, s.messages(5)...)

	group := s.env.UniqueName("lag-skip-sample-group")
	s.env.ConsumeAndCommit(s.T(), topic, group, 1)

	out, err := s.lag(consumerlag.Item{Topic: topic, Group: group, SkipConsumeRate: true})

	s.Require().NoError(err, "skipping the sample must succeed")
	s.Require().Nil(out.Groups[0].ConsumeRate,
		"no rate may be reported when none was measured, rather than a zero that looks like a stalled consumer")
	s.Require().Equal("not_measured", out.Groups[0].Status,
		"without a consume rate no completion can be estimated, and the caller must be told that is why")
}

func (s *ConsumerLagSuite) TestReportsLagAcrossPartitions() {
	topic := s.env.CreateTopicWithPartitions(s.T(), "lag-partitions", 3)

	s.env.Produce(s.T(), topic,
		testenv.Message{Value: "p0-a", Partition: 0},
		testenv.Message{Value: "p0-b", Partition: 0},
		testenv.Message{Value: "p2-a", Partition: 2},
	)

	group := s.env.UniqueName("lag-partitions-group")
	s.env.ConsumeAndCommit(s.T(), topic, group, 1)

	out, err := s.lag(consumerlag.Item{Topic: topic, Group: group, SkipConsumeRate: true})

	s.Require().NoError(err, "measuring lag across partitions must succeed")

	measured := out.Groups[0]

	s.Require().EqualValues(2, measured.Lag,
		"three messages were produced and one committed, so two remain across the partitions")

	ids := make([]int32, 0, len(measured.Partitions))
	for _, partition := range measured.Partitions {
		ids = append(ids, partition.Partition)
	}

	s.Require().IsIncreasing(ids,
		"partitions must be sorted by id, because kadm returns maps and Go map order is random")
}

func (s *ConsumerLagSuite) TestMeasuresEveryGroupWhenNoneIsNamed() {
	topic := s.env.CreateTopic(s.T(), "lag-all-groups")

	s.env.Produce(s.T(), topic, s.messages(6)...)

	first := s.env.UniqueName("lag-all-first")
	second := s.env.UniqueName("lag-all-second")

	s.env.ConsumeAndCommit(s.T(), topic, first, 1)
	s.env.ConsumeAndCommit(s.T(), topic, second, 4)

	out, err := s.lag(consumerlag.Item{Topic: topic, SkipConsumeRate: true})

	s.Require().NoError(err, "measuring every group on a topic must succeed")
	s.Require().Len(out.Groups, 2,
		"both groups consuming this topic must be measured when no single group was named")

	byName := make(map[string]int64, 2)
	for _, group := range out.Groups {
		byName[group.Group] = group.Lag
	}

	s.Require().EqualValues(5, byName[first],
		"the group that committed one of six messages is five behind")
	s.Require().EqualValues(2, byName[second],
		"the group that committed four of six messages is two behind")
}

func (s *ConsumerLagSuite) TestFlagsACommitThatRetentionDeleted() {
	topic := s.env.CreateTopic(s.T(), "lag-expired")
	s.env.Produce(s.T(), topic, s.messages(10)...)

	group := s.env.UniqueName("lag-expired-group")
	s.env.ConsumeAndCommit(s.T(), topic, group, 2)
	s.env.TruncateBefore(s.T(), topic, 0, 6)

	out, err := s.lag(consumerlag.Item{Topic: topic, Group: group, SkipConsumeRate: true})
	s.Require().NoError(err, "measuring a group whose commit was deleted must succeed")

	partition := out.Groups[0].Partitions[0]

	s.Run("the log start is reported", func() {
		s.Require().EqualValues(6, partition.StartOffset,
			"without the start offset a caller cannot tell a large lag from a position that no longer exists")
	})
	s.Run("the expired commit is flagged", func() {
		s.Require().True(partition.OffsetExpired,
			"a commit below the log start means the consumer will hit OFFSET_OUT_OF_RANGE and reset, which is a different problem from being slow")
	})
	s.Run("the group carries a warning", func() {
		s.Require().NotEmpty(out.Groups[0].Warnings,
			"an expired position decides what happens on the next restart, so it must not hide in a per-partition flag")
	})
}

func (s *ConsumerLagSuite) TestALiveCommitIsNotFlagged() {
	topic := s.env.CreateTopic(s.T(), "lag-not-expired")
	s.env.Produce(s.T(), topic, s.messages(5)...)

	group := s.env.UniqueName("lag-not-expired-group")
	s.env.ConsumeAndCommit(s.T(), topic, group, 2)

	out, err := s.lag(consumerlag.Item{Topic: topic, Group: group, SkipConsumeRate: true})
	s.Require().NoError(err, "measuring lag must succeed")

	s.Require().False(out.Groups[0].Partitions[0].OffsetExpired,
		"a commit inside the retained log must not be reported as lost")
}

func (s *ConsumerLagSuite) TestMeasuresBacklogAge() {
	old := time.Now().Add(-2 * time.Hour).Truncate(time.Millisecond)
	topic := s.env.CreateTopicWithConfig(s.T(), "lag-age", map[string]string{"retention.ms": "10800000"})
	s.env.Produce(s.T(), topic,
		testenv.Message{Value: "consumed", Timestamp: old.Add(-time.Minute)},
		testenv.Message{Value: "oldest unconsumed", Timestamp: old},
		testenv.Message{Value: "newer", Timestamp: time.Now()},
	)

	group := s.env.UniqueName("lag-age-group")
	s.env.ConsumeAndCommit(s.T(), topic, group, 1)

	out, err := s.lag(consumerlag.Item{Topic: topic, Group: group, SkipConsumeRate: true, MeasureBacklogAge: true})
	s.Require().NoError(err, "measuring backlog age must succeed")

	measured := out.Groups[0]
	partition := measured.Partitions[0]

	s.Run("the next unconsumed message's time is reported", func() {
		s.Require().NotNil(partition.CommittedTimestamp, "the age is read from the message at the committed offset")
		s.Require().True(old.Equal(*partition.CommittedTimestamp),
			"the committed offset points at the next message to consume, whose timestamp is the backlog's start")
	})
	s.Run("the age is in seconds", func() {
		s.Require().NotNil(partition.LagSeconds, "a lag in messages says nothing about time; the age is what retention compares against")
		s.Require().InDelta(7200, *partition.LagSeconds, 120, "the oldest unconsumed message is two hours old")
	})
	s.Run("the group reports its oldest unconsumed message", func() {
		s.Require().NotNil(measured.OldestUnconsumedAt, "the group's age is its oldest partition")
		s.Require().True(old.Equal(*measured.OldestUnconsumedAt), "the oldest partition decides")
	})
	s.Run("retention risk compares age with retention", func() {
		s.Require().NotNil(out.RetentionMs, "the topic's retention must be reported beside the age")
		s.Require().EqualValues(10800000, *out.RetentionMs, "the topic sets three hours")
		s.Require().True(measured.RetentionRisk,
			"two hours of a three-hour retention is past the warning threshold: the backlog will be deleted before it is consumed if nothing changes")
	})
}

func (s *ConsumerLagSuite) TestBacklogAgeIsOptional() {
	topic := s.env.CreateTopic(s.T(), "lag-age-off")
	s.env.Produce(s.T(), topic, s.messages(3)...)
	group := s.env.UniqueName("lag-age-off-group")
	s.env.ConsumeAndCommit(s.T(), topic, group, 1)

	out, err := s.lag(consumerlag.Item{Topic: topic, Group: group, SkipConsumeRate: true})
	s.Require().NoError(err, "measuring lag must succeed")

	s.Require().Nil(out.Groups[0].Partitions[0].LagSeconds,
		"reading a message per lagging partition costs a fetch each, so it happens only when asked for")
}

func (s *ConsumerLagSuite) TestReportsProduceRatePerPartition() {
	topic := s.env.CreateTopicWithPartitions(s.T(), "lag-partition-rate", 2)
	s.env.Produce(s.T(), topic,
		testenv.Message{Value: "a", Partition: 0},
		testenv.Message{Value: "b", Partition: 0},
		testenv.Message{Value: "c", Partition: 0},
		testenv.Message{Value: "d", Partition: 1},
	)

	out, err := s.lag(consumerlag.Item{Topic: topic, SkipConsumeRate: true})
	s.Require().NoError(err, "measuring produce rate must succeed")

	s.Require().Len(out.ProduceRate.Partitions, 2, "every partition must be reported, sorted")
	s.Require().EqualValues(0, out.ProduceRate.Partitions[0].Partition, "partitions must be sorted")
	s.Require().EqualValues(3, out.ProduceRate.Partitions[0].LastHour,
		"a per-partition count is what tells key skew (one partition gets the traffic) from a slow consumer")
	s.Require().EqualValues(1, out.ProduceRate.Partitions[1].LastHour, "the other partition's count must be its own")
}

func (s *ConsumerLagSuite) TestErrorsOnUnknownTopic() {
	_, err := s.lag(consumerlag.Item{Topic: s.env.UniqueName("missing"), SkipConsumeRate: true})

	s.Require().Error(err,
		"a topic that does not exist must fail rather than report a topic nobody is behind on")
}

func (s *ConsumerLagSuite) TestRejectsAnUnboundedSampleWindow() {
	_, err := s.lag(consumerlag.Item{Topic: "t", SampleSeconds: 1 << 30})

	s.Require().ErrorContains(err, "sample_seconds",
		"the call blocks for the whole window, so an unbounded one holds the request open indefinitely")
}

func (s *ConsumerLagSuite) TestErrorsOnUnknownGroup() {
	topic := s.env.CreateTopic(s.T(), "lag-unknown-group")

	s.env.Produce(s.T(), topic, s.messages(2)...)

	_, err := s.lag(consumerlag.Item{
		Topic:           topic,
		Group:           s.env.UniqueName("never-existed"),
		SkipConsumeRate: true,
	})

	s.Require().Error(err,
		"naming a group that does not exist must fail, not silently report zero lag as though it were caught up")
}

func (s *ConsumerLagSuite) TestErrorsWhenBrokerUnreachable() {
	client, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
	s.Require().NoError(err, "building a client against a dead address must not fail yet")

	s.T().Cleanup(client.Close)

	_, err = s.lagOn(
		kadm.NewClient(client),
		consumerlag.Item{Topic: "anything", SkipConsumeRate: true},
	)

	s.Require().Error(err,
		"an unreachable broker must surface as that item's error, not as a topic with no lag")
}

func (s *ConsumerLagSuite) TestBatchMeasuresSeveralTopics() {
	first := s.env.CreateTopic(s.T(), "lag-batch-first")
	second := s.env.CreateTopic(s.T(), "lag-batch-second")
	s.env.Produce(s.T(), first, s.messages(3)...)
	s.env.Produce(s.T(), second, s.messages(5)...)
	firstGroup := s.env.UniqueName("lag-batch-first-group")
	secondGroup := s.env.UniqueName("lag-batch-second-group")
	s.env.ConsumeAndCommit(s.T(), first, firstGroup, 1)
	s.env.ConsumeAndCommit(s.T(), second, secondGroup, 2)

	out, err := consumerlag.Run(s.T().Context(), s.env.Admin(), s.env.Reader(), consumerlag.Input{
		Items: []consumerlag.Item{
			{Topic: first, Group: firstGroup, SkipConsumeRate: true},
			{Topic: second, Group: secondGroup, SkipConsumeRate: true},
		},
	})

	s.Require().NoError(err, "measuring a valid topic batch must succeed")
	s.Require().EqualValues(2, out.Results[0].Result.TotalLag, "the first result must contain the first topic's lag")
	s.Require().EqualValues(3, out.Results[1].Result.TotalLag, "the second result must contain the second topic's lag")
}
