package describetopic_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/denizgursoy/kafka-mcp/internal/domain/records"
	"github.com/denizgursoy/kafka-mcp/internal/domain/testenv"
	"github.com/denizgursoy/kafka-mcp/internal/tools/describetopic"
)

type DescribeTopicSuite struct {
	suite.Suite

	env *testenv.Environment
}

func TestDescribeTopicSuite(t *testing.T) {
	suite.Run(t, new(DescribeTopicSuite))
}

func (s *DescribeTopicSuite) SetupSuite() {
	s.env = testenv.Start(s.T())
}

func (s *DescribeTopicSuite) TearDownSuite() {
	s.env.Stop()
}

func (s *DescribeTopicSuite) TestReportsOffsetsAndCounts() {
	topic := s.env.CreateTopic(s.T(), "describe-counts")

	s.env.Produce(s.T(), topic,
		testenv.Message{Value: "first"},
		testenv.Message{Value: "second"},
		testenv.Message{Value: "third"},
	)

	out, err := describetopic.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		describetopic.Input{Topic: topic},
	)

	s.Require().NoError(err, "describing an existing topic must succeed")
	s.Require().Equal(topic, out.Topic,
		"the report must name the topic that was described")
	s.Require().Len(out.Partitions, 1,
		"a single-partition topic must report exactly one partition")
	s.Require().EqualValues(3, out.MessageCount,
		"three produced messages must be counted, so a caller can judge how big a search will be")

	partition := out.Partitions[0]

	s.Require().EqualValues(0, partition.StartOffset,
		"nothing was deleted, so the partition must still start at offset 0")
	s.Require().EqualValues(3, partition.EndOffset,
		"the end offset must be the offset the next message will get, one past the last message")
	s.Require().EqualValues(3, partition.MessageCount,
		"message count must be end minus start, which is what a search would have to scan")
}

func (s *DescribeTopicSuite) TestReportsEveryPartitionSorted() {
	topic := s.env.CreateTopicWithPartitions(s.T(), "describe-partitions", 3)

	s.env.Produce(s.T(), topic,
		testenv.Message{Value: "p0", Partition: 0},
		testenv.Message{Value: "p2-a", Partition: 2},
		testenv.Message{Value: "p2-b", Partition: 2},
	)

	out, err := describetopic.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		describetopic.Input{Topic: topic},
	)

	s.Require().NoError(err, "describing a multi-partition topic must succeed")
	s.Require().Len(out.Partitions, 3,
		"every partition must be reported, so a caller can target the right one")

	ids := make([]int32, 0, len(out.Partitions))
	for _, partition := range out.Partitions {
		ids = append(ids, partition.Partition)
	}

	s.Require().IsIncreasing(ids,
		"partitions must be sorted by id, because kadm returns maps and Go map order is random")

	s.Require().EqualValues(1, out.Partitions[0].MessageCount,
		"partition 0 received exactly one message")
	s.Require().EqualValues(0, out.Partitions[1].MessageCount,
		"partition 1 received no messages and must report an empty count, not be omitted")
	s.Require().EqualValues(2, out.Partitions[2].MessageCount,
		"partition 2 received exactly two messages")
	s.Require().EqualValues(3, out.MessageCount,
		"the topic total must be the sum across partitions")
}

func (s *DescribeTopicSuite) TestReportsTimestampRange() {
	topic := s.env.CreateTopic(s.T(), "describe-timestamps")

	oldest := time.Now().Add(-2 * time.Hour).Truncate(time.Millisecond)
	newest := time.Now().Add(-1 * time.Hour).Truncate(time.Millisecond)

	s.env.Produce(s.T(), topic,
		testenv.Message{Value: "old", Timestamp: oldest},
		testenv.Message{Value: "new", Timestamp: newest},
	)

	out, err := describetopic.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		describetopic.Input{Topic: topic},
	)

	s.Require().NoError(err, "describing a topic with timestamps must succeed")
	s.Require().NotNil(out.OldestTimestamp,
		"a non-empty topic must report its oldest timestamp, so a caller can pick a time window")
	s.Require().NotNil(out.NewestTimestamp,
		"a non-empty topic must report its newest timestamp, so a caller can pick a time window")
	s.Require().Equal(oldest.UnixMilli(), out.OldestTimestamp.UnixMilli(),
		"the oldest timestamp must be the first message's own timestamp, not the time the test ran")
	s.Require().Equal(newest.UnixMilli(), out.NewestTimestamp.UnixMilli(),
		"the newest timestamp must be the last message's own timestamp")
}

func (s *DescribeTopicSuite) TestEmptyTopicReportsZeroCount() {
	topic := s.env.CreateTopic(s.T(), "describe-empty")

	out, err := describetopic.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		describetopic.Input{Topic: topic},
	)

	s.Require().NoError(err, "an empty topic is a normal case, not an error")
	s.Require().Zero(out.MessageCount,
		"an empty topic must report zero messages so a caller does not bother searching it")
	s.Require().Len(out.Partitions, 1,
		"an empty topic still has partitions and must report them")
	s.Require().Equal(
		out.Partitions[0].StartOffset,
		out.Partitions[0].EndOffset,
		"in an empty partition the start and end offsets are equal, which is how emptiness is detected",
	)
	s.Require().Nil(out.OldestTimestamp,
		"an empty topic has no timestamps, and must say so rather than inventing a zero time")
}

func (s *DescribeTopicSuite) TestErrorsOnUnknownTopic() {
	_, err := describetopic.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		describetopic.Input{Topic: s.env.UniqueName("does-not-exist")},
	)

	s.Require().Error(err,
		"describing a topic that does not exist must fail loudly, not report an empty topic")
}

func (s *DescribeTopicSuite) TestErrorsWhenBrokerUnreachable() {
	client, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
	s.Require().NoError(err, "building a client against a dead address must not fail yet")

	s.T().Cleanup(client.Close)

	_, err = describetopic.Run(
		s.T().Context(),
		kadm.NewClient(client),
		records.NewReader("127.0.0.1:1"),
		describetopic.Input{Topic: "anything"},
	)

	s.Require().Error(err,
		"an unreachable broker must surface as an error, not as an empty description")
}

func (s *DescribeTopicSuite) configs(out describetopic.Output) map[string]describetopic.Config {
	s.T().Helper()

	byKey := make(map[string]describetopic.Config, len(out.Configs))

	for _, config := range out.Configs {
		byKey[config.Key] = config
	}

	return byKey
}

func (s *DescribeTopicSuite) TestReportsInheritedRetention() {
	topic := s.env.CreateTopic(s.T(), "describe-config-default")

	out, err := describetopic.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		describetopic.Input{Topic: topic},
	)

	s.Require().NoError(err, "describing a topic must also report its configuration")

	byKey := s.configs(out)

	s.Run("retention is reported", func() {
		retention, ok := byKey["retention.ms"]

		s.Require().True(ok,
			"retention.ms must be reported, because it is what decides how far back a search can possibly find anything")
		s.Require().NotEmpty(retention.Value,
			"a reported config must carry its value, or the caller learns nothing from it")
	})

	s.Run("an inherited config is marked as a default", func() {
		retention := byKey["retention.ms"]

		s.Require().True(retention.IsDefault,
			"this topic sets no retention of its own, so the value must be marked inherited rather than deliberate")
		s.Require().Equal("DEFAULT_CONFIG", retention.Source,
			"the source must be named, not returned as a bare enum number that no caller can interpret")
	})

	s.Run("the configs that change how a topic is searched are present", func() {
		for _, key := range []string{
			"cleanup.policy",
			"max.message.bytes",
			"retention.bytes",
			"segment.bytes",
		} {
			s.Require().Contains(byKey, key,
				"every config key must be returned, since the caller asked for the complete configuration")
		}
	})

	s.Run("configs are sorted by key", func() {
		keys := make([]string, 0, len(out.Configs))
		for _, config := range out.Configs {
			keys = append(keys, config.Key)
		}

		s.Require().IsIncreasing(keys,
			"configs must be sorted, because kadm returns them in no guaranteed order and unstable output confuses MCP clients")
	})
}

func (s *DescribeTopicSuite) TestReportsExplicitlySetRetention() {
	topic := s.env.CreateTopicWithConfig(s.T(), "describe-config-set", map[string]string{
		"retention.ms":   "60000",
		"cleanup.policy": "compact",
	})

	out, err := describetopic.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		describetopic.Input{Topic: topic},
	)

	s.Require().NoError(err, "describing a topic with its own configuration must succeed")

	byKey := s.configs(out)

	s.Run("the set value is reported", func() {
		s.Require().Equal("60000", byKey["retention.ms"].Value,
			"the value set on the topic must be reported, not the cluster default it overrides")
	})

	s.Run("a deliberately set config is not marked as a default", func() {
		retention := byKey["retention.ms"]

		s.Require().False(retention.IsDefault,
			"a config set on the topic is a deliberate choice, and reporting it as inherited would hide that")
		s.Require().Equal("DYNAMIC_TOPIC_CONFIG", retention.Source,
			"a topic-level config must be named as such, which is what distinguishes it from an inherited value")
	})

	s.Run("cleanup policy is reported", func() {
		s.Require().Equal("compact", byKey["cleanup.policy"].Value,
			"a compacted topic keeps only the latest value per key, so a caller must see this before concluding a message is missing")
	})
}
