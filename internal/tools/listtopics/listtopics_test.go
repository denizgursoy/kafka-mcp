package listtopics_test

import (
	"crypto/rand"
	"encoding/hex"
	"testing"
	"time"

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

// list runs the tool against this suite's broker.
func (s *ListTopicsSuite) list(input listtopics.Input) listtopics.Output {
	s.T().Helper()

	out, err := listtopics.Run(s.T().Context(), s.env.Admin(), input)
	s.Require().NoError(err, "listing topics on a healthy broker must succeed")

	return out
}

// names reduces the report to topic names, so a test asserts on membership
// rather than on the shape of each entry.
func names(out listtopics.Output) []string {
	got := make([]string, 0, len(out.Topics))

	for _, topic := range out.Topics {
		got = append(got, topic.Topic)
	}

	return got
}

func (s *ListTopicsSuite) TestReturnsAllTopics() {
	topics := s.env.CreateTopics(s.T(), "all-orders", "all-payments")

	out := s.list(listtopics.Input{})

	s.Require().Equal(len(out.Topics), out.Count,
		"count must report how many topics were returned")
	s.Require().Contains(names(out), topics[0],
		"an unfiltered listing must include every created topic")
	s.Require().Contains(names(out), topics[1],
		"an unfiltered listing must include every created topic")
	s.Require().IsIncreasing(names(out),
		"topics must be sorted by name, because kadm returns a map and Go map order is random")
}

func (s *ListTopicsSuite) TestReportsTheShapeOfEachTopic() {
	s.env.CreateNamedTopic(s.T(), s.env.UniqueName("shape-reported"), 3,
		map[string]string{"retention.ms": "60000"})

	out := s.list(listtopics.Input{
		Script: `return topic.indexOf("shape-reported") >= 0`,
	})

	s.Require().Len(out.Topics, 1, "exactly the one created topic must match")

	reported := out.Topics[0]

	s.Run("the partition count is reported", func() {
		s.Require().Equal(3, reported.Partitions,
			"the partition count is what a caller filters on, so returning it saves a describe_topic call per topic")
	})

	s.Run("the replication factor is reported", func() {
		s.Require().Equal(1, reported.ReplicationFactor,
			"a topic with one replica has no redundancy, which is worth seeing without a second call")
	})

	s.Run("the explicit configs are reported", func() {
		s.Require().Equal("60000", reported.Configs["retention.ms"],
			"a value the topic sets for itself must be reported, since it is what the caller filtered on")
	})

	s.Run("the topic is not marked internal", func() {
		s.Require().False(reported.Internal,
			"a topic created by a caller is not internal, and saying otherwise would hide it from default listings")
	})
}

func (s *ListTopicsSuite) TestReportsAndFiltersBySize() {
	big := s.env.CreateTopic(s.T(), "size-big")
	small := s.env.CreateTopic(s.T(), "size-small")
	s.env.Produce(s.T(), big, testenv.Message{Value: incompressible(20000)})

	var out listtopics.Output
	s.Require().Eventually(func() bool {
		out = s.list(listtopics.Input{Script: `return topic.indexOf("size-") >= 0 && size_bytes >= 20000`})
		return len(out.Topics) == 1
	}, 15*time.Second, 250*time.Millisecond,
		"size_bytes must be in the script's scope, so 'which topics are biggest' is one call")

	s.Require().Equal(big, out.Topics[0].Topic, "only the topic holding 20 KB passes the size filter")
	s.Require().GreaterOrEqual(out.Topics[0].SizeBytes, int64(20000), "the size is reported beside the name")
	s.Require().NotContains(names(out), small, "an empty topic is far below the threshold")
}

func (s *ListTopicsSuite) TestFiltersByNameWithAScript() {
	topics := s.env.CreateTopics(s.T(), "filter-ORDERS-created", "filter-payments-created")

	out := s.list(listtopics.Input{
		Script: `return topic.toLowerCase().indexOf("orders") >= 0`,
	})

	s.Require().Contains(names(out), topics[0],
		"a script matching the name must select that topic, which is what the removed substring filter used to do")
	s.Require().NotContains(names(out), topics[1],
		"a topic the predicate rejects must not be returned")
	s.Require().Equal(len(out.Topics), out.Count,
		"count must report how many topics were returned")
}

func (s *ListTopicsSuite) TestFiltersByPartitionCount() {
	small := s.env.CreateNamedTopic(s.T(), s.env.UniqueName("partitions-small"), 1, nil)
	large := s.env.CreateNamedTopic(s.T(), s.env.UniqueName("partitions-large"), 4, nil)

	out := s.list(listtopics.Input{
		Script: `return topic.indexOf("partitions-") >= 0 && partitions > 2`,
	})

	s.Require().Contains(names(out), large,
		"filtering on partition count is the thing a substring filter could never express, and is why a script exists here")
	s.Require().NotContains(names(out), small,
		"a topic below the threshold must be excluded, or the predicate is not being applied at all")
}

func (s *ListTopicsSuite) TestFiltersByConfig() {
	compacted := s.env.CreateNamedTopic(s.T(), s.env.UniqueName("config-compacted"), 1,
		map[string]string{"cleanup.policy": "compact"})
	plain := s.env.CreateNamedTopic(s.T(), s.env.UniqueName("config-plain"), 1, nil)

	out := s.list(listtopics.Input{
		Script: `return topic.indexOf("config-") >= 0 && configs["cleanup.policy"] === "compact"`,
	})

	s.Require().Contains(names(out), compacted,
		"a compacted topic keeps only the latest value per key, so finding them is a real audit question")
	s.Require().NotContains(names(out), plain,
		"a topic that never set the config must not match a predicate that reads it")
}

func (s *ListTopicsSuite) TestConfigsHoldOnlyExplicitValues() {
	s.env.CreateNamedTopic(s.T(), s.env.UniqueName("explicit-only"), 1,
		map[string]string{"retention.ms": "60000"})

	out := s.list(listtopics.Input{
		Script: `return topic.indexOf("explicit-only") >= 0`,
	})

	s.Require().Len(out.Topics, 1, "the created topic must match")

	configs := out.Topics[0].Configs

	s.Run("a value the topic sets is present", func() {
		s.Require().Contains(configs, "retention.ms",
			"a deliberate setting is what a caller asks about")
	})

	s.Run("an inherited default is absent", func() {
		s.Require().NotContains(configs, "compression.type",
			"inherited defaults are not choices anyone made, and including them would mean every topic looks configured")
	})
}

func (s *ListTopicsSuite) TestExcludesInternalTopicsFromTheDefaultView() {
	// The flag comes from the broker rather than from guessing at a leading
	// underscore, so a user topic named with one must still be listed.
	topic := s.env.CreateNamedTopic(s.T(), s.env.UniqueName("_underscore-named"), 1, nil)

	out := s.list(listtopics.Input{
		Script: `return topic.indexOf("_underscore-named") >= 0`,
	})

	s.Require().Contains(names(out), topic,
		"a leading underscore is a naming convention, not the broker's internal flag, so such a topic must still be returned")
}

func (s *ListTopicsSuite) TestReturnsEmptySliceWhenNothingMatches() {
	s.env.CreateTopic(s.T(), "empty-orders")

	out := s.list(listtopics.Input{
		Script: `return topic === "no-such-topic-anywhere"`,
	})

	s.Require().NotNil(out.Topics,
		"topics must be an empty slice, not nil, so the JSON output is [] and not null")
	s.Require().Empty(out.Topics,
		"no topic satisfies the predicate, so none may be returned")
	s.Require().Zero(out.Count,
		"count must be zero when no topic matches")
}

func (s *ListTopicsSuite) TestRejectsAScriptThatDoesNotCompile() {
	_, err := listtopics.Run(
		s.T().Context(),
		s.env.Admin(),
		listtopics.Input{Script: `return topic ===`},
	)

	s.Require().Error(err,
		"a malformed script must be refused before any topic is read, rather than failing once per topic")
}

func (s *ListTopicsSuite) TestRejectsAnUnboundedTimeout() {
	_, err := listtopics.Run(
		s.T().Context(),
		s.env.Admin(),
		listtopics.Input{Script: `return true`, TimeoutSecond: 1 << 30},
	)

	s.Require().ErrorContains(err, "timeout_seconds",
		"a timeout of years is no timeout, so a runaway predicate would hold the call that long")
}

func (s *ListTopicsSuite) TestCountsTopicsTheScriptThrowsOn() {
	s.env.CreateTopic(s.T(), "throwing-script")

	// Reading through a property that does not exist throws, which is a
	// different outcome from the topic not matching.
	out := s.list(listtopics.Input{
		Script: `return topic.nothing.here === 1`,
	})

	s.Run("no topic is returned", func() {
		s.Require().Empty(out.Topics,
			"a script that throws reached no verdict, so its topics must not be reported as matches")
	})

	s.Run("the failures are counted", func() {
		s.Require().NotZero(out.ScriptErrors,
			"a broken predicate must be distinguishable from a genuine absence of matches, or a caller concludes the cluster is empty")
	})
}

func (s *ListTopicsSuite) TestRunawayScriptStopsAtTheTimeout() {
	s.env.CreateTopic(s.T(), "runaway-script")

	// goja does not yield, so nothing outside the runtime observes the deadline
	// unless the script is interrupted. Without that, this call never returns.
	out, err := listtopics.Run(
		s.T().Context(),
		s.env.Admin(),
		listtopics.Input{Script: `while (true) {}`, TimeoutSecond: 1},
	)

	s.Require().NoError(err,
		"a timeout is a bounded outcome rather than a failure of the call")
	s.Require().NotZero(out.ScriptErrors,
		"an interrupted script must be counted as a failure, not silently treated as no match")
}

func (s *ListTopicsSuite) TestReturnsErrorWhenBrokerUnreachable() {
	client, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
	s.Require().NoError(err, "building a client against a dead address must not fail yet")

	s.T().Cleanup(client.Close)

	_, err = listtopics.Run(s.T().Context(), kadm.NewClient(client), listtopics.Input{})

	s.Require().Error(err,
		"an unreachable broker must surface as an error, not as an empty topic list")
}

// incompressible returns n bytes of random hex, so the size the brokers report
// is not shrunk by compression.
func incompressible(n int) string {
	raw := make([]byte, n/2)
	_, _ = rand.Read(raw)

	return hex.EncodeToString(raw)
}
