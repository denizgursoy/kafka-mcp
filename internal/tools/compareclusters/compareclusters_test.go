package compareclusters_test

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/denizgursoy/kafka-mcp/internal/domain/config"
	"github.com/denizgursoy/kafka-mcp/internal/domain/kafkaclient"
	"github.com/denizgursoy/kafka-mcp/internal/domain/testenv"
	"github.com/denizgursoy/kafka-mcp/internal/tools/compareclusters"
)

type CompareClustersSuite struct {
	suite.Suite

	env   *testenv.Environment
	other *testenv.Environment
}

func TestCompareClustersSuite(t *testing.T) {
	suite.Run(t, new(CompareClustersSuite))
}

func (s *CompareClustersSuite) SetupSuite() {
	// Two real brokers, because a comparison that cannot cross a cluster
	// boundary proves nothing: two topics on one broker would compare equal
	// whatever the code did.
	s.env, s.other = testenv.StartPair(s.T())
}

func (s *CompareClustersSuite) TearDownSuite() {
	s.other.Stop()
	s.env.Stop()
}

// clusters builds a registry holding this suite's two brokers under the names a
// caller would pass.
func (s *CompareClustersSuite) clusters() *kafkaclient.Registry {
	s.T().Helper()

	registry, err := kafkaclient.NewRegistry(&config.Config{
		Clusters: map[string]*config.Cluster{
			"here":  {Name: "here", Brokers: []string{s.env.Broker()}},
			"there": {Name: "there", Brokers: []string{s.other.Broker()}},
		},
	})
	s.Require().NoError(err, "building the registry must succeed")

	s.T().Cleanup(registry.Close)

	return registry
}

// compare runs one comparison against the other cluster and returns that item's
// result. A single comparison is an items array of length one.
func (s *CompareClustersSuite) compare(item compareclusters.Item) compareclusters.Output {
	s.T().Helper()

	out, err := compareclusters.Run(
		s.T().Context(), s.clusters(), "here",
		compareclusters.Input{Items: []compareclusters.Item{item}},
	)

	s.Require().NoError(err, "a structurally valid comparison must succeed")
	s.Require().Len(out.Results, 1,
		"one item in must produce exactly one result out, or results cannot be matched to inputs by position")
	s.Require().Empty(out.Results[0].Error,
		"comparing two reachable clusters must not produce an item error")

	return *out.Results[0].Result
}

// names reduces topic reports to their names, so a test asserts on membership
// rather than on the shape of each entry.
func names(topics []compareclusters.Topic) []string {
	got := make([]string, 0, len(topics))

	for _, topic := range topics {
		got = append(got, topic.Topic)
	}

	return got
}

func (s *CompareClustersSuite) TestReportsTopicsMissingOnEachSide() {
	shared := s.env.UniqueName("compare-shared")
	onlyHere := s.env.UniqueName("compare-only-here")
	onlyThere := s.env.UniqueName("compare-only-there")

	s.env.CreateNamedTopic(s.T(), shared, 1, nil)
	s.other.CreateNamedTopic(s.T(), shared, 1, nil)
	s.env.CreateNamedTopic(s.T(), onlyHere, 1, nil)
	s.other.CreateNamedTopic(s.T(), onlyThere, 1, nil)

	out := s.compare(compareclusters.Item{Cluster: "there", Search: "compare-only"})

	s.Run("the direction is named", func() {
		s.Require().Equal("here", out.Here,
			"a comparison is asymmetric, so the response must say which cluster 'only_here' refers to")
		s.Require().Equal("there", out.There,
			"the compared cluster must be named, since a batch compares several")
	})

	s.Run("a topic only this cluster has is reported", func() {
		s.Require().Contains(names(out.OnlyHere), onlyHere,
			"a topic this cluster has and the other lacks is half the answer a drift report exists to give")
		s.Require().NotContains(names(out.OnlyThere), onlyHere,
			"a topic cannot be missing from the cluster that holds it")
	})

	s.Run("a topic only the other cluster has is reported", func() {
		s.Require().Contains(names(out.OnlyThere), onlyThere,
			"these are the topics a caller would create here, which is the whole point of asking")
	})

	s.Run("the search filter is applied to both sides", func() {
		s.Require().NotContains(names(out.OnlyHere), shared,
			"a topic both clusters hold is not missing anywhere")
		s.Require().NotContains(names(out.OnlyThere), shared,
			"a shared topic must not appear as missing on either side")
	})
}

func (s *CompareClustersSuite) TestMissingTopicsCarryTheirShape() {
	topic := s.env.UniqueName("compare-shape")

	s.other.CreateNamedTopic(s.T(), topic, 3, map[string]string{"retention.ms": "60000"})

	out := s.compare(compareclusters.Item{Cluster: "there", Search: "compare-shape"})

	s.Require().Len(out.OnlyThere, 1, "exactly the one topic created on the other cluster must be reported")

	missing := out.OnlyThere[0]

	s.Run("the partition count comes from the cluster that has it", func() {
		s.Require().Equal(3, missing.Partitions,
			"recreating a topic with the wrong partition count is not recreating it, and the count can never be reduced afterwards")
	})

	s.Run("the explicit configs come with it", func() {
		s.Require().Equal("60000", missing.Configs["retention.ms"],
			"a topic recreated without its retention keeps data for a different length of time, which is a different topic in practice")
	})

	s.Run("the replication factor comes with it", func() {
		s.Require().Equal(1, missing.ReplicationFactor,
			"the replication factor is part of the shape a caller has to reproduce")
	})
}

func (s *CompareClustersSuite) TestReportsDifferingPartitionCounts() {
	topic := s.env.UniqueName("compare-partitions")

	s.env.CreateNamedTopic(s.T(), topic, 1, nil)
	s.other.CreateNamedTopic(s.T(), topic, 3, nil)

	out := s.compare(compareclusters.Item{Cluster: "there", Search: "compare-partitions"})

	s.Require().Len(out.Differing, 1,
		"a topic both clusters hold with different partition counts is drift, not agreement")

	drift := out.Differing[0]

	s.Run("both counts are reported", func() {
		s.Require().Equal(1, drift.Here.Partitions,
			"this cluster's count must be reported, or a reader cannot tell which side to change")
		s.Require().Equal(3, drift.There.Partitions,
			"the other cluster's count is what this side would have to match")
	})

	s.Run("the reason is named", func() {
		s.Require().Contains(drift.Differences, "partitions",
			"naming what differs is what makes the report actionable rather than merely alarming")
	})

	s.Run("it is not reported as missing", func() {
		s.Require().NotContains(names(out.OnlyHere), topic,
			"a topic that exists on both sides must never be listed as absent from one")
		s.Require().NotContains(names(out.OnlyThere), topic,
			"reporting a present topic as missing would invite creating a topic that already exists")
	})
}

func (s *CompareClustersSuite) TestReportsDifferingConfigs() {
	topic := s.env.UniqueName("compare-configs")

	s.env.CreateNamedTopic(s.T(), topic, 1, map[string]string{"retention.ms": "60000"})
	s.other.CreateNamedTopic(s.T(), topic, 1, map[string]string{"retention.ms": "120000"})

	out := s.compare(compareclusters.Item{Cluster: "there", Search: "compare-configs"})

	s.Require().Len(out.Differing, 1,
		"the same topic configured differently on two clusters is exactly the drift that makes a bug reproduce on one and not the other")

	drift := out.Differing[0]

	s.Require().Equal("60000", drift.Here.Configs["retention.ms"],
		"this side's value must be reported")
	s.Require().Equal("120000", drift.There.Configs["retention.ms"],
		"the other side's value must be reported, so the difference can be judged rather than guessed")
	s.Require().Contains(drift.Differences, "retention.ms",
		"the differing key must be named, because a topic has dozens of configs and a reader must not have to diff them by eye")
}

func (s *CompareClustersSuite) TestIdenticalTopicsAreNotReportedAsDrift() {
	topic := s.env.UniqueName("compare-identical")

	s.env.CreateNamedTopic(s.T(), topic, 2, map[string]string{"retention.ms": "60000"})
	s.other.CreateNamedTopic(s.T(), topic, 2, map[string]string{"retention.ms": "60000"})

	out := s.compare(compareclusters.Item{Cluster: "there", Search: "compare-identical"})

	s.Run("no drift is reported", func() {
		s.Require().Empty(out.Differing,
			"two topics configured the same way must compare equal, or every report is noise and the real differences are lost in it")
	})

	s.Run("the shared topic is counted", func() {
		s.Require().Equal(1, out.InBoth,
			"a count says how much was compared; the names are omitted because a cluster may share hundreds")
	})
}

func (s *CompareClustersSuite) TestExcludesInternalTopicsByDefault() {
	// A fresh broker holds no internal topics at all, so this asserts the rule
	// rather than the presence of any particular one: whatever the broker marks
	// internal must not appear.
	out := s.compare(compareclusters.Item{Cluster: "there"})

	s.Require().NotContains(names(out.OnlyHere), "__consumer_offsets",
		"an internal topic must be excluded by default: it exists on every cluster and is never a difference worth acting on")
	s.Require().NotContains(names(out.OnlyThere), "__consumer_offsets",
		"the same exclusion must apply to the cluster being compared against")
}

func (s *CompareClustersSuite) TestComparesATopicWhoseNameLooksInternal() {
	// The internal flag comes from the broker rather than from guessing at a
	// leading underscore, so a user topic named with one must still be compared.
	// Silently skipping it would hide a real difference.
	topic := s.env.UniqueName("_compare-underscore")

	s.env.CreateNamedTopic(s.T(), topic, 1, nil)

	out := s.compare(compareclusters.Item{Cluster: "there", Search: "_compare-underscore"})

	s.Require().Contains(names(out.OnlyHere), topic,
		"a leading underscore is a naming convention, not the broker's internal flag, so such a topic must still be reported")
}

func (s *CompareClustersSuite) TestReportsBrokerCounts() {
	out := s.compare(compareclusters.Item{Cluster: "there", Search: "compare-brokers-none"})

	s.Require().Equal(1, out.HereCluster.Brokers,
		"the broker count bounds the replication factor a topic can have, so it is part of knowing whether a topic can be recreated at all")
	s.Require().Equal(1, out.ThereCluster.Brokers,
		"the other cluster's broker count must be reported for the same reason")
}

func (s *CompareClustersSuite) TestComparesSeveralClustersInOneCall() {
	out, err := compareclusters.Run(
		s.T().Context(), s.clusters(), "here",
		compareclusters.Input{Items: []compareclusters.Item{
			{Cluster: "there", Search: "compare-batch"},
			{Cluster: "here", Search: "compare-batch"},
		}},
	)

	s.Require().NoError(err, "a valid batch must succeed")
	s.Require().Equal(2, out.Succeeded, "both comparisons must be reported")
	s.Require().Equal("there", out.Results[0].Result.There,
		"results must stay in input order, or each comparison is attributed to the wrong cluster")
	s.Require().Equal("here", out.Results[1].Result.There,
		"comparing a cluster with itself is allowed and must report no difference")
	s.Require().Empty(out.Results[1].Result.Differing,
		"a cluster cannot differ from itself, and saying otherwise would mean the comparison is not reading what it claims")
}

func (s *CompareClustersSuite) TestErrorsOnAnUnknownCluster() {
	out, err := compareclusters.Run(
		s.T().Context(), s.clusters(), "here",
		compareclusters.Input{Items: []compareclusters.Item{{Cluster: "never-configured"}}},
	)

	s.Require().NoError(err,
		"an unknown cluster is one item's problem, not a failure of the whole call")
	s.Require().NotEmpty(out.Results[0].Error,
		"the item must carry the error, so a batch's other comparisons still return")
	s.Require().Contains(out.Results[0].Error, "never-configured",
		"the error must quote the unknown name, since a typo is the likeliest cause")
}

func (s *CompareClustersSuite) TestItemErrorDoesNotHideSuccesses() {
	out, err := compareclusters.Run(
		s.T().Context(), s.clusters(), "here",
		compareclusters.Input{Items: []compareclusters.Item{
			{Cluster: "never-configured"},
			{Cluster: "there", Search: "compare-partial"},
		}},
	)

	s.Require().NoError(err, "one bad item is data in the response")
	s.Require().Equal(1, out.Failed, "the unknown cluster must be counted as failed")
	s.Require().Equal(1, out.Succeeded, "the valid comparison must still be reported")
	s.Require().NotNil(out.Results[1].Result,
		"a later valid item must still carry its result after an earlier failure")
}

func (s *CompareClustersSuite) TestRejectsAnEmptyItemList() {
	_, err := compareclusters.Run(
		s.T().Context(), s.clusters(), "here",
		compareclusters.Input{Items: []compareclusters.Item{}},
	)

	s.Require().Error(err,
		"an empty items array means the intended comparisons were lost before the call, which is a caller mistake rather than a no-op")
}

func (s *CompareClustersSuite) TestRequiresAClusterName() {
	out, err := compareclusters.Run(
		s.T().Context(), s.clusters(), "here",
		compareclusters.Input{Items: []compareclusters.Item{{Search: "anything"}}},
	)

	s.Require().NoError(err, "a missing name is one item's problem")
	s.Require().NotEmpty(out.Results[0].Error,
		"an item without a cluster names nothing to compare against and must say so rather than silently comparing with itself")
}
