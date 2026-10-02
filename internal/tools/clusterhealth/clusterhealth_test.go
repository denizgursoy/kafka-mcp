package clusterhealth_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/denizgursoy/kafka-mcp/internal/domain/config"
	"github.com/denizgursoy/kafka-mcp/internal/domain/kafkaclient"
	"github.com/denizgursoy/kafka-mcp/internal/domain/testenv"
	"github.com/denizgursoy/kafka-mcp/internal/tools/clusterhealth"
)

type ClusterHealthSuite struct {
	suite.Suite

	env *testenv.Environment
}

func TestClusterHealthSuite(t *testing.T) {
	suite.Run(t, new(ClusterHealthSuite))
}

func (s *ClusterHealthSuite) SetupSuite() {
	s.env = testenv.Start(s.T())
}

func (s *ClusterHealthSuite) TearDownSuite() {
	s.env.Stop()
}

func (s *ClusterHealthSuite) TestHealthyClusterReportsBrokersAndNoProblems() {
	topic := s.env.CreateTopicWithPartitions(s.T(), "health-ok", 3)

	out, err := clusterhealth.Run(s.T().Context(), s.env.Admin(), clusterhealth.Input{Search: topic})
	s.Require().NoError(err, "checking a running cluster must succeed")

	s.Run("brokers and controller are reported", func() {
		s.Require().NotEmpty(out.ClusterID, "the cluster id tells the caller which cluster answered")
		s.Require().Len(out.Brokers, 1, "the test cluster has exactly one broker")
		s.Require().True(out.Brokers[0].Controller,
			"the only broker is the controller, and the caller must be able to see which broker holds that role")
		s.Require().Equal(3, out.Brokers[0].Leaders,
			"the broker leads every partition checked, and leader counts are how an unbalanced cluster shows up")
	})

	s.Run("the checked topic is healthy", func() {
		s.Require().Equal(1, out.Summary.Topics, "search must narrow the check to the one matching topic")
		s.Require().Equal(3, out.Summary.Partitions, "every partition of the matching topic must be checked")
		s.Require().Empty(out.Problems, "a single-replica topic with its leader up has nothing wrong with it")
		s.Require().NotNil(out.Problems, "no problems must be [] rather than null")
		s.Require().True(out.Healthy, "no problems and a controller means healthy")
	})

	s.Run("min.insync.replicas is either judged or declared unknown", func() {
		// Redpanda does not report min.insync.replicas the way Kafka does. The
		// tool must then say so rather than silently skip the check.
		if len(out.MinISRUnknown) > 0 {
			s.Require().Contains(out.MinISRUnknown, topic,
				"a topic whose min.insync.replicas was not reported must be named, so the missing check is visible")
		}
	})
}

func (s *ClusterHealthSuite) TestSearchIsCaseInsensitive() {
	topic := s.env.CreateTopic(s.T(), "health-case")

	out, err := clusterhealth.Run(s.T().Context(), s.env.Admin(), clusterhealth.Input{
		Search: strings.ToUpper(topic),
	})
	s.Require().NoError(err, "checking with a search must succeed")
	s.Require().Equal(1, out.Summary.Topics,
		"search is documented as case-insensitive, so an upper-case search must still find the topic")
}

func (s *ClusterHealthSuite) TestSearchThatMatchesNothingStillReportsBrokers() {
	out, err := clusterhealth.Run(s.T().Context(), s.env.Admin(), clusterhealth.Input{
		Search: s.env.UniqueName("no-such-topic"),
	})

	s.Require().NoError(err, "a search that matches nothing is not an error")
	s.Require().Zero(out.Summary.Topics, "no topic matches, so none is checked")
	s.Require().NotEmpty(out.Brokers, "brokers are always reported, because a broker problem is not tied to a topic")
}

func (s *ClusterHealthSuite) TestInternalTopicsAreSkippedUnlessAsked() {
	without, err := clusterhealth.Run(s.T().Context(), s.env.Admin(), clusterhealth.Input{Search: "__consumer_offsets"})
	s.Require().NoError(err, "checking without internal topics must succeed")
	s.Require().Zero(without.Summary.Topics, "internal topics are skipped by default")

	// __consumer_offsets exists only once a group has committed.
	topic := s.env.CreateTopic(s.T(), "health-internal")
	s.env.Produce(s.T(), topic, testenv.Message{Value: "one"})
	s.env.ConsumeAndCommit(s.T(), topic, s.env.UniqueName("health-internal-group"), 1)

	with, err := clusterhealth.Run(s.T().Context(), s.env.Admin(), clusterhealth.Input{
		Search: "__consumer_offsets", IncludeInternal: true,
	})
	s.Require().NoError(err, "checking with internal topics must succeed")
	s.Require().Equal(1, with.Summary.Topics,
		"include_internal must bring __consumer_offsets in, because a broken offsets topic breaks every group")
}

func (s *ClusterHealthSuite) TestErrorsWhenBrokerUnreachable() {
	client, err := kafkaclient.New(&config.Cluster{Name: "test", Brokers: []string{"127.0.0.1:1"}})
	s.Require().NoError(err, "building a client against a dead address must not fail yet")
	s.T().Cleanup(client.Close)

	_, err = clusterhealth.Run(s.T().Context(), client.Admin(), clusterhealth.Input{})
	s.Require().Error(err, "an unreachable cluster must be an error, never a report of a healthy cluster with no topics")
}

// The states below cannot be produced on a one-broker test cluster without
// stopping the broker, so they are checked against the metadata a broker would
// return for them.

func (s *ClusterHealthSuite) TestClassifyAPartitionWithNoLeaderAsOffline() {
	problem, unhealthy := clusterhealth.Classify(kadm.PartitionDetail{
		Topic: "orders", Partition: 2, Leader: -1, Replicas: []int32{1, 2, 3}, ISR: []int32{},
	}, nil)

	s.Require().True(unhealthy, "a partition with no leader cannot be read or written")
	s.Require().Contains(problem.Issues, clusterhealth.IssueOffline, "no leader is the definition of offline")
}

func (s *ClusterHealthSuite) TestClassifyALeaderErrorAsOffline() {
	problem, unhealthy := clusterhealth.Classify(kadm.PartitionDetail{
		Topic: "orders", Partition: 0, Leader: 1, Err: kerr.LeaderNotAvailable,
	}, nil)

	s.Require().True(unhealthy, "a partition the broker answers with LEADER_NOT_AVAILABLE is not usable")
	s.Require().Equal([]string{clusterhealth.IssueOffline}, problem.Issues,
		"a leader error is the same condition as a missing leader, and must not be filed as a generic error")
}

func (s *ClusterHealthSuite) TestClassifyAShrunkenISRAsUnderReplicated() {
	problem, unhealthy := clusterhealth.Classify(kadm.PartitionDetail{
		Topic: "orders", Partition: 1, Leader: 3, Replicas: []int32{3, 1, 2}, ISR: []int32{3, 1},
	}, nil)

	s.Require().True(unhealthy, "a replica out of sync means one more failure loses availability")
	s.Require().Equal([]string{clusterhealth.IssueUnderReplicated}, problem.Issues,
		"min.insync.replicas is unknown here, so under_min_isr must not be guessed")
	s.Require().True(slices.IsSorted(problem.Replicas), "replicas are sorted so the report is stable")
}

func (s *ClusterHealthSuite) TestClassifyISRBelowMinimum() {
	minimum := 2

	problem, unhealthy := clusterhealth.Classify(kadm.PartitionDetail{
		Topic: "orders", Partition: 1, Leader: 3, Replicas: []int32{1, 2, 3}, ISR: []int32{3},
	}, &minimum)

	s.Require().True(unhealthy, "one in-sync replica against a minimum of two rejects acks=all producers")
	s.Require().Contains(problem.Issues, clusterhealth.IssueUnderMinISR,
		"this is the condition behind NOT_ENOUGH_REPLICAS, and naming it is the point of the check")
	s.Require().Contains(problem.Issues, clusterhealth.IssueUnderReplicated,
		"it is also under-replicated, and both issues must be listed")
}

func (s *ClusterHealthSuite) TestClassifyAFullISRAsHealthy() {
	minimum := 2

	_, unhealthy := clusterhealth.Classify(kadm.PartitionDetail{
		Topic: "orders", Partition: 0, Leader: 1, Replicas: []int32{1, 2, 3}, ISR: []int32{1, 2, 3},
	}, &minimum)

	s.Require().False(unhealthy, "every replica in sync with a leader is healthy, and reporting it would bury real problems")
}
