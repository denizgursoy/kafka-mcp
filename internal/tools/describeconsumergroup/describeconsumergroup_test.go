package describeconsumergroup_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/denizgursoy/kafka-mcp/internal/domain/config"
	"github.com/denizgursoy/kafka-mcp/internal/domain/kafkaclient"
	"github.com/denizgursoy/kafka-mcp/internal/domain/testenv"
	"github.com/denizgursoy/kafka-mcp/internal/tools/describeconsumergroup"
)

type DescribeConsumerGroupSuite struct {
	suite.Suite

	env *testenv.Environment
}

func TestDescribeConsumerGroupSuite(t *testing.T) {
	suite.Run(t, new(DescribeConsumerGroupSuite))
}

func (s *DescribeConsumerGroupSuite) SetupSuite() {
	s.env = testenv.Start(s.T())
}

func (s *DescribeConsumerGroupSuite) TearDownSuite() {
	s.env.Stop()
}

func (s *DescribeConsumerGroupSuite) describe(group string) (describeconsumergroup.Output, error) {
	s.T().Helper()

	out, err := describeconsumergroup.Run(s.T().Context(), s.env.Admin(), describeconsumergroup.Input{
		Items: []describeconsumergroup.Item{{Group: group}},
	})
	if err != nil {
		return describeconsumergroup.Output{}, err
	}

	s.Require().Len(out.Results, 1,
		"one item in must produce exactly one result out, or results cannot be matched to inputs by position")

	if out.Results[0].Error != "" {
		return describeconsumergroup.Output{}, errors.New(out.Results[0].Error)
	}

	return *out.Results[0].Result, nil
}

func (s *DescribeConsumerGroupSuite) messages(n int, partition int32) []testenv.Message {
	out := make([]testenv.Message, 0, n)
	for range n {
		out = append(out, testenv.Message{Value: "message", Partition: partition})
	}

	return out
}

func (s *DescribeConsumerGroupSuite) TestActiveGroupShowsWhoOwnsEachPartition() {
	topic := s.env.CreateTopicWithPartitions(s.T(), "describe-group-active", 2)
	s.env.Produce(s.T(), topic, append(s.messages(3, 0), s.messages(5, 1)...)...)

	// Committing everything first and producing afterwards makes the lag on
	// each partition exact, whichever partitions the consumer happened to
	// read from first.
	group := s.env.UniqueName("describe-group-active")
	s.env.ConsumeAndCommit(s.T(), topic, group, 8)
	s.env.Produce(s.T(), topic, append(s.messages(2, 0), s.messages(4, 1)...)...)
	s.env.JoinGroup(s.T(), topic, group, "orders-worker-7")

	out, err := s.describe(group)
	s.Require().NoError(err, "describing a running group must succeed")

	s.Run("the group state and members are reported", func() {
		s.Require().Equal("Stable", out.State, "a group with a live member that finished rebalancing is Stable")
		s.Require().Len(out.Members, 1, "the one running consumer must be listed")
		s.Require().Equal("orders-worker-7", out.Members[0].ClientID,
			"the client id is how an operator recognises which deployment a member belongs to")
		s.Require().NotEmpty(out.Members[0].Host, "the host is how an operator finds the pod or machine to look at")
		s.Require().Len(out.Members[0].Assignments, 1, "the member is assigned the one topic it consumes")
		s.Require().Equal([]int32{0, 1}, out.Members[0].Assignments[0].Partitions,
			"the only member owns both partitions, sorted so the report is stable")
	})

	s.Run("every partition names its owner and lag", func() {
		s.Require().Len(out.Partitions, 2, "every partition the group touches must be listed")

		for _, partition := range out.Partitions {
			s.Require().Equal(out.Members[0].MemberID, partition.MemberID,
				"each partition must name the member consuming it, which is what turns a stuck partition into a pod to inspect")
			s.Require().Equal("orders-worker-7", partition.ClientID,
				"the owner's client id is repeated on the partition so the caller does not have to join the two lists")
		}

		total := int64(0)
		for _, partition := range out.Partitions {
			total += partition.Lag
		}

		s.Require().EqualValues(6, total,
			"lag is end offset minus committed offset summed over partitions; six messages arrived after the last commit")
		s.Require().EqualValues(total, out.TotalLag, "the total must match the partitions it summarises")
	})
}

func (s *DescribeConsumerGroupSuite) observe(group string, seconds int) (describeconsumergroup.Output, error) {
	s.T().Helper()

	out, err := describeconsumergroup.Run(s.T().Context(), s.env.Admin(), describeconsumergroup.Input{
		Items: []describeconsumergroup.Item{{Group: group, SampleSeconds: seconds}},
	})
	if err != nil {
		return describeconsumergroup.Output{}, err
	}
	if out.Results[0].Error != "" {
		return describeconsumergroup.Output{}, errors.New(out.Results[0].Error)
	}

	return *out.Results[0].Result, nil
}

func (s *DescribeConsumerGroupSuite) TestObservingAStableGroupReportsNoChurn() {
	topic := s.env.CreateTopic(s.T(), "describe-group-stable")
	group := s.env.UniqueName("describe-group-stable")
	s.env.JoinGroup(s.T(), topic, group, "steady-worker")

	out, err := s.observe(group, 2)
	s.Require().NoError(err, "observing a group must succeed")

	s.Require().NotNil(out.Observation, "the observation is reported only when sample_seconds was set")
	s.Require().GreaterOrEqual(out.Observation.Samples, 2, "the group must be read more than once to see change")
	s.Require().Empty(out.Observation.Joined, "no member joined")
	s.Require().Empty(out.Observation.Left, "no member left")
	s.Require().Equal([]string{"Stable"}, out.Observation.States,
		"a group that never left Stable is the answer that rules out a rebalance storm")
	s.Require().False(out.Observation.Unstable, "nothing changed, so the group is not unstable")
}

func (s *DescribeConsumerGroupSuite) TestObservingReportsMembersThatJoinAndLeave() {
	topic := s.env.CreateTopicWithPartitions(s.T(), "describe-group-churn", 2)
	group := s.env.UniqueName("describe-group-churn")
	leave := s.env.JoinGroup(s.T(), topic, group, "worker-a")

	done := make(chan struct{})
	go func() {
		defer close(done)
		time.Sleep(time.Second)
		leave()
	}()

	out, err := s.observe(group, 4)
	<-done
	s.Require().NoError(err, "observing a changing group must succeed")

	s.Require().NotEmpty(out.Observation.Left, "the member that stopped must be reported as having left")
	s.Require().Equal("worker-a", out.Observation.Left[0].ClientID,
		"the client id and host of a departed member are how an operator finds the crash-looping pod")
	s.Require().True(out.Observation.Unstable, "membership changed during the window")
	s.Require().GreaterOrEqual(len(out.Observation.States), 2,
		"the state moved away from Stable when the member left, and the sequence must show it")
}

func (s *DescribeConsumerGroupSuite) TestRejectsAnUnboundedObservation() {
	_, err := s.observe("anything", 3600)
	s.Require().ErrorContains(err, "sample_seconds", "the call blocks for the window, so it must be bounded")
}

func (s *DescribeConsumerGroupSuite) TestEmptyGroupStillReportsCommittedOffsets() {
	topic := s.env.CreateTopic(s.T(), "describe-group-empty")
	s.env.Produce(s.T(), topic, s.messages(6, 0)...)

	group := s.env.UniqueName("describe-group-empty")
	s.env.ConsumeAndCommit(s.T(), topic, group, 4)

	out, err := s.describe(group)
	s.Require().NoError(err, "describing a group with no running members must succeed")

	s.Require().Equal("Empty", out.State, "a group whose consumers stopped is Empty")
	s.Require().Empty(out.Members, "there are no members to report")
	s.Require().NotNil(out.Members, "no members must be [] rather than null so callers can iterate safely")
	s.Require().Len(out.Partitions, 1, "committed offsets outlive the consumers, and are what decides where they resume")
	s.Require().EqualValues(4, out.Partitions[0].CommittedOffset, "the committed offset must be read from the broker")
	s.Require().EqualValues(2, out.Partitions[0].Lag, "six messages, four committed")
	s.Require().Empty(out.Partitions[0].MemberID, "nobody owns a partition of an empty group")
}

func (s *DescribeConsumerGroupSuite) TestAssignedPartitionWithoutCommitIsReported() {
	topic := s.env.CreateTopic(s.T(), "describe-group-no-commit")
	s.env.Produce(s.T(), topic, s.messages(3, 0)...)

	group := s.env.UniqueName("describe-group-no-commit")
	s.env.JoinGroup(s.T(), topic, group, "fresh-worker")

	out, err := s.describe(group)
	s.Require().NoError(err, "describing a group that has never committed must succeed")
	s.Require().Len(out.Partitions, 1, "an assigned partition must be listed even before the first commit")
	s.Require().False(out.Partitions[0].HasCommit,
		"a partition with no commit must say so, because where it resumes then depends on auto.offset.reset, not on any offset")
}

func (s *DescribeConsumerGroupSuite) TestUnknownGroupIsAnItemError() {
	_, err := s.describe(s.env.UniqueName("never-existed"))

	s.Require().Error(err, "a group the broker does not know must be an error rather than an empty description")
}

func (s *DescribeConsumerGroupSuite) TestBatchKeepsOrderAndIsolatesErrors() {
	topic := s.env.CreateTopic(s.T(), "describe-group-batch")
	s.env.Produce(s.T(), topic, s.messages(2, 0)...)

	group := s.env.UniqueName("describe-group-batch")
	s.env.ConsumeAndCommit(s.T(), topic, group, 1)

	out, err := describeconsumergroup.Run(s.T().Context(), s.env.Admin(), describeconsumergroup.Input{
		Items: []describeconsumergroup.Item{
			{Group: s.env.UniqueName("missing")},
			{Group: group},
		},
	})

	s.Require().NoError(err, "a structurally valid batch must return per-item results")
	s.Require().Len(out.Results, 2, "every group must have a result in input order")
	s.Require().NotEmpty(out.Results[0].Error, "the missing group must fail on its own item")
	s.Require().NotNil(out.Results[1].Result, "the real group must still be described")
	s.Require().Equal(group, out.Results[1].Result.Group, "results must stay aligned with the input order")
}

func (s *DescribeConsumerGroupSuite) TestRefusesAnEmptyBatch() {
	_, err := describeconsumergroup.Run(s.T().Context(), s.env.Admin(), describeconsumergroup.Input{})

	s.Require().Error(err, "an empty items array names nothing to describe")
}

func (s *DescribeConsumerGroupSuite) TestErrorsWhenBrokerUnreachable() {
	client, err := kafkaclient.New(&config.Cluster{Name: "test", Brokers: []string{"127.0.0.1:1"}})
	s.Require().NoError(err, "building a client against a dead address must not fail yet")
	s.T().Cleanup(client.Close)

	out, err := describeconsumergroup.Run(s.T().Context(), client.Admin(), describeconsumergroup.Input{
		Items: []describeconsumergroup.Item{{Group: "anything"}},
	})

	s.Require().NoError(err, "a failure to reach the broker belongs to the item, so the call still returns results")
	s.Require().NotEmpty(out.Results[0].Error,
		"an unreachable broker must surface as an error, not as a group with no members")
}
