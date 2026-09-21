package addpartitions_test

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/denizgursoy/kafka-mcp/internal/domain/testenv"
	"github.com/denizgursoy/kafka-mcp/internal/tools/addpartitions"
)

type AddPartitionsSuite struct {
	suite.Suite

	env *testenv.Environment
}

func TestAddPartitionsSuite(t *testing.T) {
	suite.Run(t, new(AddPartitionsSuite))
}

func (s *AddPartitionsSuite) SetupSuite() {
	s.env = testenv.Start(s.T())
}

func (s *AddPartitionsSuite) TearDownSuite() {
	s.env.Stop()
}

func (s *AddPartitionsSuite) TestDryRunDoesNotChangeTheTopic() {
	topic := s.env.CreateTopicWithPartitions(s.T(), "add-dry-run", 2)

	out, err := addpartitions.Run(
		s.T().Context(),
		s.env.ClusterClient(s.T(), false),
		s.env.Reader(),
		addpartitions.Input{Topic: topic, Partitions: 6},
	)

	s.Require().NoError(err, "a dry run against a valid request must succeed")

	s.Run("the topic is untouched", func() {
		s.Require().Equal(2, s.env.PartitionCount(s.T(), topic),
			"a dry run must change nothing: a preview that silently applied would be the worst possible bug in an irreversible operation")
	})

	s.Run("the result says it was not applied", func() {
		s.Require().False(out.Applied,
			"the caller must be able to tell a preview from a change that happened")
		s.Require().True(out.WouldApply,
			"the request is valid, so the preview must say it would work if confirmed")
	})

	s.Run("the current and requested counts are reported", func() {
		s.Require().Equal(2, out.CurrentPartitions,
			"the caller needs the starting point to judge the change")
		s.Require().Equal(6, out.RequestedPartitions,
			"the target must be echoed back so a mistyped number is visible before confirming")
	})
}

func (s *AddPartitionsSuite) TestConfirmAddsPartitions() {
	topic := s.env.CreateTopicWithPartitions(s.T(), "add-confirm", 1)

	out, err := addpartitions.Run(
		s.T().Context(),
		s.env.ClusterClient(s.T(), false),
		s.env.Reader(),
		addpartitions.Input{Topic: topic, Partitions: 3, Confirm: true},
	)

	s.Require().NoError(err, "a confirmed request must succeed")

	s.Run("the broker now reports the new count", func() {
		s.Require().Equal(3, s.env.PartitionCount(s.T(), topic),
			"the partitions must actually exist on the broker, which is the only proof the operation worked")
	})

	s.Run("the result reports what happened", func() {
		s.Require().True(out.Applied,
			"a change that happened must be reported as applied")
		s.Require().Equal(3, out.ResultingPartitions,
			"the resulting count must be re-read from the broker rather than assumed from the request")
	})
}

func (s *AddPartitionsSuite) TestRefusesToReducePartitions() {
	topic := s.env.CreateTopicWithPartitions(s.T(), "add-shrink", 3)

	_, err := addpartitions.Run(
		s.T().Context(),
		s.env.ClusterClient(s.T(), false),
		s.env.Reader(),
		addpartitions.Input{Topic: topic, Partitions: 1, Confirm: true},
	)

	s.Require().Error(err,
		"Kafka cannot remove partitions, so asking for fewer must fail with a clear reason rather than an obscure broker rejection")
	s.Require().Contains(err.Error(), "cannot reduce",
		"the error must say why it is impossible, so the caller does not simply retry")
	s.Require().Equal(3, s.env.PartitionCount(s.T(), topic),
		"a refused request must leave the topic exactly as it was")
}

func (s *AddPartitionsSuite) TestRefusesToReduceEvenInADryRun() {
	topic := s.env.CreateTopicWithPartitions(s.T(), "add-shrink-dry", 3)

	_, err := addpartitions.Run(
		s.T().Context(),
		s.env.ClusterClient(s.T(), false),
		s.env.Reader(),
		addpartitions.Input{Topic: topic, Partitions: 2},
	)

	s.Require().Error(err,
		"a preview must not suggest an impossible change is worth confirming")
}

func (s *AddPartitionsSuite) TestEqualCountIsANoOp() {
	topic := s.env.CreateTopicWithPartitions(s.T(), "add-same", 2)

	out, err := addpartitions.Run(
		s.T().Context(),
		s.env.ClusterClient(s.T(), false),
		s.env.Reader(),
		addpartitions.Input{Topic: topic, Partitions: 2, Confirm: true},
	)

	s.Require().NoError(err,
		"asking for the count a topic already has is not an error, which is what makes the target-count form safe to repeat")
	s.Require().False(out.Applied,
		"nothing changed, so nothing may be reported as applied")
	s.Require().Equal(2, s.env.PartitionCount(s.T(), topic),
		"the topic must be untouched")
}

func (s *AddPartitionsSuite) TestReadOnlyBlocksTheChange() {
	topic := s.env.CreateTopicWithPartitions(s.T(), "add-read-only", 1)

	_, err := addpartitions.Run(
		s.T().Context(),
		s.env.ClusterClient(s.T(), true),
		s.env.Reader(),
		addpartitions.Input{Topic: topic, Partitions: 4, Confirm: true},
	)

	s.Require().Error(err,
		"a read-only server must refuse to change the cluster, which is the whole purpose of the setting")
	s.Require().Contains(err.Error(), "read-only",
		"the error must name the reason so the operator knows to look at the configuration, not at Kafka")
	s.Require().Equal(1, s.env.PartitionCount(s.T(), topic),
		"a refused change must leave the topic as it was")
}

func (s *AddPartitionsSuite) TestReadOnlyStillAllowsADryRun() {
	topic := s.env.CreateTopicWithPartitions(s.T(), "add-read-only-dry", 1)

	out, err := addpartitions.Run(
		s.T().Context(),
		s.env.ClusterClient(s.T(), true),
		s.env.Reader(),
		addpartitions.Input{Topic: topic, Partitions: 4},
	)

	s.Require().NoError(err,
		"a preview changes nothing, so a read-only server can still answer what a change would do")
	s.Require().False(out.Applied,
		"a preview must never be reported as applied")
	s.Require().Equal(1, s.env.PartitionCount(s.T(), topic),
		"the topic must be untouched by a preview")
}

func (s *AddPartitionsSuite) TestKeyedTopicRequiresAcknowledgement() {
	topic := s.env.CreateTopicWithPartitions(s.T(), "add-keyed", 1)

	s.env.Produce(s.T(), topic,
		testenv.Message{Key: "order-1", Value: `{"id":1}`},
		testenv.Message{Key: "order-2", Value: `{"id":2}`},
	)

	_, err := addpartitions.Run(
		s.T().Context(),
		s.env.ClusterClient(s.T(), false),
		s.env.Reader(),
		addpartitions.Input{Topic: topic, Partitions: 3, Confirm: true},
	)

	s.Require().Error(err,
		"adding partitions to a keyed topic breaks ordering for existing keys, so it must not happen without the caller saying they understand")
	s.Require().Contains(err.Error(), "acknowledge_key_ordering",
		"the error must name the parameter that unblocks it, so the caller knows what to do next")
	s.Require().Equal(1, s.env.PartitionCount(s.T(), topic),
		"a refused change must leave the topic as it was")
}

func (s *AddPartitionsSuite) TestKeyedTopicProceedsWithAcknowledgement() {
	topic := s.env.CreateTopicWithPartitions(s.T(), "add-keyed-ack", 1)

	s.env.Produce(s.T(), topic, testenv.Message{Key: "order-1", Value: `{"id":1}`})

	out, err := addpartitions.Run(
		s.T().Context(),
		s.env.ClusterClient(s.T(), false),
		s.env.Reader(),
		addpartitions.Input{
			Topic:                  topic,
			Partitions:             3,
			Confirm:                true,
			AcknowledgeKeyOrdering: true,
		},
	)

	s.Require().NoError(err,
		"an explicit acknowledgement must allow the change: the tool warns, it does not forbid")
	s.Require().True(out.Applied, "the change must have been applied")
	s.Require().Equal(3, s.env.PartitionCount(s.T(), topic),
		"the partitions must exist on the broker")
}

func (s *AddPartitionsSuite) TestUnkeyedTopicNeedsNoAcknowledgement() {
	topic := s.env.CreateTopicWithPartitions(s.T(), "add-unkeyed", 1)

	s.env.Produce(s.T(), topic,
		testenv.Message{Value: "no key here"},
		testenv.Message{Value: "nor here"},
	)

	out, err := addpartitions.Run(
		s.T().Context(),
		s.env.ClusterClient(s.T(), false),
		s.env.Reader(),
		addpartitions.Input{Topic: topic, Partitions: 2, Confirm: true},
	)

	s.Require().NoError(err,
		"without keys there is no ordering guarantee to break, so no acknowledgement is warranted")
	s.Require().True(out.Applied, "the change must have been applied")
	s.Require().False(out.KeyedMessages,
		"the report must say the sampled messages carried no keys, which is why it went ahead")
}

func (s *AddPartitionsSuite) TestWarnsAboutKeyedMessagesInADryRun() {
	topic := s.env.CreateTopicWithPartitions(s.T(), "add-keyed-warn", 1)

	s.env.Produce(s.T(), topic, testenv.Message{Key: "order-1", Value: `{"id":1}`})

	out, err := addpartitions.Run(
		s.T().Context(),
		s.env.ClusterClient(s.T(), false),
		s.env.Reader(),
		addpartitions.Input{Topic: topic, Partitions: 3},
	)

	s.Require().NoError(err, "a preview of a keyed topic must succeed rather than error")
	s.Require().True(out.KeyedMessages,
		"the preview must report that messages are keyed, since that is the risk the caller needs to weigh")
	s.Require().NotEmpty(out.Warnings,
		"a keyed topic must carry a warning explaining that ordering for existing keys will break")
}

func (s *AddPartitionsSuite) TestErrorsOnUnknownTopic() {
	_, err := addpartitions.Run(
		s.T().Context(),
		s.env.ClusterClient(s.T(), false),
		s.env.Reader(),
		addpartitions.Input{Topic: s.env.UniqueName("missing"), Partitions: 3},
	)

	s.Require().Error(err,
		"a topic that does not exist must fail rather than appear to be scalable")
}

func (s *AddPartitionsSuite) TestErrorsOnZeroPartitions() {
	topic := s.env.CreateTopicWithPartitions(s.T(), "add-zero", 1)

	_, err := addpartitions.Run(
		s.T().Context(),
		s.env.ClusterClient(s.T(), false),
		s.env.Reader(),
		addpartitions.Input{Topic: topic, Partitions: 0},
	)

	s.Require().Error(err,
		"a missing or zero target must be refused, or an omitted parameter would read as a request to remove every partition")
}
