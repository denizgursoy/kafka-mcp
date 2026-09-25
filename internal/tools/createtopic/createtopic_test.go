package createtopic_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/denizgursoy/kafka-mcp/internal/domain/testenv"
	"github.com/denizgursoy/kafka-mcp/internal/tools/createtopic"
)

type CreateTopicSuite struct {
	suite.Suite

	env *testenv.Environment
}

func TestCreateTopicSuite(t *testing.T) {
	suite.Run(t, new(CreateTopicSuite))
}

func (s *CreateTopicSuite) SetupSuite() {
	s.env = testenv.Start(s.T())
}

func (s *CreateTopicSuite) TearDownSuite() {
	s.env.Stop()
}

// create creates one topic and returns that item's result.
//
// Every call is a batch, so a single topic is an items array of length one, and
// a failure for it arrives as the item's error rather than as an error for the
// call. Structural problems, such as an empty items array, still fail the call.
func (s *CreateTopicSuite) create(
	readOnly bool,
	confirm bool,
	item createtopic.Item,
) (createtopic.Output, error) {
	s.T().Helper()

	out, err := createtopic.Run(
		s.T().Context(),
		s.env.ClusterClient(s.T(), readOnly),
		createtopic.Input{Items: []createtopic.Item{item}, Confirm: confirm},
	)
	if err != nil {
		return createtopic.Output{}, err
	}

	s.Require().Len(out.Results, 1,
		"one item in must produce exactly one result out, or results cannot be matched to inputs by position")

	if out.Results[0].Error != "" {
		return createtopic.Output{}, errors.New(out.Results[0].Error)
	}

	return *out.Results[0].Result, nil
}

func (s *CreateTopicSuite) TestPreviewCreatesNothing() {
	topic := s.env.UniqueName("create-preview")

	out, err := s.create(false, false, createtopic.Item{Topic: topic, Partitions: 3, ReplicationFactor: 1})

	s.Require().NoError(err,
		"a valid request must preview successfully, because that is how the caller learns it would work")

	s.Run("the broker does not have the topic", func() {
		s.Require().False(s.env.TopicExists(s.T(), topic),
			"a preview must create nothing: a topic that appeared without confirmation cannot be undone without deleting data")
	})

	s.Run("the result separates a preview from a creation", func() {
		s.Require().False(out.Created,
			"the caller must be able to tell a preview from a topic that now exists")
		s.Require().True(out.WouldCreate,
			"the request is valid, so the preview must say it would work if confirmed")
	})
}

func (s *CreateTopicSuite) TestConfirmCreatesTheTopic() {
	topic := s.env.UniqueName("create-confirm")
	defer s.env.DeleteTopics(s.T(), topic)

	out, err := s.create(false, true, createtopic.Item{Topic: topic, Partitions: 3, ReplicationFactor: 1})

	s.Require().NoError(err, "a confirmed request against a valid broker must succeed")

	s.Run("the broker reports the topic with the requested partitions", func() {
		s.Require().True(s.env.TopicExists(s.T(), topic),
			"the topic must exist on the broker, which is the only proof the tool did anything")
		s.Require().Equal(3, s.env.PartitionCount(s.T(), topic),
			"the partition count must be what was asked for, because it cannot be reduced afterwards")
	})

	s.Run("the result reports what was created", func() {
		s.Require().True(out.Created,
			"a topic that now exists must be reported as created")
		s.Require().Equal(3, out.Partitions,
			"the reported count must come from the broker's response, not be echoed from the request")
	})
}

func (s *CreateTopicSuite) TestCreatesWithTopicConfigs() {
	topic := s.env.UniqueName("create-configs")
	defer s.env.DeleteTopics(s.T(), topic)

	_, err := s.create(false, true, createtopic.Item{
		Topic:             topic,
		Partitions:        1,
		ReplicationFactor: 1,
		Configs:           map[string]string{"retention.ms": "60000"},
	})

	s.Require().NoError(err, "creating a topic with a valid config must succeed")
	s.Require().Equal("60000", s.env.TopicConfig(s.T(), topic, "retention.ms"),
		"the config must be set on the topic itself: retention is the usual reason to create a topic explicitly rather than letting a producer do it")
}

func (s *CreateTopicSuite) TestOmittedCountsUseBrokerDefaults() {
	topic := s.env.UniqueName("create-defaults")
	defer s.env.DeleteTopics(s.T(), topic)

	out, err := s.create(false, true, createtopic.Item{Topic: topic})

	s.Require().NoError(err,
		"omitting the counts must mean the broker's defaults, not a rejected request: a caller who does not care should not have to guess")

	s.Require().True(s.env.TopicExists(s.T(), topic), "the topic must exist on the broker")
	partitions := s.env.PartitionCount(s.T(), topic)
	s.Require().Positive(partitions,
		"a topic created with broker defaults still has partitions, and the count must be reported")
	s.Require().Equal(partitions, out.Partitions,
		"the reported count must match the broker, so the caller learns what the defaults actually were")
}

func (s *CreateTopicSuite) TestRefusesATopicThatAlreadyExists() {
	topic := s.env.CreateTopicWithPartitions(s.T(), "create-existing", 2)

	_, err := s.create(false, true, createtopic.Item{Topic: topic, Partitions: 5, ReplicationFactor: 1})

	s.Require().Error(err,
		"an existing topic must fail loudly: silently succeeding would let a caller believe a topic has settings it does not have")
	s.Require().Contains(err.Error(), "already exists",
		"the error must say the topic is already there, so the caller looks at the existing topic rather than retrying")

	s.Require().Equal(2, s.env.PartitionCount(s.T(), topic),
		"the existing topic must be untouched: create_topic must never be a back door to changing one")
}

func (s *CreateTopicSuite) TestReadOnlyRefusesEvenThePreview() {
	topic := s.env.UniqueName("create-read-only")

	_, err := s.create(true, false, createtopic.Item{Topic: topic, Partitions: 1, ReplicationFactor: 1})

	s.Require().Error(err,
		"creating a topic is the tool's only purpose, so a read-only cluster must refuse outright: previewing a capability the server does not have is misleading")
	s.Require().Contains(err.Error(), "read-only",
		"the error must name the reason, so the operator looks at this server's configuration rather than at Kafka")

	s.Require().False(s.env.TopicExists(s.T(), topic), "a refused request must create nothing")
}

func (s *CreateTopicSuite) TestErrorsOnMissingTopicName() {
	_, err := s.create(false, true, createtopic.Item{Partitions: 1})

	s.Require().Error(err,
		"an empty name must be refused here rather than sent to the broker, which would answer with an error no caller can act on")
}

func (s *CreateTopicSuite) TestDoesNotRewriteTheTopicName() {
	topic := " " + s.env.UniqueName("create-spaced") + " "

	out, err := s.create(false, false, createtopic.Item{Topic: topic, Partitions: 1, ReplicationFactor: 1})

	s.Require().NoError(err,
		"this broker accepts the name, so the preview should report its own validation result rather than imposing a different naming policy")
	s.Require().Equal(topic, out.Topic,
		"a mutating tool must preserve the target exactly as supplied: trimming would redirect the request to a different topic")
}

func (s *CreateTopicSuite) TestErrorsOnCountsKafkaCannotRepresent() {
	s.Run("partition count exceeds Kafka's int32 field", func() {
		_, err := s.create(false, false, createtopic.Item{
			Topic:      s.env.UniqueName("create-too-many-partitions"),
			Partitions: int(^uint32(0)>>1) + 1,
		})

		s.Require().Error(err,
			"a partition count that overflows Kafka's int32 request field must be rejected instead of wrapping to a negative number")
	})

	s.Run("replication factor exceeds Kafka's int16 field", func() {
		_, err := s.create(false, false, createtopic.Item{
			Topic:             s.env.UniqueName("create-too-many-replicas"),
			ReplicationFactor: int(^uint16(0)>>1) + 1,
		})

		s.Require().Error(err,
			"a replication factor that overflows Kafka's int16 request field must be rejected instead of wrapping to a negative number")
	})
}

func (s *CreateTopicSuite) TestPreviewSurfacesABrokerRejection() {
	topic := s.env.UniqueName("create-invalid-rf")

	_, err := s.create(false, false, createtopic.Item{Topic: topic, Partitions: 1, ReplicationFactor: 5})

	s.Require().Error(err,
		"the test broker has one node, so a replication factor of 5 is impossible and the preview must say so instead of reporting it would work")

	s.Require().False(s.env.TopicExists(s.T(), topic),
		"a rejected preview must leave nothing behind, including a partially created topic")
}

func (s *CreateTopicSuite) TestBatchCreatesSeveralTopics() {
	first := s.env.UniqueName("create-batch-first")
	second := s.env.UniqueName("create-batch-second")
	defer s.env.DeleteTopics(s.T(), first, second)

	out, err := createtopic.Run(
		s.T().Context(), s.env.ClusterClient(s.T(), false),
		createtopic.Input{
			Items: []createtopic.Item{
				{Topic: first, Partitions: 1, ReplicationFactor: 1},
				{Topic: second, Partitions: 2, ReplicationFactor: 1},
			},
			Confirm: true,
		},
	)

	s.Require().NoError(err, "creating a structurally valid batch must succeed")
	s.Require().Equal(2, out.Succeeded, "both valid topics must be created")
	s.Require().False(out.Atomic, "Kafka does not make a multi-topic batch transactional and the response must say so")
	s.Require().True(s.env.TopicExists(s.T(), first), "the first topic must exist")
	s.Require().Equal(2, s.env.PartitionCount(s.T(), second), "the second topic must keep its own requested partition count")
}
