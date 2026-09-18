package copymessage_test

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/denizgursoy/kafka-mcp/internal/domain/config"
	"github.com/denizgursoy/kafka-mcp/internal/domain/kafkaclient"
	"github.com/denizgursoy/kafka-mcp/internal/domain/records"
	"github.com/denizgursoy/kafka-mcp/internal/domain/testenv"
	"github.com/denizgursoy/kafka-mcp/internal/tools/copymessage"
)

type CopyMessageSuite struct {
	suite.Suite

	env *testenv.Environment
}

func TestCopyMessageSuite(t *testing.T) {
	suite.Run(t, new(CopyMessageSuite))
}

func (s *CopyMessageSuite) SetupSuite() {
	s.env = testenv.Start(s.T())
}

func (s *CopyMessageSuite) TearDownSuite() {
	s.env.Stop()
}

func (s *CopyMessageSuite) client(readOnly bool) *kafkaclient.Client {
	s.T().Helper()

	client, err := kafkaclient.New(&config.Config{
		Environment: "test",
		Brokers:     []string{s.env.Broker()},
		ReadOnly:    readOnly,
	})
	s.Require().NoError(err, "connecting to the test broker must succeed")

	s.T().Cleanup(client.Close)

	return client
}

// endOffset reads a topic's end offset straight from the broker, so a test can
// prove whether anything was actually written.
func (s *CopyMessageSuite) endOffset(topic string) int64 {
	s.T().Helper()

	ends, err := s.env.Admin().ListEndOffsets(s.T().Context(), topic)
	s.Require().NoError(err, "reading the end offset must succeed")

	end, ok := ends.Lookup(topic, 0)
	s.Require().True(ok, "the topic must have partition 0")

	return end.Offset
}

// read returns the message at an offset of a topic.
func (s *CopyMessageSuite) read(topic string, offset int64) (string, string, map[string]string) {
	s.T().Helper()

	var (
		key     string
		value   string
		headers = map[string]string{}
	)

	err := s.env.Reader().Scan(
		s.T().Context(),
		topic,
		[]records.Range{{Partition: 0, Start: offset, End: offset + 1}},
		func(record *kgo.Record) bool {
			key = string(record.Key)
			value = string(record.Value)

			for _, header := range record.Headers {
				headers[header.Key] = string(header.Value)
			}

			return false
		},
	)
	s.Require().NoError(err, "reading the copied message back must succeed")

	return key, value, headers
}

func (s *CopyMessageSuite) TestCopiesTheMessageIntact() {
	source := s.env.CreateTopic(s.T(), "copy-source")
	destination := s.env.CreateTopic(s.T(), "copy-destination")

	s.env.Produce(s.T(), source, testenv.Message{
		Key:     "order-7",
		Value:   `{"id":7,"state":"POISON"}`,
		Headers: map[string]string{"correlation-id": "corr-7"},
	})

	out, err := copymessage.Run(
		s.T().Context(),
		s.client(false),
		s.env.Reader(),
		copymessage.Input{
			SourceTopic:      source,
			SourcePartition:  0,
			SourceOffset:     0,
			DestinationTopic: destination,
			Confirm:          true,
		},
	)

	s.Require().NoError(err, "copying an existing message must succeed")
	s.Require().True(out.Applied, "a copy that happened must be reported as applied")

	key, value, headers := s.read(destination, 0)

	s.Run("the key survives", func() {
		s.Require().Equal("order-7", key,
			"the key decides partitioning and identity, so a copy that loses it is not the same message")
	})

	s.Run("the value survives", func() {
		s.Require().Equal(`{"id":7,"state":"POISON"}`, value,
			"the value is the whole reason for preserving the message, and must arrive byte for byte")
	})

	s.Run("the original headers survive", func() {
		s.Require().Equal("corr-7", headers["correlation-id"],
			"headers carry correlation ids that make the message traceable, so they must be copied too")
	})
}

func (s *CopyMessageSuite) TestAddsProvenanceHeaders() {
	source := s.env.CreateTopic(s.T(), "copy-provenance-source")
	destination := s.env.CreateTopic(s.T(), "copy-provenance-destination")

	s.env.Produce(s.T(), source,
		testenv.Message{Value: "first"},
		testenv.Message{Key: "k", Value: "second"},
	)

	_, err := copymessage.Run(
		s.T().Context(),
		s.client(false),
		s.env.Reader(),
		copymessage.Input{
			SourceTopic:      source,
			SourcePartition:  0,
			SourceOffset:     1,
			DestinationTopic: destination,
			Confirm:          true,
		},
	)

	s.Require().NoError(err, "copying must succeed")

	_, _, headers := s.read(destination, 0)

	s.Run("the source coordinates are recorded", func() {
		s.Require().Equal(source, headers["kafka-mcp-copied-from-topic"],
			"a message in a dead letter topic must say where it came from, or it becomes its own debugging problem")
		s.Require().Equal("0", headers["kafka-mcp-copied-from-partition"],
			"the partition must be recorded so the original can be found again")
		s.Require().Equal("1", headers["kafka-mcp-copied-from-offset"],
			"the offset must be recorded, which is what makes the original readable with get_message afterwards")
	})

	s.Run("who made the copy is recorded", func() {
		s.Require().Equal("copy_message", headers["kafka-mcp-copied-by-tool"],
			"the tool must identify itself, so a copy is never mistaken for a message a producer sent")
		s.Require().NotEmpty(headers["kafka-mcp-copied-at"],
			"the time of the copy must be recorded, because the copy's own timestamp is not when the original was written")
		s.Require().Equal("anonymous", headers["kafka-mcp-copied-by-principal"],
			"the connecting identity must be recorded, and an unauthenticated connection must say so rather than leave it blank")
	})
}

func (s *CopyMessageSuite) TestOriginalHeadersWinOnCollision() {
	source := s.env.CreateTopic(s.T(), "copy-collision-source")
	destination := s.env.CreateTopic(s.T(), "copy-collision-destination")

	s.env.Produce(s.T(), source, testenv.Message{
		Value:   "already copied once",
		Headers: map[string]string{"kafka-mcp-copied-from-topic": "an-earlier-topic"},
	})

	out, err := copymessage.Run(
		s.T().Context(),
		s.client(false),
		s.env.Reader(),
		copymessage.Input{
			SourceTopic:      source,
			SourcePartition:  0,
			SourceOffset:     0,
			DestinationTopic: destination,
			Confirm:          true,
		},
	)

	s.Require().NoError(err, "copying a message that already carries provenance must succeed")

	_, _, headers := s.read(destination, 0)

	s.Require().Equal("an-earlier-topic", headers["kafka-mcp-copied-from-topic"],
		"the original header must survive: provenance added by this tool must never overwrite data the message already carried")
	s.Require().NotEmpty(out.Warnings,
		"the collision must be reported, or the caller would believe the provenance describes this copy when it describes an earlier one")
}

func (s *CopyMessageSuite) TestDryRunWritesNothing() {
	source := s.env.CreateTopic(s.T(), "copy-dry-source")
	destination := s.env.CreateTopic(s.T(), "copy-dry-destination")

	s.env.Produce(s.T(), source, testenv.Message{Key: "k", Value: "payload"})

	out, err := copymessage.Run(
		s.T().Context(),
		s.client(false),
		s.env.Reader(),
		copymessage.Input{
			SourceTopic:      source,
			SourcePartition:  0,
			SourceOffset:     0,
			DestinationTopic: destination,
		},
	)

	s.Require().NoError(err, "a dry run against a valid request must succeed")
	s.Require().False(out.Applied,
		"the caller must be able to tell a preview from a copy that happened")
	s.Require().Zero(s.endOffset(destination),
		"a dry run must write nothing: the destination must still be empty afterwards")
	s.Require().Equal("payload", out.Message.Value,
		"the preview must show what would be written, so the caller can confirm it is the right message")
}

func (s *CopyMessageSuite) TestReadOnlyRefusesEvenADryRun() {
	source := s.env.CreateTopic(s.T(), "copy-read-only-source")
	destination := s.env.CreateTopic(s.T(), "copy-read-only-destination")

	s.env.Produce(s.T(), source, testenv.Message{Value: "payload"})

	_, err := copymessage.Run(
		s.T().Context(),
		s.client(true),
		s.env.Reader(),
		copymessage.Input{
			SourceTopic:      source,
			SourcePartition:  0,
			SourceOffset:     0,
			DestinationTopic: destination,
		},
	)

	s.Require().Error(err,
		"a read-only server must refuse this tool outright: its only purpose is to write, so a preview would offer a capability the server does not have")
	s.Require().Contains(err.Error(), "read-only",
		"the error must name the reason so the operator looks at the configuration rather than at Kafka permissions")
	s.Require().Zero(s.endOffset(destination),
		"nothing may have been written")
}

func (s *CopyMessageSuite) TestErrorsWhenTheDestinationDoesNotExist() {
	source := s.env.CreateTopic(s.T(), "copy-missing-destination-source")

	s.env.Produce(s.T(), source, testenv.Message{Value: "payload"})

	_, err := copymessage.Run(
		s.T().Context(),
		s.client(false),
		s.env.Reader(),
		copymessage.Input{
			SourceTopic:      source,
			SourcePartition:  0,
			SourceOffset:     0,
			DestinationTopic: s.env.UniqueName("never-created"),
			Confirm:          true,
		},
	)

	s.Require().Error(err,
		"the destination must already exist: creating it silently would hide a typo and scatter messages into topics nobody meant to make")
}

func (s *CopyMessageSuite) TestErrorsWhenTheSourceOffsetDoesNotExist() {
	source := s.env.CreateTopic(s.T(), "copy-missing-offset-source")
	destination := s.env.CreateTopic(s.T(), "copy-missing-offset-destination")

	s.env.Produce(s.T(), source, testenv.Message{Value: "only one"})

	_, err := copymessage.Run(
		s.T().Context(),
		s.client(false),
		s.env.Reader(),
		copymessage.Input{
			SourceTopic:      source,
			SourcePartition:  0,
			SourceOffset:     99,
			DestinationTopic: destination,
			Confirm:          true,
		},
	)

	s.Require().Error(err,
		"an offset that holds no message must fail rather than write an empty copy that looks like a real message")
	s.Require().Zero(s.endOffset(destination),
		"nothing may have been written")
}

func (s *CopyMessageSuite) TestErrorsWhenSourceAndDestinationAreTheSame() {
	source := s.env.CreateTopic(s.T(), "copy-same-topic")

	s.env.Produce(s.T(), source, testenv.Message{Value: "payload"})

	_, err := copymessage.Run(
		s.T().Context(),
		s.client(false),
		s.env.Reader(),
		copymessage.Input{
			SourceTopic:      source,
			SourcePartition:  0,
			SourceOffset:     0,
			DestinationTopic: source,
			Confirm:          true,
		},
	)

	s.Require().Error(err,
		"copying a topic onto itself appends a duplicate to the topic being debugged, which is never what the caller meant")
}
