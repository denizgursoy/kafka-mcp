package producemessage_test

import (
	"encoding/base64"
	"errors"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/denizgursoy/kafka-mcp/internal/domain/config"
	"github.com/denizgursoy/kafka-mcp/internal/domain/kafkaclient"
	"github.com/denizgursoy/kafka-mcp/internal/domain/records"
	"github.com/denizgursoy/kafka-mcp/internal/domain/testenv"
	"github.com/denizgursoy/kafka-mcp/internal/tools/producemessage"
)

type ProduceMessageSuite struct {
	suite.Suite

	env   *testenv.Environment
	other *testenv.Environment
}

func TestProduceMessageSuite(t *testing.T) {
	suite.Run(t, new(ProduceMessageSuite))
}

func (s *ProduceMessageSuite) SetupSuite() {
	// Two real brokers, so producing to another cluster is proven to cross a
	// cluster boundary rather than merely reaching another topic of one broker.
	s.env, s.other = testenv.StartPair(s.T())
}

func (s *ProduceMessageSuite) TearDownSuite() {
	s.other.Stop()
	s.env.Stop()
}

// clusters builds a registry holding this suite's two brokers. The names are
// the ones a caller would pass as destination_cluster.
func (s *ProduceMessageSuite) clusters(readOnly bool, otherReadOnly bool) *kafkaclient.Registry {
	s.T().Helper()

	registry, err := kafkaclient.NewRegistry(&config.Config{
		Clusters: map[string]*config.Cluster{
			"here":  {Name: "here", Brokers: []string{s.env.Broker()}, ReadOnly: readOnly},
			"there": {Name: "there", Brokers: []string{s.other.Broker()}, ReadOnly: otherReadOnly},
		},
	})
	s.Require().NoError(err, "building the registry must succeed")

	s.T().Cleanup(registry.Close)

	return registry
}

// client is the local cluster only, for the cases that do not cross.
func (s *ProduceMessageSuite) client(readOnly bool) *kafkaclient.Registry {
	s.T().Helper()

	return s.clusters(readOnly, false)
}

// endOffsetOn reads a partition's end offset straight from the broker, so a
// test can prove whether anything was actually written.
func (s *ProduceMessageSuite) endOffsetOn(
	env *testenv.Environment,
	topic string,
	partition int32,
) int64 {
	s.T().Helper()

	ends, err := env.Admin().ListEndOffsets(s.T().Context(), topic)
	s.Require().NoError(err, "reading the end offset must succeed")

	end, ok := ends.Lookup(topic, partition)
	s.Require().True(ok, "the topic must have the partition being asserted on")

	return end.Offset
}

func (s *ProduceMessageSuite) endOffset(topic string) int64 {
	s.T().Helper()

	return s.endOffsetOn(s.env, topic, 0)
}

// readOn returns the raw record at one offset, so assertions can be made on
// exact bytes rather than on what the tool reported back about itself.
func (s *ProduceMessageSuite) readOn(
	env *testenv.Environment,
	topic string,
	partition int32,
	offset int64,
) (string, []byte, map[string]string) {
	s.T().Helper()

	var (
		key     string
		value   []byte
		headers = map[string]string{}
	)

	err := env.Reader().Scan(
		s.T().Context(),
		topic,
		[]records.Range{{Partition: partition, Start: offset, End: offset + 1}},
		func(record *kgo.Record) bool {
			key = string(record.Key)
			value = record.Value

			for _, header := range record.Headers {
				headers[header.Key] = string(header.Value)
			}

			return false
		},
	)
	s.Require().NoError(err, "reading the produced message back must succeed")

	return key, value, headers
}

func (s *ProduceMessageSuite) read(topic string, offset int64) (string, []byte, map[string]string) {
	s.T().Helper()

	return s.readOn(s.env, topic, 0, offset)
}

// produceOne writes one message and returns that item's result.
//
// Every call is a batch, so a single message is an items array of length one,
// and a failure for it arrives as the item's error rather than as an error for
// the call. Structural refusals, such as a read-only destination, still fail the
// call.
func (s *ProduceMessageSuite) produceOne(
	clusters *kafkaclient.Registry,
	own string,
	confirm bool,
	item producemessage.Item,
) (producemessage.Output, error) {
	s.T().Helper()

	out, err := producemessage.Run(s.T().Context(), clusters, own, producemessage.Input{
		Items:   []producemessage.Item{item},
		Confirm: confirm,
	})
	if err != nil {
		return producemessage.Output{}, err
	}

	s.Require().Len(out.Results, 1,
		"one item in must produce exactly one result out, or results cannot be matched to inputs by position")

	if out.Results[0].Error != "" {
		return producemessage.Output{}, errors.New(out.Results[0].Error)
	}

	return *out.Results[0].Result, nil
}

func (s *ProduceMessageSuite) TestProducesTheMessageIntact() {
	topic := s.env.CreateTopic(s.T(), "produce-intact")

	out, err := s.produceOne(s.client(false), "here", true, producemessage.Item{
		Topic:   topic,
		Key:     "order-7",
		Value:   `{"id":7,"state":"REPAIRED"}`,
		Headers: map[string]string{"correlation-id": "corr-7"},
	})

	s.Require().NoError(err, "producing to an existing topic must succeed")
	s.Require().True(out.Applied, "a write that happened must be reported as applied")

	key, value, headers := s.read(topic, out.WrittenOffset)

	s.Run("the key is written as given", func() {
		s.Require().Equal("order-7", key,
			"the key decides partitioning and identity, so a repaired message must keep the key it is replacing")
	})

	s.Run("the value is written byte for byte", func() {
		s.Require().Equal(`{"id":7,"state":"REPAIRED"}`, string(value),
			"the value is the whole point of producing, and a transformed payload would be a different message")
	})

	s.Run("caller headers are written", func() {
		s.Require().Equal("corr-7", headers["correlation-id"],
			"headers carry correlation ids that tie the new message to the original investigation")
	})

	s.Run("the reported offset is where it landed", func() {
		s.Require().EqualValues(1, s.endOffset(topic),
			"exactly one message must exist, so the offset the tool reported is the one a follow-up call can read")
	})
}

func (s *ProduceMessageSuite) TestStampsProvenanceHeaders() {
	topic := s.env.CreateTopic(s.T(), "produce-provenance")

	out, err := s.produceOne(s.client(false), "here", true, producemessage.Item{Topic: topic, Value: "fabricated"})

	s.Require().NoError(err, "producing must succeed")

	_, _, headers := s.read(topic, out.WrittenOffset)

	s.Run("the tool identifies itself", func() {
		s.Require().Equal("produce_message", headers["kafka-mcp-produced-by-tool"],
			"a message this server invented must never be indistinguishable from one a real producer sent")
	})

	s.Run("when and by whom is recorded", func() {
		s.Require().NotEmpty(headers["kafka-mcp-produced-at"],
			"the write time must be recorded, so a fabricated message can be correlated with the session that made it")
		s.Require().Equal("anonymous", headers["kafka-mcp-produced-by-principal"],
			"the connecting identity must be recorded, and an unauthenticated connection must say so rather than leave it blank")
	})

	s.Run("the added headers are reported", func() {
		s.Require().Contains(out.ProvenanceHeaders, "kafka-mcp-produced-by-tool",
			"the response must list what it added, so the caller knows the message carries more than they supplied")
	})
}

func (s *ProduceMessageSuite) TestCallerHeadersWinOnCollision() {
	topic := s.env.CreateTopic(s.T(), "produce-collision")

	out, err := s.produceOne(s.client(false), "here", true, producemessage.Item{
		Topic:   topic,
		Value:   "mine",
		Headers: map[string]string{"kafka-mcp-produced-by-tool": "something-else"},
	})

	s.Require().NoError(err, "a header colliding with provenance must not fail the write")

	_, _, headers := s.read(topic, out.WrittenOffset)

	s.Run("the caller's value is kept", func() {
		s.Require().Equal("something-else", headers["kafka-mcp-produced-by-tool"],
			"data the caller supplied is never overwritten by this server's bookkeeping")
	})

	s.Run("the collision is reported", func() {
		s.Require().NotEmpty(out.Warnings,
			"silently dropping provenance would leave the caller believing the message is traceable when it is not")
	})
}

func (s *ProduceMessageSuite) TestDryRunWritesNothing() {
	topic := s.env.CreateTopic(s.T(), "produce-dry-run")

	out, err := s.produceOne(s.client(false), "here", false, producemessage.Item{Topic: topic, Value: "not written"})

	s.Require().NoError(err, "a preview must succeed rather than error")

	s.Run("nothing reached the broker", func() {
		s.Require().Zero(s.endOffset(topic),
			"a message cannot be unsent, so omitting confirm must write nothing at all")
	})

	s.Run("the preview says so", func() {
		s.Require().False(out.Applied,
			"a preview must not claim to have applied anything")
		s.Require().NotEmpty(out.Note,
			"the response must tell the caller how to actually perform the write")
	})
}

func (s *ProduceMessageSuite) TestProducesBase64Value() {
	topic := s.env.CreateTopic(s.T(), "produce-base64")

	// Bytes that are not valid UTF-8, which is the case base64 exists for: a
	// protobuf or Avro payload cannot survive being carried as a JSON string.
	raw := []byte{0x00, 0xff, 0xfe, 0x01}

	out, err := s.produceOne(s.client(false), "here", true, producemessage.Item{
		Topic:    topic,
		Value:    base64.StdEncoding.EncodeToString(raw),
		Encoding: "base64",
	})

	s.Require().NoError(err, "producing a base64 value must succeed")

	_, value, _ := s.read(topic, out.WrittenOffset)

	s.Require().Equal(raw, value,
		"base64 must be decoded before writing, or a binary payload arrives as its own encoding rather than as the bytes a consumer expects")
}

func (s *ProduceMessageSuite) TestRejectsInvalidBase64() {
	topic := s.env.CreateTopic(s.T(), "produce-bad-base64")

	_, err := s.produceOne(s.client(false), "here", true, producemessage.Item{
		Topic:    topic,
		Value:    "not valid base64!!",
		Encoding: "base64",
	})

	s.Require().Error(err,
		"an undecodable value must be refused, because writing the literal text instead would put a corrupt payload in the topic")
	s.Require().Zero(s.endOffset(topic),
		"a rejected encoding must leave the topic untouched")
}

func (s *ProduceMessageSuite) TestProducesToAnExplicitPartition() {
	topic := s.env.CreateTopicWithPartitions(s.T(), "produce-partition", 3)

	out, err := s.produceOne(s.client(false), "here", true, producemessage.Item{
		Topic:     topic,
		Value:     "targeted",
		Partition: intPointer(2),
	})

	s.Require().NoError(err, "producing to a named partition must succeed")

	s.Run("it landed on the named partition", func() {
		s.Require().EqualValues(2, out.WrittenPartition,
			"an explicit partition must be honoured, because reproducing a bug often depends on which partition a consumer reads")
		s.Require().EqualValues(1, s.endOffsetOn(s.env, topic, 2),
			"the broker must actually hold the record on that partition, not merely report it")
	})

	s.Run("the other partitions are untouched", func() {
		s.Require().Zero(s.endOffsetOn(s.env, topic, 0),
			"the default partitioner ignores Record.Partition, so a wrong implementation would quietly land here instead")
	})
}

func (s *ProduceMessageSuite) TestKeyDecidesPartitionWhenNoneIsGiven() {
	topic := s.env.CreateTopicWithPartitions(s.T(), "produce-keyed", 3)

	first, err := s.produceOne(s.client(false), "here", true, producemessage.Item{Topic: topic, Key: "order-7", Value: "one"})
	s.Require().NoError(err, "producing a keyed message must succeed")

	second, err := s.produceOne(s.client(false), "here", true, producemessage.Item{Topic: topic, Key: "order-7", Value: "two"})
	s.Require().NoError(err, "producing the same key again must succeed")

	s.Require().Equal(first.WrittenPartition, second.WrittenPartition,
		"one key must always hash to one partition, or re-injecting a repaired message would break the ordering of that key")
}

func (s *ProduceMessageSuite) TestErrorsOnAPartitionThatDoesNotExist() {
	topic := s.env.CreateTopic(s.T(), "produce-missing-partition")

	_, err := s.produceOne(s.client(false), "here", true, producemessage.Item{
		Topic:     topic,
		Value:     "nowhere",
		Partition: intPointer(9),
	})

	s.Require().Error(err,
		"a partition the topic does not have must be refused rather than silently rerouted, since the caller chose it for a reason")
	s.Require().Zero(s.endOffset(topic),
		"a refused partition must leave the topic untouched")
}

func (s *ProduceMessageSuite) TestErrorsWhenTheTopicDoesNotExist() {
	_, err := s.produceOne(s.client(false), "here", true, producemessage.Item{
		Topic: s.env.UniqueName("produce-absent"),
		Value: "orphan",
	})

	s.Require().Error(err,
		"a missing topic must be refused, because auto-creation would scatter messages into a topic nobody meant to make")
	s.Require().Contains(err.Error(), "produce-absent",
		"the error must quote the topic, since a typo is the likeliest cause")
	s.Require().Contains(err.Error(), "does not exist",
		"the caller must be told the topic is missing in words they can act on, not handed the broker's UNKNOWN_TOPIC_OR_PARTITION code")
}

func (s *ProduceMessageSuite) TestErrorsWhenTheValueIsMissing() {
	topic := s.env.CreateTopic(s.T(), "produce-no-value")

	_, err := s.produceOne(s.client(false), "here", true, producemessage.Item{Topic: topic})

	s.Require().Error(err,
		"an absent value must be refused rather than written as empty, because the two are different messages and only one was intended")
}

func (s *ProduceMessageSuite) TestReadOnlyRefusesEvenADryRun() {
	topic := s.env.CreateTopic(s.T(), "produce-read-only")

	_, err := s.produceOne(s.client(true), "here", false, producemessage.Item{Topic: topic, Value: "refused"})

	s.Require().Error(err,
		"writing is all this tool does, so a read-only endpoint must refuse the preview too rather than describe a capability it does not have")
	s.Require().Zero(s.endOffset(topic),
		"a refused call must leave the topic untouched")
}

func (s *ProduceMessageSuite) TestProducesToAnotherCluster() {
	topic := s.other.CreateTopic(s.T(), "produce-cross-cluster")

	out, err := s.produceOne(s.clusters(false, false), "here", true, producemessage.Item{
		Topic:              topic,
		Value:              "reproduced",
		DestinationCluster: "there",
	})

	s.Require().NoError(err, "producing to another configured cluster must succeed")

	s.Run("it was written to the other broker", func() {
		s.Require().EqualValues(1, s.endOffsetOn(s.other, topic, 0),
			"the message must exist on the destination cluster, which is what makes reproducing a bug in preprod possible")
	})

	s.Run("the destination is reported", func() {
		s.Require().Equal("there", out.DestinationCluster,
			"the response must name the cluster written to, because the caller's endpoint is a different one")
	})
}

func (s *ProduceMessageSuite) TestRefusesAReadOnlyDestinationCluster() {
	topic := s.other.CreateTopic(s.T(), "produce-read-only-destination")

	_, err := s.produceOne(s.clusters(false, true), "here", true, producemessage.Item{
		Topic:              topic,
		Value:              "refused",
		DestinationCluster: "there",
	})

	s.Require().Error(err,
		"read_only protects the cluster being written to, so a writable endpoint must not be a way around it")
	s.Require().Zero(s.endOffsetOn(s.other, topic, 0),
		"a refused destination must leave the other cluster untouched")
}

func (s *ProduceMessageSuite) TestAllowsAReadOnlySourceEndpoint() {
	topic := s.other.CreateTopic(s.T(), "produce-from-read-only")

	_, err := s.produceOne(s.clusters(true, false), "here", true, producemessage.Item{
		Topic:              topic,
		Value:              "seeded",
		DestinationCluster: "there",
	})

	s.Require().NoError(err,
		"a session on a read-only endpoint writes nothing to that cluster by producing elsewhere, which is how test data reaches preprod from a protected session")
	s.Require().EqualValues(1, s.endOffsetOn(s.other, topic, 0),
		"the writable destination must have received the message")
}

func (s *ProduceMessageSuite) TestErrorsOnAnUnknownDestinationCluster() {
	topic := s.env.CreateTopic(s.T(), "produce-unknown-cluster")

	_, err := s.produceOne(s.client(false), "here", true, producemessage.Item{
		Topic:              topic,
		Value:              "nowhere",
		DestinationCluster: "never-configured",
	})

	s.Require().Error(err, "an unknown cluster name must be an error rather than a silent local write")
	s.Require().Contains(err.Error(), "never-configured",
		"the error must quote the unknown name, since a typo is the likeliest cause")
}

func (s *ProduceMessageSuite) TestBatchProducesInInputOrder() {
	topic := s.env.CreateTopic(s.T(), "produce-batch")

	out, err := producemessage.Run(
		s.T().Context(),
		s.client(false),
		"here",
		producemessage.Input{
			Items: []producemessage.Item{
				{Topic: topic, Value: "first"},
				{Topic: topic, Value: "second"},
			},
			Confirm: true,
		},
	)

	s.Require().NoError(err, "producing a valid batch must succeed")

	s.Run("every item was written", func() {
		s.Require().Equal(2, out.Succeeded, "both messages must be produced")
		s.Require().Zero(out.Failed, "a valid batch must not report failed items")
		s.Require().EqualValues(2, s.endOffset(topic), "the topic must hold both messages")
	})

	s.Run("results follow input order", func() {
		_, first, _ := s.read(topic, out.Results[0].Result.WrittenOffset)
		_, second, _ := s.read(topic, out.Results[1].Result.WrittenOffset)

		s.Require().Equal("first", string(first),
			"results are matched to inputs by position, so a reordered result misattributes every offset")
		s.Require().Equal("second", string(second),
			"the second result must correspond to the second input")
	})

	s.Run("the batch reports it is not atomic", func() {
		s.Require().False(out.Atomic,
			"Kafka cannot retract a produced record, so a caller must never believe a partial batch was rolled back")
	})
}

func (s *ProduceMessageSuite) TestBatchReportsItemErrorsWithoutHidingSuccesses() {
	topic := s.env.CreateTopic(s.T(), "produce-batch-partial")

	out, err := producemessage.Run(
		s.T().Context(),
		s.client(false),
		"here",
		producemessage.Input{
			Items: []producemessage.Item{
				{Topic: topic, Value: "good"},
				{Topic: s.env.UniqueName("produce-batch-absent"), Value: "bad"},
			},
			Confirm: true,
		},
	)

	s.Require().NoError(err,
		"one bad item is data in the response, not a failure of the whole call")

	s.Run("the valid item was written", func() {
		s.Require().Equal(1, out.Succeeded, "the good item must still be produced")
		s.Require().NotNil(out.Results[0].Result, "the successful item must carry its result")
	})

	s.Run("the invalid item is reported in place", func() {
		s.Require().Equal(1, out.Failed, "the bad item must be counted as failed")
		s.Require().NotEmpty(out.Results[1].Error,
			"an item error must be attached to the item, so it cannot be mistaken for a different one")
	})
}

func (s *ProduceMessageSuite) TestBatchPreviewWritesNothing() {
	topic := s.env.CreateTopic(s.T(), "produce-batch-preview")

	out, err := producemessage.Run(
		s.T().Context(),
		s.client(false),
		"here",
		producemessage.Input{
			Items: []producemessage.Item{
				{Topic: topic, Value: "first"},
				{Topic: topic, Value: "second"},
			},
		},
	)

	s.Require().NoError(err, "previewing a batch must succeed")
	s.Require().Zero(out.Applied, "a preview must apply nothing")
	s.Require().Zero(s.endOffset(topic),
		"one confirm covers the whole batch, so without it not a single item may be written")
}

func (s *ProduceMessageSuite) TestBatchRejectsTooManyItems() {
	topic := s.env.CreateTopic(s.T(), "produce-batch-limit")

	items := make([]producemessage.Item, 21)
	for index := range items {
		items[index] = producemessage.Item{Topic: topic, Value: "x"}
	}

	_, err := producemessage.Run(s.T().Context(), s.client(false), "here",
		producemessage.Input{Items: items, Confirm: true})

	s.Require().Error(err,
		"a batch that opens a record writer is bounded, so an oversized batch must be refused before anything is written")
	s.Require().Zero(s.endOffset(topic),
		"a refused batch must leave the topic untouched")
}

func (s *ProduceMessageSuite) TestBatchRejectsAnEmptyItemList() {
	_, err := producemessage.Run(s.T().Context(), s.client(false), "here",
		producemessage.Input{Items: []producemessage.Item{}, Confirm: true})

	s.Require().Error(err,
		"an empty batch is a caller mistake rather than a no-op, because it means the intended operations were lost before the call")
}

func intPointer(value int32) *int32 {
	return &value
}
