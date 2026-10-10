package copymessage_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/twmb/avro"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"

	"github.com/denizgursoy/kafka-mcp/internal/domain/config"
	"github.com/denizgursoy/kafka-mcp/internal/domain/kafkaclient"
	"github.com/denizgursoy/kafka-mcp/internal/domain/records"
	"github.com/denizgursoy/kafka-mcp/internal/domain/testenv"
	"github.com/denizgursoy/kafka-mcp/internal/tools/copymessage"
)

type CopyMessageSuite struct {
	suite.Suite

	env   *testenv.Environment
	other *testenv.Environment
}

func TestCopyMessageSuite(t *testing.T) {
	suite.Run(t, new(CopyMessageSuite))
}

func (s *CopyMessageSuite) SetupSuite() {
	// Two real brokers, so a cross-cluster copy is proven to cross a cluster
	// boundary rather than merely moving between two topics of one broker.
	s.env, s.other = testenv.StartPair(s.T())
}

func (s *CopyMessageSuite) TearDownSuite() {
	s.other.Stop()
	s.env.Stop()
}

// clusters builds a registry holding this suite's two brokers. The names are
// the ones a caller would use as destination_cluster.
func (s *CopyMessageSuite) clusters(readOnly bool, otherReadOnly bool) *kafkaclient.Registry {
	s.T().Helper()

	registry, err := kafkaclient.NewRegistry(&config.Config{
		Clusters: map[string]*config.Cluster{
			"here": {
				Name: "here", Brokers: []string{s.env.Broker()}, ReadOnly: readOnly,
				SchemaRegistry: &config.SchemaRegistry{URLs: []string{s.env.SchemaRegistry()}},
			},
			"there": {
				Name: "there", Brokers: []string{s.other.Broker()}, ReadOnly: otherReadOnly,
				SchemaRegistry: &config.SchemaRegistry{URLs: []string{s.other.SchemaRegistry()}},
			},
		},
	})
	s.Require().NoError(err, "building the registry must succeed")

	s.T().Cleanup(registry.Close)

	return registry
}

// client is the local cluster only, for the cases that do not cross.
func (s *CopyMessageSuite) client(readOnly bool) *kafkaclient.Registry {
	s.T().Helper()

	return s.clusters(readOnly, false)
}

// endOffset reads a topic's end offset straight from the broker, so a test can
// prove whether anything was actually written.
func (s *CopyMessageSuite) endOffsetOn(env *testenv.Environment, topic string) int64 {
	s.T().Helper()

	ends, err := env.Admin().ListEndOffsets(s.T().Context(), topic)
	s.Require().NoError(err, "reading the end offset must succeed")

	end, ok := ends.Lookup(topic, 0)
	s.Require().True(ok, "the topic must have partition 0")

	return end.Offset
}

func (s *CopyMessageSuite) endOffset(topic string) int64 {
	s.T().Helper()

	return s.endOffsetOn(s.env, topic)
}

// read returns the message at an offset of a topic.
func (s *CopyMessageSuite) readOn(
	env *testenv.Environment,
	topic string,
	offset int64,
) (string, string, map[string]string) {
	s.T().Helper()

	var (
		key     string
		value   string
		headers = map[string]string{}
	)

	err := env.Reader().Scan(
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

func (s *CopyMessageSuite) read(topic string, offset int64) (string, string, map[string]string) {
	s.T().Helper()

	return s.readOn(s.env, topic, offset)
}

// copyOne copies one message and returns that item's result.
//
// Every call is a batch, so a single copy is an items array of length one, and a
// failure for it arrives as the item's error rather than as an error for the
// call. Structural refusals, such as a read-only destination, still fail the
// call.
func (s *CopyMessageSuite) copyOne(
	clusters *kafkaclient.Registry,
	own string,
	confirm bool,
	item copymessage.Item,
) (copymessage.Output, error) {
	s.T().Helper()

	out, err := copymessage.Run(s.T().Context(), clusters, own, copymessage.Input{
		Items:   []copymessage.Item{item},
		Confirm: confirm,
	})
	if err != nil {
		return copymessage.Output{}, err
	}

	s.Require().Len(out.Results, 1,
		"one item in must produce exactly one result out, or results cannot be matched to inputs by position")

	if out.Results[0].Error != "" {
		return copymessage.Output{}, errors.New(out.Results[0].Error)
	}

	return *out.Results[0].Result, nil
}

const orderAvro = `{"type":"record","name":"Order","namespace":"shop","fields":[
	{"name":"id","type":"string"},{"name":"amount","type":"long"}]}`

// avroMessage registers the order schema on env under subject and returns a
// framed message carrying it, with the id the registry assigned.
func (s *CopyMessageSuite) avroMessage(env *testenv.Environment, subject string) (string, int) {
	s.T().Helper()

	id := env.RegisterSchema(s.T(), subject, sr.Schema{Schema: orderAvro})

	payload, err := avro.MustParse(orderAvro).Encode(map[string]any{"id": "o-1", "amount": int64(5)})
	s.Require().NoError(err, "the fixture record must encode")

	return testenv.WireFormat(s.T(), id, nil, payload), id
}

// schemaID reads the id from a framed message.
func schemaID(value string) int {
	return int(value[1])<<24 | int(value[2])<<16 | int(value[3])<<8 | int(value[4])
}

func (s *CopyMessageSuite) TestWarnsWhenASchemaIDCrossesRegistries() {
	source := s.env.CreateTopic(s.T(), "copy-schema-src")
	destination := s.other.CreateTopic(s.T(), "copy-schema-dst")

	// A filler on the source registry makes the source id differ from what
	// the destination registry would assign the same schema.
	s.env.RegisterSchema(s.T(), s.env.UniqueName("filler")+"-value", sr.Schema{Schema: `"string"`})

	value, _ := s.avroMessage(s.env, source+"-value")
	s.env.Produce(s.T(), source, testenv.Message{Value: value})

	out, err := s.copyOne(s.clusters(false, false), "here", false, copymessage.Item{
		SourceTopic:        source,
		DestinationTopic:   destination,
		DestinationCluster: "there",
	})

	s.Require().NoError(err, "previewing a cross-registry copy must succeed")
	s.Require().NotEmpty(out.Warnings,
		"a schema id is only meaningful in the registry that issued it, so copying it unchanged to another must not pass silently")
	s.Require().Contains(out.Warnings[len(out.Warnings)-1], "translate_schema",
		"the warning must name the parameter that fixes it")
}

func (s *CopyMessageSuite) TestTranslatesTheSchemaIDToTheDestinationRegistry() {
	source := s.env.CreateTopic(s.T(), "copy-translate-src")
	destination := s.other.CreateTopic(s.T(), "copy-translate-dst")

	s.env.RegisterSchema(s.T(), s.env.UniqueName("filler")+"-value", sr.Schema{Schema: `"string"`})

	value, sourceID := s.avroMessage(s.env, source+"-value")
	s.env.Produce(s.T(), source, testenv.Message{Key: "o-1", Value: value})

	out, err := s.copyOne(s.clusters(false, false), "here", true, copymessage.Item{
		SourceTopic:        source,
		DestinationTopic:   destination,
		DestinationCluster: "there",
		TranslateSchema:    true,
	})

	s.Require().NoError(err, "a translated copy must succeed")

	_, copied, _ := s.readOn(s.other, destination, out.WrittenOffset)

	s.Run("the copy carries the destination's id for the same schema", func() {
		lookup, err := sr.NewClient(sr.URLs(s.other.SchemaRegistry()))
		s.Require().NoError(err, "the destination registry must be reachable")

		schema, err := lookup.SchemaByID(s.T().Context(), schemaID(copied))
		s.Require().NoError(err, "the id in the copy must exist in the destination registry")
		s.Require().JSONEq(orderAvro, schema.Schema,
			"the id must name the very schema the message was written with, or the consumer decodes garbage")
		s.Require().NotEqual(sourceID, schemaID(copied),
			"this fixture makes the ids differ, so an unchanged id would mean nothing was translated")
	})

	s.Run("the payload is untouched", func() {
		s.Require().Equal(value[5:], copied[5:],
			"translation rewrites the id only; the record itself must arrive byte for byte")
	})

	s.Run("the schema is registered under the destination topic's subject", func() {
		s.Require().Equal(destination+"-value", out.ValueSchema.Subject,
			"the topic name strategy is what a destination consumer looks the schema up by")
		s.Require().Equal(schemaID(copied), out.ValueSchema.SchemaID,
			"the reported id must be the one written")
	})
}

func (s *CopyMessageSuite) TestTranslationIsRefusedWithoutADestinationRegistry() {
	source := s.env.CreateTopic(s.T(), "copy-no-registry-src")
	destination := s.env.CreateTopic(s.T(), "copy-no-registry-dst")

	value, _ := s.avroMessage(s.env, source+"-value")
	s.env.Produce(s.T(), source, testenv.Message{Value: value})

	registry, err := kafkaclient.NewRegistry(&config.Config{
		Clusters: map[string]*config.Cluster{
			"here": {Name: "here", Brokers: []string{s.env.Broker()},
				SchemaRegistry: &config.SchemaRegistry{URLs: []string{s.env.SchemaRegistry()}}},
			"bare": {Name: "bare", Brokers: []string{s.env.Broker()}},
		},
	})
	s.Require().NoError(err, "building the registry must succeed")
	s.T().Cleanup(registry.Close)

	_, err = s.copyOne(registry, "here", false, copymessage.Item{
		SourceTopic:        source,
		DestinationTopic:   destination,
		DestinationCluster: "bare",
		TranslateSchema:    true,
	})

	s.Require().ErrorContains(err, "schema_registry",
		"there is nowhere to register the schema, and copying the source id unchanged is exactly what was asked not to happen")
}

func (s *CopyMessageSuite) TestTranslationLeavesPlainMessagesAlone() {
	source := s.env.CreateTopic(s.T(), "copy-plain-src")
	destination := s.other.CreateTopic(s.T(), "copy-plain-dst")

	s.env.Produce(s.T(), source, testenv.Message{Value: `{"plain":true}`})

	out, err := s.copyOne(s.clusters(false, false), "here", true, copymessage.Item{
		SourceTopic:        source,
		DestinationTopic:   destination,
		DestinationCluster: "there",
		TranslateSchema:    true,
	})

	s.Require().NoError(err, "a message without a schema needs no translation, and asking for it must not fail")

	_, copied, _ := s.readOn(s.other, destination, out.WrittenOffset)
	s.Require().Equal(`{"plain":true}`, copied, "a plain message must be copied byte for byte")
	s.Require().Nil(out.ValueSchema, "there was no schema to translate, so none may be reported")
}

func (s *CopyMessageSuite) TestCopiesTheMessageIntact() {
	source := s.env.CreateTopic(s.T(), "copy-source")
	destination := s.env.CreateTopic(s.T(), "copy-destination")

	s.env.Produce(s.T(), source, testenv.Message{
		Key:     "order-7",
		Value:   `{"id":7,"state":"POISON"}`,
		Headers: map[string]string{"correlation-id": "corr-7"},
	})

	out, err := s.copyOne(s.client(false), "here", true, copymessage.Item{
		SourceTopic:      source,
		SourcePartition:  0,
		SourceOffset:     0,
		DestinationTopic: destination,
	})

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

	_, err := s.copyOne(s.client(false), "here", true, copymessage.Item{
		SourceTopic:      source,
		SourcePartition:  0,
		SourceOffset:     1,
		DestinationTopic: destination,
	})

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

	out, err := s.copyOne(s.client(false), "here", true, copymessage.Item{
		SourceTopic:      source,
		SourcePartition:  0,
		SourceOffset:     0,
		DestinationTopic: destination,
	})

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

	out, err := s.copyOne(s.client(false), "here", false, copymessage.Item{
		SourceTopic:      source,
		SourcePartition:  0,
		SourceOffset:     0,
		DestinationTopic: destination,
	})

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

	_, err := s.copyOne(s.client(true), "here", false, copymessage.Item{
		SourceTopic:      source,
		SourcePartition:  0,
		SourceOffset:     0,
		DestinationTopic: destination,
	})

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

	_, err := s.copyOne(s.client(false), "here", true, copymessage.Item{
		SourceTopic:      source,
		SourcePartition:  0,
		SourceOffset:     0,
		DestinationTopic: s.env.UniqueName("never-created"),
	})

	s.Require().Error(err,
		"the destination must already exist: creating it silently would hide a typo and scatter messages into topics nobody meant to make")
	s.Require().Contains(err.Error(), "does not exist",
		"the caller must be told the topic is missing in words they can act on, not handed the broker's UNKNOWN_TOPIC_OR_PARTITION code")
}

func (s *CopyMessageSuite) TestErrorsWhenTheSourceOffsetDoesNotExist() {
	source := s.env.CreateTopic(s.T(), "copy-missing-offset-source")
	destination := s.env.CreateTopic(s.T(), "copy-missing-offset-destination")

	s.env.Produce(s.T(), source, testenv.Message{Value: "only one"})

	_, err := s.copyOne(s.client(false), "here", true, copymessage.Item{
		SourceTopic:      source,
		SourcePartition:  0,
		SourceOffset:     99,
		DestinationTopic: destination,
	})

	s.Require().Error(err,
		"an offset that holds no message must fail rather than write an empty copy that looks like a real message")
	s.Require().Zero(s.endOffset(destination),
		"nothing may have been written")
}

func (s *CopyMessageSuite) TestErrorsWhenSourceAndDestinationAreTheSame() {
	source := s.env.CreateTopic(s.T(), "copy-same-topic")

	s.env.Produce(s.T(), source, testenv.Message{Value: "payload"})

	_, err := s.copyOne(s.client(false), "here", true, copymessage.Item{
		SourceTopic:      source,
		SourcePartition:  0,
		SourceOffset:     0,
		DestinationTopic: source,
	})

	s.Require().Error(err,
		"copying a topic onto itself appends a duplicate to the topic being debugged, which is never what the caller meant")
}

func (s *CopyMessageSuite) TestCopiesToAnotherCluster() {
	source := s.env.CreateTopic(s.T(), "cross-source")
	destination := s.other.CreateTopic(s.T(), "cross-destination")

	s.env.Produce(s.T(), source, testenv.Message{
		Key:     "order-42",
		Value:   `{"id":42}`,
		Headers: map[string]string{"correlation-id": "corr-42"},
	})

	out, err := s.copyOne(s.clusters(false, false), "here", true, copymessage.Item{
		SourceTopic:        source,
		SourcePartition:    0,
		SourceOffset:       0,
		DestinationTopic:   destination,
		DestinationCluster: "there",
	})

	s.Require().NoError(err, "copying to another cluster must succeed")
	s.Require().True(out.Applied, "a copy that happened must be reported as applied")

	key, value, headers := s.readOn(s.other, destination, 0)

	s.Run("the message arrives on the other cluster", func() {
		s.Require().Equal("order-42", key,
			"the key must survive a cross-cluster copy, or the message is not the same message")
		s.Require().Equal(`{"id":42}`, value,
			"the value is the whole reason for copying, and must arrive byte for byte on the other cluster")
		s.Require().Equal("corr-42", headers["correlation-id"],
			"original headers must cross too, since they carry the correlation ids that make a message traceable")
	})

	s.Run("the source cluster is recorded", func() {
		s.Require().Equal("here", headers["kafka-mcp-copied-from-cluster"],
			"once two clusters are in play, the source topic alone is ambiguous: a message in preprod must say it came from prod")
	})

	s.Run("nothing was written to the source cluster", func() {
		s.Require().Equal(int64(1), s.endOffsetOn(s.env, source),
			"a copy must not append to the cluster it read from, which would duplicate the message being investigated")
	})
}

func (s *CopyMessageSuite) TestRefusesAReadOnlyDestinationCluster() {
	source := s.env.CreateTopic(s.T(), "cross-ro-source")
	destination := s.other.CreateTopic(s.T(), "cross-ro-destination")

	s.env.Produce(s.T(), source, testenv.Message{Value: "payload"})

	_, err := s.copyOne(s.clusters(false, true), "here", true, copymessage.Item{
		SourceTopic:        source,
		SourcePartition:    0,
		SourceOffset:       0,
		DestinationTopic:   destination,
		DestinationCluster: "there",
	})

	s.Require().Error(err,
		"read_only protects the cluster being written to, so a read-only destination must refuse the copy")
	s.Require().Contains(err.Error(), "read-only",
		"the error must name the reason, so the operator looks at that cluster's configuration")
	s.Require().Zero(s.endOffsetOn(s.other, destination),
		"nothing may have been written to the read-only cluster")
}

func (s *CopyMessageSuite) TestAllowsAReadOnlySourceCluster() {
	source := s.env.CreateTopic(s.T(), "cross-ro-src-source")
	destination := s.other.CreateTopic(s.T(), "cross-ro-src-destination")

	s.env.Produce(s.T(), source, testenv.Message{Value: "payload"})

	out, err := s.copyOne(s.clusters(true, false), "here", true, copymessage.Item{
		SourceTopic:        source,
		SourcePartition:    0,
		SourceOffset:       0,
		DestinationTopic:   destination,
		DestinationCluster: "there",
	})

	s.Require().NoError(err,
		"copying out of a read-only cluster changes nothing there, so it must be allowed: this is how a message is rescued from production")
	s.Require().True(out.Applied, "the copy must have happened")
	s.Require().Equal(int64(1), s.endOffsetOn(s.other, destination),
		"the message must have arrived on the writable cluster")
}

func (s *CopyMessageSuite) TestRefusesADestinationTheEndpointDoesNotList() {
	source := s.env.CreateTopic(s.T(), "cross-unlisted-source")
	destination := s.other.CreateTopic(s.T(), "cross-unlisted-destination")

	s.env.Produce(s.T(), source, testenv.Message{Value: "payload"})

	registry, err := kafkaclient.NewRegistry(&config.Config{
		Clusters: map[string]*config.Cluster{
			"here":  {Name: "here", Brokers: []string{s.env.Broker()}},
			"there": {Name: "there", Brokers: []string{s.other.Broker()}},
		},
		Endpoints: map[string]*config.Endpoint{
			"here-read": {Name: "here-read", Cluster: "here", ReadOnly: true},
			"there":     {Name: "there", Cluster: "there"},
		},
	})
	s.Require().NoError(err, "building the registry must succeed")
	s.T().Cleanup(registry.Close)

	_, err = s.copyOne(registry, "here-read", true, copymessage.Item{
		SourceTopic:        source,
		DestinationTopic:   destination,
		DestinationCluster: "there",
	})

	s.Require().ErrorContains(err, "destinations",
		"another endpoint can write to there, but that is not this session's permission")
	s.Require().Zero(s.endOffsetOn(s.other, destination), "nothing may have been written")
}

func (s *CopyMessageSuite) TestErrorsOnAnUnknownDestinationCluster() {
	source := s.env.CreateTopic(s.T(), "cross-unknown-source")

	s.env.Produce(s.T(), source, testenv.Message{Value: "payload"})

	_, err := s.copyOne(s.clusters(false, false), "here", true, copymessage.Item{
		SourceTopic:        source,
		SourcePartition:    0,
		SourceOffset:       0,
		DestinationTopic:   "anything",
		DestinationCluster: "never-configured",
	})

	s.Require().Error(err,
		"a destination cluster that does not exist must fail by name, so the caller can correct it from list_clusters rather than guess")
	s.Require().Contains(err.Error(), "never-configured",
		"the error must quote the unknown name, since a typo is the likeliest cause")
}

func (s *CopyMessageSuite) TestBatchCopiesSeveralMessagesInInputOrder() {
	source := s.env.CreateTopic(s.T(), "copy-batch-source")
	destination := s.env.CreateTopic(s.T(), "copy-batch-destination")
	s.env.Produce(s.T(), source,
		testenv.Message{Value: "first"},
		testenv.Message{Value: "second"},
	)

	out, err := copymessage.Run(
		s.T().Context(), s.client(false), "here",
		copymessage.Input{
			Items: []copymessage.Item{
				{SourceTopic: source, SourcePartition: 0, SourceOffset: 0, DestinationTopic: destination},
				{SourceTopic: source, SourcePartition: 0, SourceOffset: 1, DestinationTopic: destination},
			},
			Confirm: true,
		},
	)

	s.Require().NoError(err, "copying a valid batch must succeed")
	s.Require().Equal(2, out.Succeeded, "both source messages must be copied")
	s.Require().Zero(out.Failed, "a valid batch must not report failed items")
	s.Require().EqualValues(2, s.endOffset(destination), "the destination must contain both copies")
	s.Require().EqualValues(0, out.Results[0].Result.WrittenOffset, "the first result must correspond to the first input")
	s.Require().EqualValues(1, out.Results[1].Result.WrittenOffset, "the second result must correspond to the second input")
}
