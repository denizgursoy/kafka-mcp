package transformmessages_test

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/twmb/avro"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"

	"github.com/denizgursoy/kafka-mcp/internal/domain/config"
	"github.com/denizgursoy/kafka-mcp/internal/domain/kafkaclient"
	"github.com/denizgursoy/kafka-mcp/internal/domain/records"
	"github.com/denizgursoy/kafka-mcp/internal/domain/testenv"
	"github.com/denizgursoy/kafka-mcp/internal/tools/producemessage"
	"github.com/denizgursoy/kafka-mcp/internal/tools/transformmessages"
)

type TransformMessagesSuite struct {
	suite.Suite

	env   *testenv.Environment
	other *testenv.Environment
}

func TestTransformMessagesSuite(t *testing.T) {
	suite.Run(t, new(TransformMessagesSuite))
}

func (s *TransformMessagesSuite) SetupSuite() {
	// Two real brokers, so writing the repaired messages to another cluster is
	// proven to cross a cluster boundary.
	s.env, s.other = testenv.StartPair(s.T())
}

func (s *TransformMessagesSuite) TearDownSuite() {
	s.other.Stop()
	s.env.Stop()
}

// clusters builds a registry holding this suite's two brokers, under the names
// a caller would pass as destination_cluster.
func (s *TransformMessagesSuite) clusters(readOnly bool, otherReadOnly bool) *kafkaclient.Registry {
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

func (s *TransformMessagesSuite) run(registry *kafkaclient.Registry, dir string, input transformmessages.Input) (transformmessages.Output, error) {
	s.T().Helper()

	return transformmessages.Run(s.T().Context(), registry, "here", dir, input)
}

func (s *TransformMessagesSuite) endOffsetOn(env *testenv.Environment, topic string) int64 {
	s.T().Helper()

	ends, err := env.Admin().ListEndOffsets(s.T().Context(), topic)
	s.Require().NoError(err, "reading the end offset must succeed")

	end, ok := ends.Lookup(topic, 0)
	s.Require().True(ok, "the topic must have partition 0")

	return end.Offset
}

// readAllOn returns every record in partition 0 of a topic, read straight from
// the broker, so assertions are made on what was written rather than on what
// the tool reported about itself.
func (s *TransformMessagesSuite) readAllOn(env *testenv.Environment, topic string) []*kgo.Record {
	s.T().Helper()

	end := s.endOffsetOn(env, topic)
	found := make([]*kgo.Record, 0, end)

	if end == 0 {
		return found
	}

	err := env.Reader().Scan(s.T().Context(), topic,
		[]records.Range{{Partition: 0, Start: 0, End: end}},
		func(record *kgo.Record) bool {
			found = append(found, record)

			return true
		})
	s.Require().NoError(err, "reading the topic back must succeed")
	s.Require().Len(found, int(end), "every offset of the topic must be read back")

	return found
}

func header(record *kgo.Record, key string) (string, bool) {
	for _, h := range record.Headers {
		if h.Key == key {
			return string(h.Value), true
		}
	}

	return "", false
}

// wrapped produces the messages this tool exists for: the real document
// mistakenly nested under payload.
func (s *TransformMessagesSuite) wrapped(topic string) {
	s.T().Helper()

	s.env.Produce(s.T(), topic,
		testenv.Message{Key: "o-1", Value: `{"payload":{"id":"o-1","amount":5}}`, Headers: map[string]string{"trace": "t-1"}},
		testenv.Message{Key: "o-2", Value: `{"payload":{"id":"o-2","amount":7}}`, Headers: map[string]string{"trace": "t-2"}},
		testenv.Message{Key: "o-3", Value: `{"payload":{"id":"o-3","amount":9}}`, Headers: map[string]string{"trace": "t-3"}},
	)
}

func (s *TransformMessagesSuite) TestUnwrapsThePayloadIntoAnotherTopic() {
	source := s.env.CreateTopic(s.T(), "transform-src")
	destination := s.env.CreateTopic(s.T(), "transform-dst")
	s.wrapped(source)

	out, err := s.run(s.clusters(false, false), "", transformmessages.Input{
		Topic:            source,
		Transform:        `return { value: value.payload }`,
		DestinationTopic: destination,
		Confirm:          true,
	})
	s.Require().NoError(err, "unwrapping a nested payload is the case this tool exists for and must succeed")

	written := s.readAllOn(s.env, destination)

	s.Run("every message is written once, in order", func() {
		s.Require().Len(written, 3,
			"three wrapped messages must become exactly three repaired ones, or a repair has lost or duplicated data")
		s.Require().Equal(3, out.Written, "the response must report what was actually written")
		s.Require().True(out.Applied, "a confirmed transform that wrote messages must say it applied")
	})

	s.Run("the value is the payload itself", func() {
		s.Require().JSONEq(`{"id":"o-1","amount":5}`, string(written[0].Value),
			"the new value must be the nested document, not the envelope around it")
		s.Require().JSONEq(`{"id":"o-3","amount":9}`, string(written[2].Value),
			"messages must be written in source order, so the last source message is the last written")
	})

	s.Run("key and headers are kept when the transform does not set them", func() {
		s.Require().Equal("o-2", string(written[1].Key),
			"a key the transform did not return must be kept, or the repaired message lands on a different partition than its key's history")

		trace, ok := header(written[1], "trace")
		s.Require().True(ok, "headers the transform did not touch must be kept, because consumers may route on them")
		s.Require().Equal("t-2", trace, "a kept header must keep its value")
	})

	s.Run("each message says where it came from", func() {
		from, ok := header(written[0], "kafka-mcp-transformed-from-topic")
		s.Require().True(ok, "a rewritten message must carry provenance, or nobody can tell it from what a producer sent")
		s.Require().Equal(source, from, "provenance must name the source topic")

		offset, ok := header(written[0], "kafka-mcp-transformed-from-offset")
		s.Require().True(ok, "provenance must name the source offset, so a repair can be traced to the exact original")
		s.Require().Equal("0", offset, "the first written message came from offset 0")
	})
}

func (s *TransformMessagesSuite) TestPreviewWritesNothing() {
	source := s.env.CreateTopic(s.T(), "transform-preview-src")
	destination := s.env.CreateTopic(s.T(), "transform-preview-dst")
	s.wrapped(source)

	out, err := s.run(s.clusters(false, false), "", transformmessages.Input{
		Topic:            source,
		Transform:        `return { value: value.payload }`,
		DestinationTopic: destination,
	})
	s.Require().NoError(err, "previewing a transform must succeed")

	s.Require().Zero(s.endOffsetOn(s.env, destination),
		"without confirm nothing may be written, because the preview is how a user checks the repair before it is irreversible")
	s.Require().False(out.Applied, "a preview must not claim it applied")
	s.Require().Equal(3, out.Transformed,
		"the preview must count every message it would write, so the user approves the real number")
	s.Require().True(out.Complete, "a preview that read the whole topic must say so")
	s.Require().NotEmpty(out.Examples, "the preview must show what a repaired message looks like")
	s.Require().JSONEq(`{"payload":{"id":"o-1","amount":5}}`, out.Examples[0].Before.Value,
		"an example must show the original, so the change is visible")
	s.Require().JSONEq(`{"id":"o-1","amount":5}`, out.Examples[0].After.Value,
		"an example must show the message that would be written")
}

func (s *TransformMessagesSuite) TestScriptSelectsWhichMessagesAreTransformed() {
	source := s.env.CreateTopic(s.T(), "transform-select-src")
	destination := s.env.CreateTopic(s.T(), "transform-select-dst")
	s.env.Produce(s.T(), source,
		testenv.Message{Key: "a", Value: `{"payload":{"id":"a"}}`},
		testenv.Message{Key: "b", Value: `{"id":"b"}`},
		testenv.Message{Key: "c", Value: `{"payload":{"id":"c"}}`},
	)

	out, err := s.run(s.clusters(false, false), "", transformmessages.Input{
		Topic:            source,
		Script:           `return value.payload !== undefined`,
		Transform:        `return { value: value.payload }`,
		DestinationTopic: destination,
		Confirm:          true,
	})
	s.Require().NoError(err, "a selective transform must succeed")

	written := s.readAllOn(s.env, destination)

	s.Require().Len(written, 2,
		"only the wrapped messages may be rewritten: a message that was already correct must not be touched")
	s.Require().Equal(3, out.Scanned, "every message in the range must be read")
	s.Require().Equal(2, out.Matched, "only messages the script selects count as matched")
	s.Require().Equal("c", string(written[1].Key), "the selected messages must be written in source order")
}

func (s *TransformMessagesSuite) TestReturningNullSkipsAMessage() {
	source := s.env.CreateTopic(s.T(), "transform-skip-src")
	destination := s.env.CreateTopic(s.T(), "transform-skip-dst")
	s.wrapped(source)

	out, err := s.run(s.clusters(false, false), "", transformmessages.Input{
		Topic:            source,
		Transform:        `if (key === 'o-2') return null; return { value: value.payload }`,
		DestinationTopic: destination,
		Confirm:          true,
	})
	s.Require().NoError(err, "a transform that skips some messages must succeed")

	s.Require().Len(s.readAllOn(s.env, destination), 2,
		"a message the transform returned null for must not be written")
	s.Require().Equal(1, out.Skipped, "a skipped message must be counted, so the user can see the transform left it out")
}

func (s *TransformMessagesSuite) TestTransformErrorsAreReportedWithTheirAddresses() {
	source := s.env.CreateTopic(s.T(), "transform-error-src")
	destination := s.env.CreateTopic(s.T(), "transform-error-dst")
	s.env.Produce(s.T(), source,
		testenv.Message{Key: "a", Value: `{"payload":{"id":"a"}}`},
		testenv.Message{Key: "b", Value: `{"other":1}`},
		testenv.Message{Key: "c", Value: `{"payload":{"id":"c"}}`},
	)

	out, err := s.run(s.clusters(false, false), "", transformmessages.Input{
		Topic:            source,
		Transform:        `return { value: { id: value.payload.id } }`,
		DestinationTopic: destination,
		Confirm:          true,
	})
	s.Require().NoError(err, "one message failing the transform must not fail the whole call")

	s.Run("the failing message is not written and the others are", func() {
		s.Require().Len(s.readAllOn(s.env, destination), 2,
			"a message the transform failed on must be left out rather than written half-transformed")
		s.Require().Equal(1, out.Failed, "the failure must be counted")
	})

	s.Run("the failure names the message it happened on", func() {
		s.Require().Len(out.Errors, 1, "one distinct failure must be reported once")
		s.Require().Equal(1, out.Errors[0].Count, "the failure must say how many messages it hit")
		s.Require().Equal([]transformmessages.Address{{Partition: 0, Offset: 1}}, out.Errors[0].Examples,
			"the failure must name the address of the message, so the user can read it with get_message")
	})
}

func (s *TransformMessagesSuite) TestRewritesInPlaceWithoutReadingItsOwnOutput() {
	topic := s.env.CreateTopic(s.T(), "transform-inplace")
	s.wrapped(topic)

	out, err := s.run(s.clusters(false, false), "", transformmessages.Input{
		Topic:     topic,
		Transform: `return { value: value.payload }`,
		Confirm:   true,
	})
	s.Require().NoError(err, "writing the repaired messages back to the same topic must succeed")

	written := s.readAllOn(s.env, topic)

	s.Require().Len(written, 6,
		"each of the three originals must be repaired exactly once: reading the messages this call wrote would repair the repairs, and on a large topic never stop")
	s.Require().Equal(3, out.Written, "the response must report three messages written")
	s.Require().JSONEq(`{"id":"o-1","amount":5}`, string(written[3].Value),
		"the repaired messages must follow the originals in the same topic")
}

func (s *TransformMessagesSuite) TestChangesKeyAndHeaders() {
	source := s.env.CreateTopic(s.T(), "transform-key-src")
	destination := s.env.CreateTopic(s.T(), "transform-key-dst")
	s.wrapped(source)

	_, err := s.run(s.clusters(false, false), "", transformmessages.Input{
		Topic:            source,
		Transform:        `return { key: value.payload.id.toUpperCase(), headers: { fixed: 'yes' } }`,
		DestinationTopic: destination,
		Confirm:          true,
	})
	s.Require().NoError(err, "rewriting the key and headers must succeed")

	written := s.readAllOn(s.env, destination)

	s.Require().Equal("O-1", string(written[0].Key), "a key the transform returned must replace the original")
	s.Require().JSONEq(`{"payload":{"id":"o-1","amount":5}}`, string(written[0].Value),
		"a value the transform did not return must be kept byte for byte")

	_, kept := header(written[0], "trace")
	s.Require().False(kept, "returning headers replaces them, so a header left out of the returned object must be gone")

	fixed, ok := header(written[0], "fixed")
	s.Require().True(ok, "a header the transform returned must be written")
	s.Require().Equal("yes", fixed, "a returned header must keep its value")
}

const orderAvro = `{"type":"record","name":"Order","namespace":"shop","fields":[
	{"name":"id","type":"string"},{"name":"amount","type":"long"}]}`

func (s *TransformMessagesSuite) TestReencodesAvroWithTheOriginalSchema() {
	source := s.env.CreateTopic(s.T(), "transform-avro-src")
	destination := s.env.CreateTopic(s.T(), "transform-avro-dst")

	id := s.env.RegisterSchema(s.T(), source+"-value", sr.Schema{Schema: orderAvro})
	payload, err := avro.MustParse(orderAvro).Encode(map[string]any{"id": "o-1", "amount": int64(5)})
	s.Require().NoError(err, "the fixture record must encode")
	s.env.Produce(s.T(), source, testenv.Message{Key: "o-1", Value: testenv.WireFormat(s.T(), id, nil, payload)})

	_, err = s.run(s.clusters(false, false), "", transformmessages.Input{
		Topic:            source,
		Transform:        `return { value: { id: value.id, amount: value.amount * 10 } }`,
		DestinationTopic: destination,
		Confirm:          true,
	})
	s.Require().NoError(err, "transforming an Avro message must succeed")

	written := s.readAllOn(s.env, destination)
	s.Require().Len(written, 1, "the one message must be written")

	value := written[0].Value
	s.Require().Equal([]byte{0, byte(id >> 24), byte(id >> 16), byte(id >> 8), byte(id)}, value[:5],
		"the rewritten value must carry the schema id it was read with, or registry-aware consumers cannot read it")

	var decoded map[string]any
	_, err = avro.MustParse(orderAvro).Decode(value[5:], &decoded)
	s.Require().NoError(err, "the payload must be valid Avro for the original schema")
	s.Require().Equal(int64(50), decoded["amount"], "the transformed field must be what was encoded")
}

func (s *TransformMessagesSuite) TestWritesToAnotherCluster() {
	source := s.env.CreateTopic(s.T(), "transform-cross-src")
	destination := s.other.CreateTopic(s.T(), "transform-cross-dst")
	s.wrapped(source)

	_, err := s.run(s.clusters(true, false), "", transformmessages.Input{
		Topic:              source,
		Transform:          `return { value: value.payload }`,
		DestinationTopic:   destination,
		DestinationCluster: "there",
		Confirm:            true,
	})
	s.Require().NoError(err,
		"read_only protects the cluster written to, so repairing out of a read-only cluster into a writable one must work")

	s.Require().Len(s.readAllOn(s.other, destination), 3, "every repaired message must reach the other cluster")
}

func (s *TransformMessagesSuite) TestRefusesToWriteToAReadOnlyCluster() {
	topic := s.env.CreateTopic(s.T(), "transform-readonly")
	s.wrapped(topic)

	_, err := s.run(s.clusters(true, false), "", transformmessages.Input{
		Topic:     topic,
		Transform: `return { value: value.payload }`,
		Confirm:   true,
	})
	s.Require().Error(err, "a read-only cluster must refuse to be written to")
	s.Require().Equal(int64(3), s.endOffsetOn(s.env, topic), "a refused transform must leave the topic untouched")
}

func (s *TransformMessagesSuite) TestPreviewsAndSavesOnAReadOnlyCluster() {
	topic := s.env.CreateTopic(s.T(), "transform-readonly-save")
	s.wrapped(topic)
	dir := s.T().TempDir()

	out, err := s.run(s.clusters(true, false), dir, transformmessages.Input{
		Topic:      topic,
		Transform:  `return { value: value.payload }`,
		OutputFile: "repaired.jsonl",
	})
	s.Require().NoError(err,
		"previewing and saving to a file write nothing to the cluster, so read_only must not stop them")
	s.Require().Equal(3, out.FileWritten, "every transformed message must be saved")
	s.Require().Equal(int64(3), s.endOffsetOn(s.env, topic), "saving must not write to Kafka")
}

func (s *TransformMessagesSuite) TestSavedFileIsSentWithProduceMessage() {
	source := s.env.CreateTopic(s.T(), "transform-file-src")
	destination := s.env.CreateTopic(s.T(), "transform-file-dst")
	s.wrapped(source)
	dir := s.T().TempDir()
	registry := s.clusters(false, false)

	out, err := s.run(registry, dir, transformmessages.Input{
		Topic:            source,
		Transform:        `return { value: value.payload }`,
		DestinationTopic: destination,
		OutputFile:       "repaired.jsonl",
	})
	s.Require().NoError(err, "saving the transformed messages must succeed")
	s.Require().Equal(filepath.Join(dir, "repaired.jsonl"), out.OutputFile,
		"the response must say where the file is, so the user can inspect it")
	s.Require().Zero(s.endOffsetOn(s.env, destination), "saving without confirm must not write to Kafka")

	s.Run("each line is a produce_message item", func() {
		file, err := os.Open(out.OutputFile)
		s.Require().NoError(err, "the saved file must be readable")
		defer file.Close()

		lines := 0
		scanner := bufio.NewScanner(file)

		for scanner.Scan() {
			var item producemessage.Item
			s.Require().NoError(json.Unmarshal(scanner.Bytes(), &item),
				"every line must decode as a produce_message item, or the file cannot be sent later")
			s.Require().Equal(destination, item.Topic, "each line must name the destination topic")
			lines++
		}

		s.Require().Equal(3, lines, "every transformed message must be one line")
	})

	s.Run("produce_message sends the file", func() {
		sent, err := producemessage.Run(s.T().Context(), registry, "here", dir, producemessage.Input{
			InputFile: "repaired.jsonl",
			Confirm:   true,
		})
		s.Require().NoError(err, "sending a saved file must succeed")
		s.Require().NotNil(sent.File, "a file send must report its own summary")
		s.Require().Equal(3, sent.File.Written, "every line must be written")

		written := s.readAllOn(s.env, destination)
		s.Require().Len(written, 3, "the destination must hold exactly the saved messages")
		s.Require().JSONEq(`{"id":"o-2","amount":7}`, string(written[1].Value),
			"the sent value must be the saved, repaired one")
		s.Require().Equal("o-2", string(written[1].Key), "the saved key must be sent with it")

		from, ok := header(written[1], "kafka-mcp-transformed-from-offset")
		s.Require().True(ok, "the transform's provenance must survive the trip through the file")
		s.Require().Equal("1", from, "provenance must still point at the original offset")
	})
}

func (s *TransformMessagesSuite) TestStopsAtMaxMessages() {
	source := s.env.CreateTopic(s.T(), "transform-limit-src")
	destination := s.env.CreateTopic(s.T(), "transform-limit-dst")
	s.wrapped(source)

	out, err := s.run(s.clusters(false, false), "", transformmessages.Input{
		Topic:            source,
		Transform:        `return { value: value.payload }`,
		DestinationTopic: destination,
		MaxMessages:      2,
	})
	s.Require().NoError(err, "a limited preview must succeed")

	s.Require().False(out.Complete,
		"a scan cut short by max_messages must not claim it covered the topic, or the user confirms a partial repair")
	s.Require().Equal("max_messages", out.StoppedReason, "the response must say why it stopped")
	s.Require().Equal(2, out.Matched, "no more than max_messages may be taken")
}

func (s *TransformMessagesSuite) TestRefusesInvalidInput() {
	topic := s.env.CreateTopic(s.T(), "transform-invalid")
	s.wrapped(topic)
	registry := s.clusters(false, false)

	s.Run("a missing transform", func() {
		_, err := s.run(registry, "", transformmessages.Input{Topic: topic})
		s.Require().ErrorContains(err, "transform",
			"a call without a transform has nothing to do, and must say which parameter is missing")
	})

	s.Run("a transform that does not compile", func() {
		_, err := s.run(registry, "", transformmessages.Input{Topic: topic, Transform: `return {`})
		s.Require().ErrorContains(err, "compile",
			"a malformed transform must be refused before any message is read")
	})

	s.Run("a source topic that does not exist", func() {
		_, err := s.run(registry, "", transformmessages.Input{
			Topic: s.env.UniqueName("missing"), Transform: `return {}`,
		})
		s.Require().ErrorContains(err, "does not exist", "a missing topic must fail, not look like an empty one")
	})

	s.Run("a destination topic that does not exist", func() {
		_, err := s.run(registry, "", transformmessages.Input{
			Topic: topic, Transform: `return {}`, DestinationTopic: s.env.UniqueName("missing"),
		})
		s.Require().ErrorContains(err, "does not exist",
			"a mistyped destination must be refused rather than auto-created")
	})

	s.Run("a transform that returns something other than an object", func() {
		out, err := s.run(registry, "", transformmessages.Input{Topic: topic, Transform: `return 42`})
		s.Require().NoError(err, "a bad return value is a per-message failure, not a failure of the call")
		s.Require().Equal(3, out.Failed, "every message must fail when the transform returns a number")
		s.Require().Contains(out.Errors[0].Reason, "object",
			"the failure must explain the transform has to return an object")
	})

	s.Run("an unknown field in the returned object", func() {
		out, err := s.run(registry, "", transformmessages.Input{Topic: topic, Transform: `return { valeu: 1 }`})
		s.Require().NoError(err, "an unknown field is a per-message failure")
		s.Require().Contains(out.Errors[0].Reason, "valeu",
			"a misspelt field must be named, because silently ignoring it would write the original unchanged")
	})
}

func (s *TransformMessagesSuite) TestKeepsLargeIntegersExact() {
	source := s.env.CreateTopic(s.T(), "transform-bigint-src")
	destination := s.env.CreateTopic(s.T(), "transform-bigint-dst")
	s.env.Produce(s.T(), source,
		testenv.Message{Key: "a", Value: `{"payload":{"id":1234567890123456789,"amount":1.5}}`},
	)
	registry := s.clusters(false, false)

	s.Run("a number the transform passes through is written exactly", func() {
		_, err := s.run(registry, "", transformmessages.Input{
			Topic:            source,
			Transform:        `return { value: value.payload }`,
			DestinationTopic: destination,
			Confirm:          true,
		})
		s.Require().NoError(err, "unwrapping a payload with a 64-bit id must succeed")

		written := s.readAllOn(s.env, destination)
		s.Require().Len(written, 1, "the one message must be written")
		s.Require().JSONEq(`{"id":1234567890123456789,"amount":1.5}`, string(written[0].Value),
			"a 64-bit id must survive byte for byte: a JavaScript double would round it to 1234567890123456800, silently pointing the repaired message at a different entity")
	})

	s.Run("a number the transform rounds is refused rather than written", func() {
		out, err := s.run(registry, "", transformmessages.Input{
			Topic:            source,
			Transform:        `return { value: { id: Number(value.payload.id) } }`,
			DestinationTopic: destination,
		})
		s.Require().NoError(err, "a rounded number is a per-message failure, not a failure of the call")
		s.Require().Equal(1, out.Failed,
			"arithmetic turns a 64-bit integer into a double, and writing the rounded value would corrupt data without a trace")
		s.Require().Contains(out.Errors[0].Reason, "exactly",
			"the failure must say the number cannot be held exactly, so the caller knows to leave the field untouched")
	})
}

func (s *TransformMessagesSuite) TestKeepsALargeAvroLongExact() {
	source := s.env.CreateTopic(s.T(), "transform-bigavro-src")
	destination := s.env.CreateTopic(s.T(), "transform-bigavro-dst")

	id := s.env.RegisterSchema(s.T(), source+"-value", sr.Schema{Schema: orderAvro})
	payload, err := avro.MustParse(orderAvro).Encode(map[string]any{"id": "o-1", "amount": int64(1234567890123456789)})
	s.Require().NoError(err, "the fixture record must encode")
	s.env.Produce(s.T(), source, testenv.Message{Value: testenv.WireFormat(s.T(), id, nil, payload)})

	_, err = s.run(s.clusters(false, false), "", transformmessages.Input{
		Topic:            source,
		Transform:        `return { value: { ...value, id: value.id.toUpperCase() } }`,
		DestinationTopic: destination,
		Confirm:          true,
	})
	s.Require().NoError(err, "transforming one field of an Avro record must succeed")

	written := s.readAllOn(s.env, destination)
	s.Require().Len(written, 1, "the one message must be written")

	var decoded map[string]any
	_, err = avro.MustParse(orderAvro).Decode(written[0].Value[5:], &decoded)
	s.Require().NoError(err, "the written value must be valid Avro")
	s.Require().Equal(int64(1234567890123456789), decoded["amount"],
		"an Avro long the transform did not touch must be written exactly, not rounded through a JavaScript double")
	s.Require().Equal("O-1", decoded["id"], "the field the transform changed must be written")
}

func (s *TransformMessagesSuite) TestNullHeaderRemovesIt() {
	source := s.env.CreateTopic(s.T(), "transform-header-src")
	destination := s.env.CreateTopic(s.T(), "transform-header-dst")
	s.env.Produce(s.T(), source, testenv.Message{
		Value:   `{"payload":{"id":"a"}}`,
		Headers: map[string]string{"error-reason": "boom", "trace": "t-1"},
	})

	_, err := s.run(s.clusters(false, false), "", transformmessages.Input{
		Topic:            source,
		Transform:        `return { value: value.payload, headers: { ...headers, 'error-reason': null } }`,
		DestinationTopic: destination,
		Confirm:          true,
	})
	s.Require().NoError(err, "dropping one header while keeping the rest must succeed")

	written := s.readAllOn(s.env, destination)
	_, kept := header(written[0], "error-reason")
	s.Require().False(kept,
		"a header set to null must be removed, which is how a message taken out of a dead letter topic loses its error header")

	trace, ok := header(written[0], "trace")
	s.Require().True(ok, "the other headers spread in must be kept")
	s.Require().Equal("t-1", trace, "a kept header must keep its value")
}

func (s *TransformMessagesSuite) TestTimeoutDoesNotSkipTheInterruptedMessage() {
	topic := s.env.CreateTopic(s.T(), "transform-timeout")
	s.env.Produce(s.T(), topic,
		testenv.Message{Value: `{"n":1}`},
		testenv.Message{Value: `{"n":2}`},
	)

	out, err := s.run(s.clusters(false, false), "", transformmessages.Input{
		Topic:          topic,
		Transform:      `if (value.n === 2) { while (true) {} } return { value: value }`,
		TimeoutSeconds: 2,
	})
	s.Require().NoError(err, "a run cut short by its timeout must still report what it did")

	s.Require().Equal("timeout", out.StoppedReason, "the response must say the timeout stopped it")
	s.Require().False(out.Complete, "a timed-out run must not claim to be complete")
	s.Require().Zero(out.Failed,
		"a transform interrupted by the timeout did not fail on its message, and counting it as failed would send the user looking for a bug that is not there")
	s.Require().Equal([]transformmessages.ScannedRange{{Partition: 0, Start: 0, End: 1}}, out.ScannedRanges,
		"the interrupted message must not be counted as covered, or resuming from end would skip it and leave it unrepaired")
}

func (s *TransformMessagesSuite) TestAFailedWriteStopsAtTheMessageThatFailed() {
	source := s.env.CreateTopic(s.T(), "transform-writefail-src")
	destination := s.env.CreateTopicWithConfig(s.T(), "transform-writefail-dst", map[string]string{
		"max.message.bytes": "2048",
	})
	// Random bytes, because the broker checks the compressed batch and a run
	// of one character would compress below the limit.
	noise := make([]byte, 8192)
	_, err := rand.Read(noise)
	s.Require().NoError(err, "the payload must be generated")
	big := `"` + hex.EncodeToString(noise) + `"`
	s.env.Produce(s.T(), source,
		testenv.Message{Value: `{"payload":{"n":1}}`},
		testenv.Message{Value: `{"payload":` + big + `}`},
		testenv.Message{Value: `{"payload":{"n":3}}`},
	)

	out, err := s.run(s.clusters(false, false), "", transformmessages.Input{
		Topic:            source,
		Transform:        `return { value: value.payload }`,
		DestinationTopic: destination,
		Confirm:          true,
	})
	s.Require().NoError(err, "a refused write must be reported in the response, not as a failed call")

	s.Require().Equal("write_failed", out.StoppedReason, "the response must say a write stopped the run")
	s.Require().False(out.Complete, "a run with a refused write is not complete")
	// The broker refuses a produce batch as a whole, so every message sent
	// with the oversized one fails with it. What matters is that the count
	// and the range agree with what reached the topic.
	written := s.readAllOn(s.env, destination)
	s.Require().Equal(len(written), out.Written,
		"written must be what actually reached the destination, or the user cannot tell how much of the repair happened")
	s.Require().Equal(3-out.Written, out.Failed, "every message that did not reach the destination must be counted as failed")
	s.Require().Len(out.ScannedRanges, 1, "one partition was read")
	s.Require().Equal(int64(len(written)), out.ScannedRanges[0].End,
		"the range must end at the first message not written, so resuming from end retries it instead of skipping it")
}
