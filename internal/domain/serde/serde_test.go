package serde_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bufbuild/protocompile"
	"github.com/stretchr/testify/suite"
	"github.com/twmb/avro"
	"github.com/twmb/franz-go/pkg/sr"
	"github.com/vmihailenco/msgpack/v5"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/denizgursoy/kafka-mcp/internal/domain/config"
	"github.com/denizgursoy/kafka-mcp/internal/domain/serde"
	"github.com/denizgursoy/kafka-mcp/internal/domain/testenv"
)

const orderAvro = `{"type":"record","name":"Order","namespace":"shop","fields":[
	{"name":"id","type":"string"},
	{"name":"amount","type":"long"},
	{"name":"note","type":["null","string"],"default":null}]}`

const commonProto = `syntax = "proto3";
package common;
message Money { string currency = 1; int64 units = 2; }`

const orderProto = `syntax = "proto3";
package shop;
import "common.proto";
message Header { string source = 1; }
message Order {
  string id = 1;
  common.Money price = 2;
  Status status = 3;
  message Line { string sku = 1; int32 qty = 2; }
}
enum Status { NEW = 0; PAID = 1; }`

const orderJSONSchema = `{
	"$schema": "http://json-schema.org/draft-07/schema#",
	"type": "object",
	"properties": {"id": {"type": "string"}, "amount": {"type": "integer"}},
	"required": ["id", "amount"],
	"additionalProperties": false
}`

type SerdeSuite struct {
	suite.Suite

	env *testenv.Environment
}

func TestSerdeSuite(t *testing.T) {
	suite.Run(t, new(SerdeSuite))
}

func (s *SerdeSuite) SetupSuite() {
	s.env = testenv.Start(s.T())
}

func (s *SerdeSuite) TearDownSuite() {
	s.env.Stop()
}

// codec builds a codec for a cluster with this environment's registry and
// the given topic formats.
func (s *SerdeSuite) codec(formats map[string]*config.TopicFormat) *serde.Codec {
	s.T().Helper()

	codec, err := serde.New(&config.Cluster{
		Name:           "test",
		SchemaRegistry: &config.SchemaRegistry{URLs: []string{s.env.SchemaRegistry()}},
		TopicFormats:   formats,
	})
	s.Require().NoError(err, "a codec for a reachable registry and valid formats must build")

	return codec
}

func (s *SerdeSuite) withoutRegistry() *serde.Codec {
	s.T().Helper()

	codec, err := serde.New(&config.Cluster{Name: "test"})
	s.Require().NoError(err, "a cluster without a registry must still get a codec, for plain messages")

	return codec
}

// compileProto compiles .proto sources independently of the code under test,
// so a decoding bug cannot be hidden by an encoder sharing the same mistake.
func (s *SerdeSuite) compileProto(files map[string]string, main string, message string) protoreflect.MessageDescriptor {
	s.T().Helper()

	compiler := protocompile.Compiler{Resolver: protocompile.WithStandardImports(&protocompile.SourceResolver{
		Accessor: func(path string) (io.ReadCloser, error) {
			source, ok := files[path]
			if !ok {
				return nil, fmt.Errorf("%s: %w", path, os.ErrNotExist)
			}

			return io.NopCloser(strings.NewReader(source)), nil
		},
	})}

	compiled, err := compiler.Compile(context.Background(), main)
	s.Require().NoError(err, "the fixture proto must compile")

	descriptor := compiled[0].FindDescriptorByName(protoreflect.FullName(message))
	s.Require().NotNil(descriptor, "the fixture message must exist")

	return descriptor.(protoreflect.MessageDescriptor)
}

func (s *SerdeSuite) protoBytes(md protoreflect.MessageDescriptor, document string) []byte {
	s.T().Helper()

	message := dynamicpb.NewMessage(md)
	s.Require().NoError(protojson.Unmarshal([]byte(document), message), "the fixture document must fit the message")

	data, err := proto.Marshal(message)
	s.Require().NoError(err, "the fixture message must marshal")

	return data
}

func (s *SerdeSuite) registerProto() (int, int) {
	s.T().Helper()

	commonSubject := s.env.UniqueName("common") + ".proto"
	commonID := s.env.RegisterSchema(s.T(), commonSubject, sr.Schema{Schema: commonProto, Type: sr.TypeProtobuf})

	orderID := s.env.RegisterSchema(s.T(), s.env.UniqueName("orders")+"-value", sr.Schema{
		Schema: orderProto,
		Type:   sr.TypeProtobuf,
		References: []sr.SchemaReference{
			{Name: "common.proto", Subject: commonSubject, Version: 1},
		},
	})

	return commonID, orderID
}

func (s *SerdeSuite) object(text string) map[string]any {
	s.T().Helper()

	var document map[string]any
	s.Require().NoError(json.Unmarshal([]byte(text), &document), "decoded output must be valid JSON, got %s", text)

	return document
}

func (s *SerdeSuite) TestDecodesPlainMessages() {
	codec := s.withoutRegistry()

	s.Run("a JSON object is reported as json and kept verbatim", func() {
		decoded := codec.Decode(s.T().Context(), "t", serde.Value, []byte(`{"b":1, "a":2}`))
		s.Require().Equal(serde.FormatJSON, decoded.Format, "plain JSON must be recognised so callers know fields can be addressed")
		s.Require().Equal(`{"b":1, "a":2}`, decoded.Text,
			"plain JSON must be shown exactly as stored, since it was not re-encoded and reformatting it would misrepresent the bytes")
	})

	s.Run("text is reported as text", func() {
		decoded := codec.Decode(s.T().Context(), "t", serde.Value, []byte("order 42 shipped"))
		s.Require().Equal(serde.FormatText, decoded.Format, "valid UTF-8 that is not a JSON document is text")
		s.Require().Equal("order 42 shipped", decoded.Text, "text must be returned unchanged")
	})

	s.Run("binary is reported as base64", func() {
		decoded := codec.Decode(s.T().Context(), "t", serde.Value, []byte{0xff, 0xfe, 0x01})
		s.Require().Equal(serde.FormatBinary, decoded.Format, "bytes that are not UTF-8 cannot be shown as text")
		s.Require().Equal("//4B", decoded.Text, "binary must be base64 so the exact bytes can be recovered")
	})

	s.Run("a missing value is null", func() {
		decoded := codec.Decode(s.T().Context(), "t", serde.Value, nil)
		s.Require().Equal(serde.FormatNull, decoded.Format,
			"a tombstone is a different message from an empty string, and compaction treats it differently")
	})

	s.Run("a registry-framed message without a registry says so", func() {
		decoded := codec.Decode(s.T().Context(), "t", serde.Value, []byte{0, 0, 0, 0, 7, 0x02, 0x41})
		s.Require().Equal(serde.FormatBinary, decoded.Format, "without a registry the bytes cannot be decoded")
		s.Require().Contains(decoded.Error, "schema id 7",
			"the caller must learn why the message is opaque and which schema it names, rather than seeing anonymous bytes")
		s.Require().Contains(decoded.Error, "schema_registry",
			"the message must point at the configuration that would fix it")
	})
}

func (s *SerdeSuite) TestDecodesAvroFromTheRegistry() {
	codec := s.codec(nil)
	id := s.env.RegisterSchema(s.T(), s.env.UniqueName("orders")+"-value", sr.Schema{Schema: orderAvro})

	payload, err := avro.MustParse(orderAvro).Encode(map[string]any{"id": "o-1", "amount": int64(9007199254740993), "note": "rush"})
	s.Require().NoError(err, "the fixture record must encode")

	s.Run("the record is decoded to JSON", func() {
		decoded := codec.Decode(s.T().Context(), "orders", serde.Value, []byte(testenv.WireFormat(s.T(), id, nil, payload)))
		s.Require().Empty(decoded.Error, "a registered Avro message must decode cleanly")
		s.Require().Equal(serde.FormatAvro, decoded.Format, "the format must name what the bytes really are")
		s.Require().Equal(id, decoded.SchemaID, "the schema id is how the caller fetches the schema for a follow-up write")
		s.Require().Equal("shop.Order", decoded.MessageType, "the record name tells the caller which type the topic carries")
		s.Require().Equal(`{"amount":9007199254740993,"id":"o-1","note":"rush"}`, decoded.Text,
			"the JSON must have sorted keys, so two reads of one message never differ, and a long beyond 2^53 must not lose precision")
	})

	s.Run("an unknown schema id is reported, not hidden", func() {
		decoded := codec.Decode(s.T().Context(), "orders", serde.Value, []byte(testenv.WireFormat(s.T(), 999999, nil, payload)))
		s.Require().Equal(serde.FormatBinary, decoded.Format, "a message that cannot be decoded must still be shown")
		s.Require().Contains(decoded.Error, "999999", "the error must name the schema id that could not be found")
	})

	s.Run("bytes that do not fit the schema are reported", func() {
		decoded := codec.Decode(s.T().Context(), "orders", serde.Value, []byte(testenv.WireFormat(s.T(), id, nil, []byte{0x02})))
		s.Require().NotEmpty(decoded.Error, "a payload truncated mid-record must not decode as if it were whole")
	})
}

func (s *SerdeSuite) TestDecodesProtobufFromTheRegistry() {
	codec := s.codec(nil)
	_, orderID := s.registerProto()

	files := map[string]string{"common.proto": commonProto, "order.proto": orderProto}

	s.Run("a top-level message with a reference decodes", func() {
		md := s.compileProto(files, "order.proto", "shop.Order")
		payload := s.protoBytes(md, `{"id":"o-7","price":{"currency":"EUR","units":"12"},"status":"PAID"}`)

		// Order is the second message in the file, so its index is [1].
		decoded := codec.Decode(s.T().Context(), "orders", serde.Value, []byte(testenv.WireFormat(s.T(), orderID, []int{1}, payload)))
		s.Require().Empty(decoded.Error, "a registered protobuf message with a referenced import must decode")
		s.Require().Equal(serde.FormatProtobuf, decoded.Format, "the format must say protobuf")
		s.Require().Equal("shop.Order", decoded.MessageType,
			"the message index must select Order, not the first message in the file")

		document := s.object(decoded.Text)
		s.Require().Equal("o-7", document["id"], "field values must survive decoding")
		s.Require().Equal("PAID", document["status"], "enums must be shown by name, which is what a person searches for")
		s.Require().Equal(map[string]any{"currency": "EUR", "units": "12"}, document["price"],
			"a field whose type lives in a referenced schema must decode")
	})

	s.Run("the first message uses the short index", func() {
		md := s.compileProto(files, "order.proto", "shop.Header")
		payload := s.protoBytes(md, `{"source":"web"}`)

		decoded := codec.Decode(s.T().Context(), "orders", serde.Value, []byte(testenv.WireFormat(s.T(), orderID, []int{0}, payload)))
		s.Require().Empty(decoded.Error, "the single zero byte serializers write for the first message must be understood")
		s.Require().Equal("shop.Header", decoded.MessageType, "index [0] is the first message in the file")
	})

	s.Run("a nested message decodes", func() {
		md := s.compileProto(files, "order.proto", "shop.Order.Line")
		payload := s.protoBytes(md, `{"sku":"A-1","qty":3}`)

		decoded := codec.Decode(s.T().Context(), "orders", serde.Value, []byte(testenv.WireFormat(s.T(), orderID, []int{1, 0}, payload)))
		s.Require().Empty(decoded.Error, "a nested message index must be followed into the nested type")
		s.Require().Equal("shop.Order.Line", decoded.MessageType, "index [1, 0] is the first message nested in Order")
		s.Require().EqualValues(3, s.object(decoded.Text)["qty"], "nested message fields must decode")
	})

	s.Run("unset proto3 fields are shown with their defaults", func() {
		md := s.compileProto(files, "order.proto", "shop.Order")
		payload := s.protoBytes(md, `{"id":"o-8"}`)

		decoded := codec.Decode(s.T().Context(), "orders", serde.Value, []byte(testenv.WireFormat(s.T(), orderID, []int{1}, payload)))
		s.Require().Equal("NEW", s.object(decoded.Text)["status"],
			"proto3 cannot tell an unset field from its default, so a search for status NEW must see it")
	})
}

func (s *SerdeSuite) TestDecodesJSONSchemaFromTheRegistry() {
	codec := s.codec(nil)
	id := s.env.RegisterSchema(s.T(), s.env.UniqueName("orders")+"-value", sr.Schema{Schema: orderJSONSchema, Type: sr.TypeJSON})

	decoded := codec.Decode(s.T().Context(), "orders", serde.Value,
		[]byte(testenv.WireFormat(s.T(), id, nil, []byte(`{"id":"o-1","amount":5}`))))

	s.Require().Empty(decoded.Error, "a JSON Schema framed message must decode")
	s.Require().Equal(serde.FormatJSONSchema, decoded.Format, "the format must say the JSON is schema-registered")
	s.Require().Equal(id, decoded.SchemaID, "the schema id must be reported")
	s.Require().Equal(`{"id":"o-1","amount":5}`, decoded.Text,
		"the framing bytes must be stripped, and JSON that was not re-encoded must be shown exactly as stored")
}

func (s *SerdeSuite) TestDecodesConfiguredTopicFormats() {
	dir := s.T().TempDir()

	avroFile := filepath.Join(dir, "order.avsc")
	s.Require().NoError(os.WriteFile(avroFile, []byte(orderAvro), 0o600), "the avro fixture must be written")

	s.Require().NoError(os.WriteFile(filepath.Join(dir, "common.proto"), []byte(commonProto), 0o600), "the proto fixture must be written")
	s.Require().NoError(os.WriteFile(filepath.Join(dir, "order.proto"), []byte(orderProto), 0o600), "the proto fixture must be written")

	codec := s.codec(map[string]*config.TopicFormat{
		"avro-*":  {Value: &config.PartFormat{Format: config.FormatAvro, SchemaFile: avroFile}},
		"proto-*": {Value: &config.PartFormat{Format: config.FormatProtobuf, ProtoFiles: []string{"order.proto"}, ImportPaths: []string{dir}, MessageType: "shop.Order"}},
		"metrics": {Key: &config.PartFormat{Format: config.FormatText}, Value: &config.PartFormat{Format: config.FormatMsgpack}},
	})

	s.Run("avro from a schema file", func() {
		payload, err := avro.MustParse(orderAvro).Encode(map[string]any{"id": "o-1", "amount": int64(3)})
		s.Require().NoError(err, "the fixture must encode")

		decoded := codec.Decode(s.T().Context(), "avro-orders", serde.Value, payload)
		s.Require().Empty(decoded.Error, "raw avro must decode with the configured schema")
		s.Require().Equal(serde.FormatAvro, decoded.Format, "the configured format must be reported")
		s.Require().Zero(decoded.SchemaID, "a schema file has no registry id, and inventing one would mislead")
		s.Require().Equal(`{"amount":3,"id":"o-1","note":null}`, decoded.Text, "the record must decode to JSON")
	})

	s.Run("protobuf from proto files", func() {
		md := s.compileProto(map[string]string{"common.proto": commonProto, "order.proto": orderProto}, "order.proto", "shop.Order")

		decoded := codec.Decode(s.T().Context(), "proto-orders", serde.Value, s.protoBytes(md, `{"id":"o-2","status":"PAID"}`))
		s.Require().Empty(decoded.Error, "raw protobuf must decode with the configured message type")
		s.Require().Equal("shop.Order", decoded.MessageType, "the configured message type must be reported")
		s.Require().Equal("PAID", s.object(decoded.Text)["status"], "fields must decode")
	})

	s.Run("msgpack", func() {
		payload, err := msgpack.Marshal(map[string]any{"cpu": 0.5, "host": "a", "tags": []string{"x"}})
		s.Require().NoError(err, "the fixture must encode")

		decoded := codec.Decode(s.T().Context(), "metrics", serde.Value, payload)
		s.Require().Empty(decoded.Error, "msgpack must decode")
		s.Require().Equal(serde.FormatMsgpack, decoded.Format, "the configured format must be reported")
		s.Require().Equal(`{"cpu":0.5,"host":"a","tags":["x"]}`, decoded.Text, "msgpack must be shown as sorted JSON")
	})

	s.Run("a configured key format applies to the key only", func() {
		decoded := codec.Decode(s.T().Context(), "metrics", serde.Key, []byte("host-a"))
		s.Require().Equal(serde.FormatText, decoded.Format, "the key has its own configured format")
	})

	s.Run("a topic matching no pattern is detected per message", func() {
		decoded := codec.Decode(s.T().Context(), "other", serde.Value, []byte(`{"a":1}`))
		s.Require().Equal(serde.FormatJSON, decoded.Format, "a topic without a configured format must fall back to detection")
	})

	s.Run("bytes that do not fit the configured format are reported", func() {
		decoded := codec.Decode(s.T().Context(), "metrics", serde.Value, []byte{0xc1})
		s.Require().NotEmpty(decoded.Error, "a message that is not valid msgpack must say so rather than decode as nothing")
		s.Require().Equal(serde.FormatBinary, decoded.Format, "the undecodable bytes must still be shown")
	})
}

func (s *SerdeSuite) TestRefusesInvalidTopicFormatsAtStartup() {
	s.Run("a missing schema file", func() {
		_, err := serde.New(&config.Cluster{Name: "test", TopicFormats: map[string]*config.TopicFormat{
			"t": {Value: &config.PartFormat{Format: config.FormatAvro, SchemaFile: "/nonexistent/x.avsc"}},
		}})
		s.Require().ErrorContains(err, "x.avsc",
			"a schema that cannot be loaded must stop the server, rather than fail on every message later")
	})

	s.Run("a message type the proto files do not define", func() {
		dir := s.T().TempDir()
		s.Require().NoError(os.WriteFile(filepath.Join(dir, "common.proto"), []byte(commonProto), 0o600), "fixture")

		_, err := serde.New(&config.Cluster{Name: "test", TopicFormats: map[string]*config.TopicFormat{
			"t": {Value: &config.PartFormat{Format: config.FormatProtobuf, ProtoFiles: []string{"common.proto"}, ImportPaths: []string{dir}, MessageType: "common.Nope"}},
		}})
		s.Require().ErrorContains(err, "common.Nope", "a type that does not exist can never decode anything")
	})
}

func (s *SerdeSuite) TestEncodesAgainstTheRegistry() {
	codec := s.codec(nil)

	s.Run("avro round-trips through the registry", func() {
		subject := s.env.UniqueName("enc-avro") + "-value"
		id := s.env.RegisterSchema(s.T(), subject, sr.Schema{Schema: orderAvro})

		data, encoded, err := codec.Encode(s.T().Context(), "t", serde.Value,
			`{"id":"o-1","amount":12}`, serde.Target{Subject: subject})
		s.Require().NoError(err, "a document that fits the schema must encode")
		s.Require().Equal(id, encoded.SchemaID, "the latest version's id must be used when none is named")
		s.Require().Equal(subject, encoded.Subject, "the subject used must be reported")

		decoded := codec.Decode(s.T().Context(), "t", serde.Value, data)
		s.Require().Equal(`{"amount":12,"id":"o-1","note":null}`, decoded.Text,
			"what was written must read back as the same record, with the default filled in")
	})

	s.Run("avro rejects a document that does not fit", func() {
		subject := s.env.UniqueName("enc-avro-bad") + "-value"
		s.env.RegisterSchema(s.T(), subject, sr.Schema{Schema: orderAvro})

		_, _, err := codec.Encode(s.T().Context(), "t", serde.Value, `{"id":"o-1","amount":"many"}`, serde.Target{Subject: subject})
		s.Require().ErrorContains(err, "amount",
			"the error must name the field, or the caller cannot fix the document")
	})

	s.Run("avro rejects a field the schema does not have", func() {
		subject := s.env.UniqueName("enc-avro-unknown") + "-value"
		s.env.RegisterSchema(s.T(), subject, sr.Schema{Schema: orderAvro})

		_, _, err := codec.Encode(s.T().Context(), "t", serde.Value, `{"id":"o-1","amount":1,"nots":"rush"}`, serde.Target{Subject: subject})
		s.Require().ErrorContains(err, "nots",
			"avro's JSON reader drops unknown fields, so a misspelt note would be written as its default without this check")
	})

	s.Run("avro accepts a tagged union branch", func() {
		subject := s.env.UniqueName("enc-avro-union") + "-value"
		s.env.RegisterSchema(s.T(), subject, sr.Schema{Schema: orderAvro})

		data, _, err := codec.Encode(s.T().Context(), "t", serde.Value, `{"id":"o-1","amount":1,"note":{"string":"rush"}}`, serde.Target{Subject: subject})
		s.Require().NoError(err, "the standard Avro JSON form wraps a union value in its branch name, and it must be accepted")
		s.Require().Equal(`{"amount":1,"id":"o-1","note":"rush"}`, codec.Decode(s.T().Context(), "t", serde.Value, data).Text,
			"the tagged value must be written into the union")
	})

	s.Run("the subject defaults to the topic name strategy", func() {
		topic := s.env.UniqueName("enc-default")
		id := s.env.RegisterSchema(s.T(), topic+"-value", sr.Schema{Schema: orderAvro})

		_, encoded, err := codec.Encode(s.T().Context(), topic, serde.Value, `{"id":"o","amount":1}`, serde.Target{})
		s.Require().NoError(err, "an empty target must resolve to <topic>-value")
		s.Require().Equal(id, encoded.SchemaID, "the topic's own subject must be used")
	})

	s.Run("protobuf picks the named message and writes its index", func() {
		_, orderID := s.registerProto()

		data, encoded, err := codec.Encode(s.T().Context(), "t", serde.Value,
			`{"id":"o-9","status":"PAID"}`, serde.Target{ID: orderID, MessageType: "shop.Order"})
		s.Require().NoError(err, "a document that fits the message must encode")
		s.Require().Equal("shop.Order", encoded.MessageType, "the message used must be reported")

		decoded := codec.Decode(s.T().Context(), "t", serde.Value, data)
		s.Require().Equal("shop.Order", decoded.MessageType, "the written index must point a consumer at Order")
		s.Require().Equal("PAID", s.object(decoded.Text)["status"], "the fields must read back")
	})

	s.Run("protobuf rejects an unknown field", func() {
		_, orderID := s.registerProto()

		_, _, err := codec.Encode(s.T().Context(), "t", serde.Value, `{"idd":"x"}`, serde.Target{ID: orderID, MessageType: "shop.Order"})
		s.Require().ErrorContains(err, "idd", "a misspelt field would otherwise be dropped silently")
	})

	s.Run("json schema validates before writing", func() {
		subject := s.env.UniqueName("enc-json") + "-value"
		s.env.RegisterSchema(s.T(), subject, sr.Schema{Schema: orderJSONSchema, Type: sr.TypeJSON})

		_, _, err := codec.Encode(s.T().Context(), "t", serde.Value, `{"id":"o-1"}`, serde.Target{Subject: subject})
		s.Require().ErrorContains(err, "amount", "a document missing a required field must be refused before it reaches a consumer")

		data, _, err := codec.Encode(s.T().Context(), "t", serde.Value, `{"id":"o-1","amount":2}`, serde.Target{Subject: subject})
		s.Require().NoError(err, "a valid document must encode")
		s.Require().Equal(serde.FormatJSONSchema, codec.Decode(s.T().Context(), "t", serde.Value, data).Format,
			"the written bytes must carry the registry framing")
	})

	s.Run("a missing subject is reported", func() {
		_, _, err := codec.Encode(s.T().Context(), "t", serde.Value, `{}`, serde.Target{Subject: s.env.UniqueName("absent")})
		s.Require().ErrorContains(err, "absent", "the error must name the subject that was looked for")
	})

	s.Run("encoding without a registry is refused", func() {
		_, _, err := s.withoutRegistry().Encode(s.T().Context(), "t", serde.Value, `{}`, serde.Target{Subject: "x"})
		s.Require().ErrorContains(err, "schema_registry", "the error must point at the missing configuration")
	})
}

func (s *SerdeSuite) TestEncodesConfiguredTopicFormats() {
	dir := s.T().TempDir()
	avroFile := filepath.Join(dir, "order.avsc")
	s.Require().NoError(os.WriteFile(avroFile, []byte(orderAvro), 0o600), "the avro fixture must be written")

	codec := s.codec(map[string]*config.TopicFormat{
		"avro-t":  {Value: &config.PartFormat{Format: config.FormatAvro, SchemaFile: avroFile}},
		"metrics": {Value: &config.PartFormat{Format: config.FormatMsgpack}},
	})

	s.Run("a topic with a configured format reports it", func() {
		data, encoded, ok, err := codec.EncodeConfigured("avro-t", serde.Value, `{"id":"o","amount":4}`)
		s.Require().True(ok, "the topic has a configured value format")
		s.Require().NoError(err, "a fitting document must encode")
		s.Require().Equal(serde.FormatAvro, encoded.Format, "the format used must be reported")
		s.Require().Equal(`{"amount":4,"id":"o","note":null}`, codec.Decode(s.T().Context(), "avro-t", serde.Value, data).Text,
			"the raw avro must read back")
	})

	s.Run("msgpack keeps integers as integers", func() {
		data, _, ok, err := codec.EncodeConfigured("metrics", serde.Value, `{"count":3,"ratio":0.5}`)
		s.Require().True(ok, "the topic has a configured value format")
		s.Require().NoError(err, "a JSON document must encode as msgpack")

		var back map[string]any
		s.Require().NoError(msgpack.Unmarshal(data, &back), "the bytes must be real msgpack")
		s.Require().EqualValues(3, back["count"], "an integer must stay an integer, or a typed consumer rejects it")
	})

	s.Run("a topic without a configured format is not touched", func() {
		_, _, ok, err := codec.EncodeConfigured("plain", serde.Value, `{}`)
		s.Require().NoError(err, "no configured format is not an error")
		s.Require().False(ok, "the caller must know to write the value as given")
	})
}
