package getschema_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/twmb/franz-go/pkg/sr"

	"github.com/denizgursoy/kafka-mcp/internal/domain/config"
	"github.com/denizgursoy/kafka-mcp/internal/domain/serde"
	"github.com/denizgursoy/kafka-mcp/internal/domain/testenv"
	"github.com/denizgursoy/kafka-mcp/internal/tools/getschema"
)

const orderV1 = `{"type":"record","name":"Order","namespace":"shop","fields":[{"name":"id","type":"string"}]}`

const orderV2 = `{"type":"record","name":"Order","namespace":"shop","fields":[
	{"name":"id","type":"string"},{"name":"note","type":["null","string"],"default":null}]}`

const commonProto = `syntax = "proto3";
package common;
message Money { string currency = 1; int64 units = 2; }`

const orderProto = `syntax = "proto3";
package shop;
import "common.proto";
message Order { string id = 1; common.Money price = 2; }`

type GetSchemaSuite struct {
	suite.Suite

	env *testenv.Environment
}

func TestGetSchemaSuite(t *testing.T) {
	suite.Run(t, new(GetSchemaSuite))
}

func (s *GetSchemaSuite) SetupSuite() {
	s.env = testenv.Start(s.T())
}

func (s *GetSchemaSuite) TearDownSuite() {
	s.env.Stop()
}

func (s *GetSchemaSuite) codec() *serde.Codec {
	s.T().Helper()

	codec, err := serde.New(&config.Cluster{
		Name:           "test",
		SchemaRegistry: &config.SchemaRegistry{URLs: []string{s.env.SchemaRegistry()}},
	})
	s.Require().NoError(err, "a codec for the test registry must build")

	return codec
}

func (s *GetSchemaSuite) get(item getschema.Item) (getschema.Output, error) {
	s.T().Helper()

	out, err := getschema.Run(s.T().Context(), s.codec(), getschema.Input{Items: []getschema.Item{item}})
	if err != nil {
		return getschema.Output{}, err
	}

	s.Require().Len(out.Results, 1,
		"one item in must produce exactly one result out, or results cannot be matched to inputs by position")

	if out.Results[0].Error != "" {
		return getschema.Output{}, errors.New(out.Results[0].Error)
	}

	return *out.Results[0].Result, nil
}

func (s *GetSchemaSuite) TestReturnsTheLatestVersionOfASubject() {
	subject := s.env.UniqueName("orders") + "-value"
	s.env.RegisterSchema(s.T(), subject, sr.Schema{Schema: orderV1})
	id := s.env.RegisterSchema(s.T(), subject, sr.Schema{Schema: orderV2})

	out, err := s.get(getschema.Item{Subject: subject})

	s.Require().NoError(err, "an existing subject must be returned")
	s.Require().Equal(2, out.Version, "an omitted version means the latest, which is what a new message must be written with")
	s.Require().Equal(id, out.SchemaID, "the id is what a written message carries")
	s.Require().Equal("avro", out.Type, "the type decides how a value must be written")
	s.Require().Equal(subject, out.Subject, "the subject must be echoed so a batch result can be matched")
	s.Require().JSONEq(orderV2, out.Schema, "the schema text is what tells the caller which fields a value needs")
	s.Require().Equal([]int{1, 2}, out.Versions,
		"every version must be listed, so the caller can tell a stale message from a current one")
}

func (s *GetSchemaSuite) TestReturnsAnExactVersion() {
	subject := s.env.UniqueName("orders") + "-value"
	firstID := s.env.RegisterSchema(s.T(), subject, sr.Schema{Schema: orderV1})
	s.env.RegisterSchema(s.T(), subject, sr.Schema{Schema: orderV2})

	out, err := s.get(getschema.Item{Subject: subject, Version: 1})

	s.Require().NoError(err, "an existing version must be returned")
	s.Require().Equal(firstID, out.SchemaID, "version 1 must be returned, not the latest")
	s.Require().JSONEq(orderV1, out.Schema, "the text must be version 1's")
}

func (s *GetSchemaSuite) TestReturnsASchemaByID() {
	subject := s.env.UniqueName("orders") + "-value"
	id := s.env.RegisterSchema(s.T(), subject, sr.Schema{Schema: orderV1})

	out, err := s.get(getschema.Item{ID: id})

	s.Require().NoError(err, "a schema id that exists must be returned")
	s.Require().JSONEq(orderV1, out.Schema, "the schema text must be the one the id names")
	s.Require().Contains(out.UsedBy, getschema.SubjectVersion{Subject: subject, Version: 1},
		"looking up the id a message carries must say which subject it belongs to, which is how the caller finds the topic's current version")
}

func (s *GetSchemaSuite) TestReportsReferences() {
	commonSubject := s.env.UniqueName("common") + ".proto"
	s.env.RegisterSchema(s.T(), commonSubject, sr.Schema{Schema: commonProto, Type: sr.TypeProtobuf})

	subject := s.env.UniqueName("orders") + "-value"
	s.env.RegisterSchema(s.T(), subject, sr.Schema{
		Schema:     orderProto,
		Type:       sr.TypeProtobuf,
		References: []sr.SchemaReference{{Name: "common.proto", Subject: commonSubject, Version: 1}},
	})

	out, err := s.get(getschema.Item{Subject: subject})

	s.Require().NoError(err, "a schema with references must be returned")
	s.Require().Equal("protobuf", out.Type, "the type must say protobuf")
	s.Require().Equal([]getschema.Reference{{Name: "common.proto", Subject: commonSubject, Version: 1}}, out.References,
		"references must be listed, because the schema alone does not define the referenced types")
	s.Require().Equal([]string{"shop.Order"}, out.MessageTypes,
		"protobuf message names are what produce_message takes as message_type")
}

func (s *GetSchemaSuite) TestBatchKeepsPartialErrors() {
	subject := s.env.UniqueName("orders") + "-value"
	s.env.RegisterSchema(s.T(), subject, sr.Schema{Schema: orderV1})

	out, err := getschema.Run(s.T().Context(), s.codec(), getschema.Input{Items: []getschema.Item{
		{Subject: subject},
		{Subject: s.env.UniqueName("missing") + "-value"},
		{ID: 99999999},
	}})

	s.Require().NoError(err, "a missing subject must be reported on its item, not fail the call")
	s.Require().Len(out.Results, 3, "every item must have one result in input order")
	s.Require().NotNil(out.Results[0].Result, "the existing subject must still be returned")
	s.Require().Contains(out.Results[1].Error, "missing", "the error must name the subject that was not found")
	s.Require().Contains(out.Results[2].Error, "99999999", "the error must name the id that was not found")
}

func (s *GetSchemaSuite) TestRejectsAnItemNamingNothing() {
	_, err := s.get(getschema.Item{})

	s.Require().ErrorContains(err, "subject", "an item must name a subject or an id, and the error must say so")
}

func (s *GetSchemaSuite) TestRejectsAnItemNamingBoth() {
	_, err := s.get(getschema.Item{Subject: "a-value", ID: 1})

	s.Require().ErrorContains(err, "either",
		"a subject and an id may disagree, and silently preferring one would return a schema the caller did not ask for")
}

func (s *GetSchemaSuite) TestFailsWithoutARegistry() {
	codec, err := serde.New(&config.Cluster{Name: "test"})
	s.Require().NoError(err, "a codec without a registry must still build")

	_, err = getschema.Run(s.T().Context(), codec, getschema.Input{Items: []getschema.Item{{Subject: "a-value"}}})

	s.Require().ErrorContains(err, "schema_registry",
		"a cluster with no registry has no schemas, and the error must point at the configuration rather than look like a missing subject")
}
