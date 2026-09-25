package getmessage_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/denizgursoy/kafka-mcp/internal/domain/records"
	"github.com/denizgursoy/kafka-mcp/internal/domain/testenv"
	"github.com/denizgursoy/kafka-mcp/internal/tools/getmessage"
)

type GetMessageSuite struct {
	suite.Suite

	env *testenv.Environment
}

func TestGetMessageSuite(t *testing.T) {
	suite.Run(t, new(GetMessageSuite))
}

func (s *GetMessageSuite) SetupSuite() {
	s.env = testenv.Start(s.T())
}

func (s *GetMessageSuite) TearDownSuite() {
	s.env.Stop()
}

// get reads one address and returns that item's result.
//
// Every call is a batch, so a single address is an items array of length one,
// and a failure for that address arrives as the item's error rather than as an
// error for the call.
func (s *GetMessageSuite) get(item getmessage.Item) (getmessage.Output, error) {
	s.T().Helper()

	return s.getWith(s.env.Reader(), item)
}

func (s *GetMessageSuite) getWith(
	reader *records.Reader,
	item getmessage.Item,
) (getmessage.Output, error) {
	s.T().Helper()

	out, err := getmessage.Run(s.T().Context(), reader, getmessage.Input{
		Items: []getmessage.Item{item},
	})
	if err != nil {
		return getmessage.Output{}, err
	}

	s.Require().Len(out.Results, 1,
		"one item in must produce exactly one result out, or results cannot be matched to inputs by position")

	if out.Results[0].Error != "" {
		return getmessage.Output{}, errors.New(out.Results[0].Error)
	}

	return *out.Results[0].Result, nil
}

func (s *GetMessageSuite) TestReturnsMessageAtOffset() {
	topic := s.env.CreateTopic(s.T(), "get-one")

	s.env.Produce(s.T(), topic,
		testenv.Message{Key: "k0", Value: "first"},
		testenv.Message{
			Key:     "order-42",
			Value:   `{"order":"42"}`,
			Headers: map[string]string{"correlation-id": "abc-123"},
		},
		testenv.Message{Key: "k2", Value: "third"},
	)

	out, err := s.get(getmessage.Item{Topic: topic, Partition: 0, Offset: 1})

	s.Require().NoError(err, "reading an offset that exists must succeed")
	s.Require().EqualValues(1, out.Message.Offset,
		"the returned message must be the offset that was asked for, not a neighbour")
	s.Require().Equal("order-42", out.Message.Key,
		"the key must be returned so the caller can confirm they have the right message")
	s.Require().Equal(`{"order":"42"}`, out.Message.Value,
		"the full value must be returned, because this tool exists to show untruncated content")
	s.Require().Equal("abc-123", out.Message.Headers["correlation-id"],
		"headers must be returned, since correlation ids live there and identify a message")
	s.Require().False(out.Message.Truncated,
		"a short value must not be marked as truncated")
	s.Require().Equal("utf8", out.Message.Encoding,
		"a readable payload must be reported as utf8 so the caller knows the value is literal")
}

func (s *GetMessageSuite) TestReturnsNeighbouringMessages() {
	topic := s.env.CreateTopic(s.T(), "get-context")

	s.env.Produce(s.T(), topic,
		testenv.Message{Value: "zero"},
		testenv.Message{Value: "one"},
		testenv.Message{Value: "two"},
		testenv.Message{Value: "three"},
		testenv.Message{Value: "four"},
	)

	out, err := s.get(getmessage.Item{Topic: topic, Partition: 0, Offset: 2, Context: 1})

	s.Require().NoError(err, "reading with context must succeed")
	s.Require().Equal("two", out.Message.Value,
		"the requested offset must still be the primary message")
	s.Require().Len(out.Before, 1,
		"context of 1 must return exactly one earlier message, to show what preceded the message")
	s.Require().Len(out.After, 1,
		"context of 1 must return exactly one later message, to show what followed the message")
	s.Require().Equal("one", out.Before[0].Value,
		"the earlier message must be the immediately preceding offset")
	s.Require().Equal("three", out.After[0].Value,
		"the later message must be the immediately following offset")
}

func (s *GetMessageSuite) TestContextIsClampedAtPartitionBounds() {
	topic := s.env.CreateTopic(s.T(), "get-bounds")

	s.env.Produce(s.T(), topic,
		testenv.Message{Value: "zero"},
		testenv.Message{Value: "one"},
	)

	out, err := s.get(getmessage.Item{Topic: topic, Partition: 0, Offset: 0, Context: 5})

	s.Require().NoError(err,
		"asking for more context than the partition holds must not fail")
	s.Require().Empty(out.Before,
		"offset 0 has nothing before it, so the tool must return none rather than erroring")
	s.Require().Len(out.After, 1,
		"only one later message exists, so context must be clamped to what the partition holds")
}

func (s *GetMessageSuite) TestTruncatesOversizedValue() {
	topic := s.env.CreateTopic(s.T(), "get-truncate")

	long := make([]byte, 200)
	for i := range long {
		long[i] = 'a'
	}

	s.env.Produce(s.T(), topic, testenv.Message{Value: string(long)})

	out, err := s.get(getmessage.Item{Topic: topic, Partition: 0, Offset: 0, MaxValueBytes: 50})

	s.Require().NoError(err, "reading an oversized value must succeed, not fail")
	s.Require().True(out.Message.Truncated,
		"an oversized value must be flagged, so the caller never mistakes a cut value for the whole one")
	s.Require().Len(out.Message.Value, 50,
		"the value must be cut to the requested limit to keep the response small")
	s.Require().EqualValues(200, out.Message.ValueBytes,
		"the original size must be reported so the caller knows how much was withheld")
}

func (s *GetMessageSuite) TestBase64EncodesBinaryValue() {
	topic := s.env.CreateTopic(s.T(), "get-binary")

	s.env.Produce(s.T(), topic, testenv.Message{Value: string([]byte{0xff, 0xfe, 0x00, 0x01})})

	out, err := s.get(getmessage.Item{Topic: topic, Partition: 0, Offset: 0})

	s.Require().NoError(err, "reading a binary payload must succeed")
	s.Require().Equal("base64", out.Message.Encoding,
		"a non-UTF8 payload must be reported as base64, because invalid UTF-8 cannot survive JSON")
	s.Require().Equal("//4AAQ==", out.Message.Value,
		"the value must be the base64 of the original bytes, so the caller can recover them exactly")
}

func (s *GetMessageSuite) TestErrorsOnOffsetPastEnd() {
	topic := s.env.CreateTopic(s.T(), "get-past-end")

	s.env.Produce(s.T(), topic, testenv.Message{Value: "only"})

	_, err := s.get(getmessage.Item{Topic: topic, Partition: 0, Offset: 99})

	s.Require().Error(err,
		"an offset beyond the end of the partition must fail rather than hang or return nothing")
}

func (s *GetMessageSuite) TestErrorsOnUnknownPartition() {
	topic := s.env.CreateTopic(s.T(), "get-bad-partition")

	s.env.Produce(s.T(), topic, testenv.Message{Value: "only"})

	_, err := s.get(getmessage.Item{Topic: topic, Partition: 7, Offset: 0})

	s.Require().Error(err,
		"a partition the topic does not have must fail, not silently return nothing")
}

func (s *GetMessageSuite) TestErrorsWhenBrokerUnreachable() {
	_, err := s.getWith(
		records.NewReader("127.0.0.1:1"),
		getmessage.Item{Topic: "anything", Partition: 0, Offset: 0},
	)

	s.Require().Error(err,
		"an unreachable broker must surface as that item's error, not as a missing message")
}

func (s *GetMessageSuite) TestBatchReadsSeveralAddressesAndKeepsPartialErrors() {
	topic := s.env.CreateTopic(s.T(), "get-batch")
	s.env.Produce(s.T(), topic,
		testenv.Message{Value: "zero"},
		testenv.Message{Value: "one"},
	)

	out, err := getmessage.Run(s.T().Context(), s.env.Reader(), getmessage.Input{
		Items: []getmessage.Item{
			{Topic: topic, Partition: 0, Offset: 1},
			{Topic: topic, Partition: 0, Offset: 99},
			{Topic: topic, Partition: 0, Offset: 0},
		},
	})

	s.Require().NoError(err, "an invalid address must be reported on its item rather than hide successful reads")
	s.Require().Len(out.Results, 3, "every address must have one result in input order")
	s.Require().Equal("one", out.Results[0].Result.Message.Value, "the first result must match the first address")
	s.Require().NotEmpty(out.Results[1].Error, "the offset past the end must be reported as an item error")
	s.Require().Equal("zero", out.Results[2].Result.Message.Value, "a later valid item must still be read after an earlier failure")
}
