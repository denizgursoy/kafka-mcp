package samplemessages_test

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/denizgursoy/kafka-mcp/internal/domain/records"
	"github.com/denizgursoy/kafka-mcp/internal/domain/testenv"
	"github.com/denizgursoy/kafka-mcp/internal/tools/samplemessages"
)

type SampleMessagesSuite struct {
	suite.Suite

	env *testenv.Environment
}

func TestSampleMessagesSuite(t *testing.T) {
	suite.Run(t, new(SampleMessagesSuite))
}

func (s *SampleMessagesSuite) SetupSuite() {
	s.env = testenv.Start(s.T())
}

func (s *SampleMessagesSuite) TearDownSuite() {
	s.env.Stop()
}

func (s *SampleMessagesSuite) TestReportsJSONFieldsAndTypes() {
	topic := s.env.CreateTopic(s.T(), "sample-fields")

	s.env.Produce(s.T(), topic,
		testenv.Message{
			Key:   "order-1",
			Value: `{"eventType":"NEW","payload":{"amount":500,"verified":true,"cancelledAt":null}}`,
		},
		testenv.Message{
			Key:   "order-2",
			Value: `{"eventType":"PAID","payload":{"amount":900,"verified":false,"cancelledAt":null}}`,
		},
	)

	out, err := samplemessages.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		samplemessages.Input{Topic: topic},
	)

	s.Require().NoError(err, "sampling a topic of JSON messages must succeed")
	s.Require().Equal(2, out.ValueFormats.JSON,
		"both messages are JSON, and the caller decides whether a structured filter is usable from this count")

	paths := make(map[string]samplemessages.Field, len(out.JSONFields))
	for _, field := range out.JSONFields {
		paths[field.Path] = field
	}

	s.Require().Contains(paths, "payload.amount",
		"a nested field must be reported by its full dotted path, because that is what a filter needs")
	s.Require().Equal([]string{"number"}, paths["payload.amount"].Types,
		"the reported type decides whether a numeric comparison is valid in a filter")
	s.Require().Equal(2, paths["payload.amount"].Present,
		"the occurrence count tells the caller whether a field is reliable enough to filter on")
	s.Require().Equal([]string{"boolean"}, paths["payload.verified"].Types,
		"a boolean field must be reported as such, so is_true is chosen over eq")
	s.Require().Equal([]string{"null"}, paths["payload.cancelledAt"].Types,
		"a field seen only as null must be reported as null, which is what makes is_null the right operator")

	fieldPaths := make([]string, 0, len(out.JSONFields))
	for _, field := range out.JSONFields {
		fieldPaths = append(fieldPaths, field.Path)
	}

	s.Require().IsIncreasing(fieldPaths,
		"fields must be sorted, because they are gathered from maps whose order is random in Go")
}

func (s *SampleMessagesSuite) TestDetectsKeyInsideValue() {
	topic := s.env.CreateTopic(s.T(), "sample-key-in-value")

	s.env.Produce(s.T(), topic,
		testenv.Message{
			Key:   "order-111",
			Value: `{"payload":{"orderId":"order-111","customer":"alice"}}`,
		},
		testenv.Message{
			Key:   "order-222",
			Value: `{"payload":{"orderId":"order-222","customer":"bob"}}`,
		},
	)

	out, err := samplemessages.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		samplemessages.Input{Topic: topic},
	)

	s.Require().NoError(err, "sampling must succeed")
	s.Require().Equal([]string{"payload.orderId"}, out.KeyInValue,
		"the value path carrying the key must be reported, because that is what proves the key identifies the message")
	s.Require().True(out.KeyStats.AllUnique,
		"unique keys mean a key search targets one message, which is the cheapest and most precise lookup")
	s.Require().Equal(2, out.KeyStats.Present,
		"every sampled message has a key, so searching by key alone is viable")
	s.Require().Zero(out.KeyStats.Absent,
		"no message lacks a key here, and the caller relies on that before recommending a key search")
}

func (s *SampleMessagesSuite) TestReportsAbsentKeys() {
	topic := s.env.CreateTopic(s.T(), "sample-no-keys")

	s.env.Produce(s.T(), topic,
		testenv.Message{Value: `{"a":1}`},
		testenv.Message{Value: `{"a":2}`},
	)

	out, err := samplemessages.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		samplemessages.Input{Topic: topic},
	)

	s.Require().NoError(err, "sampling a keyless topic must succeed")
	s.Require().Equal(2, out.KeyStats.Absent,
		"a topic without keys must say so, so the caller does not propose a key search that cannot work")
	s.Require().Empty(out.KeyInValue,
		"with no keys there can be no value path matching one")
}

func (s *SampleMessagesSuite) TestReportsNonJSONFormats() {
	topic := s.env.CreateTopic(s.T(), "sample-text")

	s.env.Produce(s.T(), topic,
		testenv.Message{Value: "plain log line one"},
		testenv.Message{Value: "plain log line two"},
	)

	out, err := samplemessages.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		samplemessages.Input{Topic: topic},
	)

	s.Require().NoError(err, "sampling a plain-text topic must succeed")
	s.Require().Equal(2, out.ValueFormats.Text,
		"text messages must be counted as text, so the caller uses a query instead of a structured filter")
	s.Require().Zero(out.ValueFormats.JSON,
		"no message here is JSON, and claiming otherwise would send the caller down the filter path")
	s.Require().Empty(out.JSONFields,
		"a topic with no JSON has no field paths to offer")
}

func (s *SampleMessagesSuite) TestSamplesTheNewestMessages() {
	topic := s.env.CreateTopic(s.T(), "sample-newest")

	messages := make([]testenv.Message, 0, 30)
	for i := 0; i < 30; i++ {
		messages = append(messages, testenv.Message{
			Key:   "k",
			Value: `{"n":` + itoa(i) + `}`,
		})
	}

	s.env.Produce(s.T(), topic, messages...)

	out, err := samplemessages.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		samplemessages.Input{Topic: topic, SampleSize: 5},
	)

	s.Require().NoError(err, "sampling with a size limit must succeed")
	s.Require().Len(out.Messages, 5,
		"the sample must honour the requested size rather than reading the whole topic")
	s.Require().Len(out.SampledRanges, 1,
		"the range actually sampled must be reported for the one partition")
	s.Require().EqualValues(30, out.SampledRanges[0].End,
		"sampling must cover the newest messages, so the range must end at the end of the partition")
	s.Require().EqualValues(25, out.SampledRanges[0].Start,
		"the reported range must show exactly which offsets the shape was inferred from, since older messages may differ")

	for _, message := range out.Messages {
		s.Require().GreaterOrEqual(message.Offset, int64(25),
			"only the newest messages may be sampled when the caller asked for the newest")
	}
}

func (s *SampleMessagesSuite) TestSamplesEveryPartition() {
	topic := s.env.CreateTopicWithPartitions(s.T(), "sample-partitions", 3)

	s.env.Produce(s.T(), topic,
		testenv.Message{Value: `{"p":0}`, Partition: 0},
		testenv.Message{Value: `{"p":1}`, Partition: 1},
		testenv.Message{Value: `{"p":2}`, Partition: 2},
	)

	out, err := samplemessages.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		samplemessages.Input{Topic: topic},
	)

	s.Require().NoError(err, "sampling a multi-partition topic must succeed")
	s.Require().Len(out.Messages, 3,
		"a sample must span every partition, or a format used by only one partition would be missed")

	seen := make(map[int32]bool, 3)
	for _, message := range out.Messages {
		seen[message.Partition] = true
	}

	s.Require().Len(seen, 3,
		"all three partitions must be represented in the sample")
}

func (s *SampleMessagesSuite) TestEmptyTopicSamplesNothing() {
	topic := s.env.CreateTopic(s.T(), "sample-empty")

	out, err := samplemessages.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		samplemessages.Input{Topic: topic},
	)

	s.Require().NoError(err, "an empty topic is a normal case, not an error")
	s.Require().NotNil(out.Messages,
		"messages must be an empty slice, not nil, so the JSON output is [] and not null")
	s.Require().Empty(out.Messages,
		"there is nothing to sample in an empty topic")
	s.Require().Empty(out.JSONFields,
		"no messages means no field paths can be claimed")
}

func (s *SampleMessagesSuite) TestErrorsOnUnknownTopic() {
	_, err := samplemessages.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		samplemessages.Input{Topic: s.env.UniqueName("missing")},
	)

	s.Require().Error(err,
		"sampling a topic that does not exist must fail rather than look like an empty topic")
}

func (s *SampleMessagesSuite) TestErrorsWhenBrokerUnreachable() {
	_, err := samplemessages.Run(
		s.T().Context(),
		s.env.Admin(),
		records.NewReader("127.0.0.1:1"),
		samplemessages.Input{Topic: "anything"},
	)

	s.Require().Error(err,
		"an unreachable broker must surface as an error, not as an empty sample")
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}

	digits := make([]byte, 0, 3)

	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}

	return string(digits)
}
