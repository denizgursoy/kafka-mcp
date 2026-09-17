package searchmessages_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/denizgursoy/kafka-mcp/internal/domain/records"
	"github.com/denizgursoy/kafka-mcp/internal/domain/testenv"
	"github.com/denizgursoy/kafka-mcp/internal/tools/searchmessages"
)

type SearchMessagesSuite struct {
	suite.Suite

	env *testenv.Environment
}

func TestSearchMessagesSuite(t *testing.T) {
	suite.Run(t, new(SearchMessagesSuite))
}

func (s *SearchMessagesSuite) SetupSuite() {
	s.env = testenv.Start(s.T())
}

func (s *SearchMessagesSuite) TearDownSuite() {
	s.env.Stop()
}

// hitScript matches the messages the fixtures mark with "hit".
const hitScript = `return value.indexOf("hit") >= 0`

func (s *SearchMessagesSuite) TestFindsMatchInValue() {
	topic := s.env.CreateTopic(s.T(), "search-value")

	s.env.Produce(s.T(), topic,
		testenv.Message{Value: `{"order":"111","state":"NEW"}`},
		testenv.Message{Value: `{"order":"222","state":"PAID"}`},
		testenv.Message{Value: `{"order":"333","state":"NEW"}`},
	)

	out, err := searchmessages.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		"",
		searchmessages.Input{Topic: topic, Script: `return value.order === "222"`},
	)

	s.Require().NoError(err, "searching an existing topic must succeed")
	s.Require().Len(out.Matches, 1,
		"exactly one message contains the query, so exactly one match must be returned")
	s.Require().EqualValues(1, out.Matches[0].Offset,
		"the match must report the offset of the matching message, which is what the caller acts on")
	s.Require().Equal(topic, out.Topic,
		"the result must name the topic that was searched")
	s.Require().EqualValues(3, out.ScannedMessages,
		"all three messages had to be read to be sure only one matched")
	s.Require().Equal("range_exhausted", out.StoppedReason,
		"the whole range was scanned, which is the only reason that makes a no-match conclusive")
}

func (s *SearchMessagesSuite) TestMatchesAreCaseInsensitiveByDefault() {
	topic := s.env.CreateTopic(s.T(), "search-case")

	s.env.Produce(s.T(), topic,
		testenv.Message{Value: "Customer ALICE ordered"},
		testenv.Message{Value: "Customer BOB ordered"},
	)

	out, err := searchmessages.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		"",
		searchmessages.Input{Topic: topic, Script: `return value.toLowerCase().indexOf("alice") >= 0`},
	)

	s.Require().NoError(err, "a case-insensitive search must succeed")
	s.Require().Len(out.Matches, 1,
		"lowercase 'alice' must match 'ALICE', because a user searching for an id should not have to guess its case")
}

func (s *SearchMessagesSuite) TestSearchesKeyByDefault() {
	topic := s.env.CreateTopic(s.T(), "search-key")

	s.env.Produce(s.T(), topic,
		testenv.Message{Key: "customer-777", Value: "no id in the body"},
		testenv.Message{Key: "customer-888", Value: "no id in the body"},
	)

	out, err := searchmessages.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		"",
		searchmessages.Input{Topic: topic, Script: `return key.indexOf("777") >= 0`},
	)

	s.Require().NoError(err, "searching keys must succeed")
	s.Require().Len(out.Matches, 1,
		"the default search covers the key as well as the value, because ids are often only in the key")
	s.Require().Equal("customer-777", out.Matches[0].Key,
		"the matching message must be the one whose key contains the query")
}

func (s *SearchMessagesSuite) TestSearchInHeadersOnly() {
	topic := s.env.CreateTopic(s.T(), "search-headers")

	s.env.Produce(s.T(), topic,
		testenv.Message{
			Value:   "body mentions nothing",
			Headers: map[string]string{"correlation-id": "corr-999"},
		},
		testenv.Message{
			Value:   "corr-999",
			Headers: map[string]string{"correlation-id": "corr-000"},
		},
	)

	out, err := searchmessages.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		"",
		searchmessages.Input{
			Topic:  topic,
			Script: `return headers["correlation-id"] === "corr-999"`,
		},
	)

	s.Require().NoError(err, "searching headers must succeed")
	s.Require().Len(out.Matches, 1,
		"restricting the search to headers must ignore the message whose value alone matches")
	s.Require().EqualValues(0, out.Matches[0].Offset,
		"the match must be the message carrying the header, not the one with the matching body")
}

func (s *SearchMessagesSuite) TestExactMatchDoesNotMatchSubstrings() {
	topic := s.env.CreateTopic(s.T(), "search-exact")

	s.env.Produce(s.T(), topic,
		testenv.Message{Key: "42", Value: "exact"},
		testenv.Message{Key: "4242", Value: "substring"},
	)

	out, err := searchmessages.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		"",
		searchmessages.Input{
			Topic:  topic,
			Script: `return key === "42"`,
		},
	)

	s.Require().NoError(err, "an exact search must succeed")
	s.Require().Len(out.Matches, 1,
		"exact matching must reject '4242', which is why a caller picks exact over contains")
	s.Require().Equal("42", out.Matches[0].Key,
		"only the key equal to the query may match")
}

func (s *SearchMessagesSuite) TestRegexMatch() {
	topic := s.env.CreateTopic(s.T(), "search-regex")

	s.env.Produce(s.T(), topic,
		testenv.Message{Value: "order id ORD-2024-001 accepted"},
		testenv.Message{Value: "order id ORD-9-X rejected"},
	)

	out, err := searchmessages.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		"",
		searchmessages.Input{
			Topic:  topic,
			Script: `return /ORD-\d{4}-\d{3}/.test(value)`,
		},
	)

	s.Require().NoError(err, "a valid regex search must succeed")
	s.Require().Len(out.Matches, 1,
		"only the well-formed order id matches the pattern, which is the point of regex matching")
}

func (s *SearchMessagesSuite) TestMalformedScriptIsRejectedBeforeScanning() {
	topic := s.env.CreateTopic(s.T(), "search-bad-script")

	s.env.Produce(s.T(), topic, testenv.Message{Value: "anything"})

	_, err := searchmessages.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		"",
		searchmessages.Input{Topic: topic, Script: `return value.order ===`},
	)

	s.Require().Error(err,
		"a script that does not compile must be refused before any message is read, so the caller fixes it instead of trusting an empty result")
}

func (s *SearchMessagesSuite) TestNoMatchReturnsEmptyListNotError() {
	topic := s.env.CreateTopic(s.T(), "search-none")

	s.env.Produce(s.T(), topic, testenv.Message{Value: "nothing of interest"})

	out, err := searchmessages.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		"",
		searchmessages.Input{Topic: topic, Script: `return value.indexOf("absent-value") >= 0`},
	)

	s.Require().NoError(err,
		"finding nothing is a valid answer to a search, not a failure")
	s.Require().NotNil(out.Matches,
		"matches must be an empty slice, not nil, so the JSON output is [] and not null")
	s.Require().Empty(out.Matches,
		"no message contains the query, so no match may be reported")
	s.Require().Equal("range_exhausted", out.StoppedReason,
		"the caller can only trust a no-match answer if the whole range was actually scanned")
}

func (s *SearchMessagesSuite) TestStopsAtMaxMatches() {
	topic := s.env.CreateTopic(s.T(), "search-limit")

	s.env.Produce(s.T(), topic,
		testenv.Message{Value: "hit one"},
		testenv.Message{Value: "hit two"},
		testenv.Message{Value: "hit three"},
	)

	out, err := searchmessages.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		"",
		searchmessages.Input{
			Topic:      topic,
			Script:     hitScript,
			MaxMatches: 2,
			Direction:  "oldest_first",
		},
	)

	s.Require().NoError(err, "stopping early at the match limit must succeed")
	s.Require().Len(out.Matches, 2,
		"the search must stop at max_matches instead of returning everything")
	s.Require().Equal("max_matches", out.StoppedReason,
		"the caller must be told the scan stopped early, so a no-match elsewhere is not assumed")
}

func (s *SearchMessagesSuite) TestNewestFirstReturnsMostRecentMatches() {
	topic := s.env.CreateTopic(s.T(), "search-newest")

	s.env.Produce(s.T(), topic,
		testenv.Message{Value: "hit oldest"},
		testenv.Message{Value: "hit middle"},
		testenv.Message{Value: "hit newest"},
	)

	out, err := searchmessages.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		"",
		searchmessages.Input{Topic: topic, Script: hitScript, MaxMatches: 1},
	)

	s.Require().NoError(err, "a newest-first search must succeed")
	s.Require().Len(out.Matches, 1,
		"the match limit of one must be respected")
	s.Require().EqualValues(2, out.Matches[0].Offset,
		"newest_first is the default, so a limited search must return the most recent match, not the oldest")
}

func (s *SearchMessagesSuite) TestRestrictsToRequestedPartitions() {
	topic := s.env.CreateTopicWithPartitions(s.T(), "search-partitions", 3)

	s.env.Produce(s.T(), topic,
		testenv.Message{Value: "hit on p0", Partition: 0},
		testenv.Message{Value: "hit on p2", Partition: 2},
	)

	out, err := searchmessages.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		"",
		searchmessages.Input{
			Topic:      topic,
			Script:     hitScript,
			Partitions: []int32{2},
		},
	)

	s.Require().NoError(err, "searching a single partition must succeed")
	s.Require().Len(out.Matches, 1,
		"restricting to partition 2 must exclude the identical match on partition 0")
	s.Require().EqualValues(2, out.Matches[0].Partition,
		"the only match may come from the requested partition")
	s.Require().EqualValues(1, out.ScannedMessages,
		"only the requested partition may be scanned, which is why narrowing partitions is cheaper")
}

func (s *SearchMessagesSuite) TestRestrictsToOffsetRange() {
	topic := s.env.CreateTopic(s.T(), "search-offsets")

	s.env.Produce(s.T(), topic,
		testenv.Message{Value: "hit zero"},
		testenv.Message{Value: "hit one"},
		testenv.Message{Value: "hit two"},
		testenv.Message{Value: "hit three"},
	)

	from := int64(1)
	to := int64(3)

	out, err := searchmessages.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		"",
		searchmessages.Input{
			Topic:      topic,
			Script:     hitScript,
			FromOffset: &from,
			ToOffset:   &to,
			Direction:  "oldest_first",
		},
	)

	s.Require().NoError(err, "searching an offset window must succeed")
	s.Require().Len(out.Matches, 2,
		"the offset range is half-open, so offsets 1 and 2 match while 0 and 3 are excluded")
	s.Require().EqualValues(1, out.Matches[0].Offset,
		"the first match must be the start of the requested range")
	s.Require().EqualValues(2, out.Matches[1].Offset,
		"the last match must be the offset before the exclusive end of the range")
}

func (s *SearchMessagesSuite) TestRestrictsToTimeRange() {
	topic := s.env.CreateTopic(s.T(), "search-time")

	old := time.Now().Add(-48 * time.Hour).Truncate(time.Millisecond)
	recent := time.Now().Add(-1 * time.Hour).Truncate(time.Millisecond)

	s.env.Produce(s.T(), topic,
		testenv.Message{Value: "hit old", Timestamp: old},
		testenv.Message{Value: "hit recent", Timestamp: recent},
	)

	from := recent.Add(-10 * time.Minute)

	out, err := searchmessages.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		"",
		searchmessages.Input{
			Topic:         topic,
			Script:        hitScript,
			FromTimestamp: &from,
		},
	)

	s.Require().NoError(err, "searching from a timestamp must succeed")
	s.Require().Len(out.Matches, 1,
		"only the recent message falls inside the time window, so the old one must not be scanned or returned")
	s.Require().Equal("hit recent", out.Matches[0].Value,
		"the returned match must be the message inside the window")
}

func (s *SearchMessagesSuite) TestReportsScannedRange() {
	topic := s.env.CreateTopic(s.T(), "search-report")

	s.env.Produce(s.T(), topic,
		testenv.Message{Value: "one"},
		testenv.Message{Value: "two"},
	)

	out, err := searchmessages.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		"",
		searchmessages.Input{Topic: topic, Script: `return value.indexOf("nothing-matches") >= 0`},
	)

	s.Require().NoError(err, "a search that matches nothing must still report what it scanned")
	s.Require().Len(out.ScannedRanges, 1,
		"the single partition that was searched must appear in the scan report")
	s.Require().EqualValues(0, out.ScannedRanges[0].Start,
		"the report must state the first offset scanned, so the caller knows what was covered")
	s.Require().EqualValues(2, out.ScannedRanges[0].End,
		"the report must state the exclusive end offset scanned")
}

func (s *SearchMessagesSuite) TestErrorsOnUnknownTopic() {
	_, err := searchmessages.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		"",
		searchmessages.Input{Topic: s.env.UniqueName("missing"), Script: `return true`},
	)

	s.Require().Error(err,
		"searching a topic that does not exist must fail, not look like a topic with no matches")
}

func (s *SearchMessagesSuite) TestOmittingTheScriptMatchesEveryMessage() {
	topic := s.env.CreateTopic(s.T(), "search-no-script")

	s.env.Produce(s.T(), topic,
		testenv.Message{Value: "one"},
		testenv.Message{Value: "two"},
	)

	out, err := searchmessages.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		"",
		searchmessages.Input{Topic: topic},
	)

	s.Require().NoError(err,
		"a search without a script is how a caller browses recent messages, and max_matches already bounds it")
	s.Require().Len(out.Matches, 2,
		"with no condition to apply every message matches")
}

func (s *SearchMessagesSuite) TestRejectsImpossibleParallelism() {
	topic := s.env.CreateTopic(s.T(), "search-bad-parallelism")

	_, err := searchmessages.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		"",
		searchmessages.Input{Topic: topic, Parallelism: 99},
	)

	s.Require().Error(err,
		"each reader is a connection, so an unbounded parallelism would let one search exhaust the broker's connection budget")
}

func (s *SearchMessagesSuite) TestErrorsWhenBrokerUnreachable() {
	_, err := searchmessages.Run(
		s.T().Context(),
		s.env.Admin(),
		records.NewReader("127.0.0.1:1"),
		"",
		searchmessages.Input{Topic: "anything", Script: `return true`},
	)

	s.Require().Error(err,
		"an unreachable broker must surface as an error, not as a search with no matches")
}
