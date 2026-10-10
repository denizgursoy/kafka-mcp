package searchmessages_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"github.com/twmb/avro"
	"github.com/twmb/franz-go/pkg/sr"

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

func (s *SearchMessagesSuite) TestScriptSeesDecodedSchemaRegistryFields() {
	topic := s.env.CreateTopic(s.T(), "search-avro")

	const schema = `{"type":"record","name":"Order","namespace":"shop","fields":[
		{"name":"id","type":"string"},{"name":"amount","type":"long"}]}`

	id := s.env.RegisterSchema(s.T(), topic+"-value", sr.Schema{Schema: schema})
	codec := avro.MustParse(schema)

	for _, amount := range []int64{100, 900, 300} {
		payload, err := codec.Encode(map[string]any{"id": fmt.Sprintf("o-%d", amount), "amount": amount})
		s.Require().NoError(err, "the fixture record must encode")

		s.env.Produce(s.T(), topic, testenv.Message{Value: testenv.WireFormat(s.T(), id, nil, payload)})
	}

	out, err := searchmessages.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		"",
		searchmessages.Input{Topic: topic, Script: `return value.amount >= 500`},
	)

	s.Require().NoError(err, "searching an Avro topic must succeed")
	s.Require().Zero(out.ScriptErrors,
		"the script must receive the decoded record; raw bytes would make value.amount throw on every message")
	s.Require().Len(out.Matches, 1,
		"a field condition must work on Avro exactly as it does on JSON, which is the point of decoding")
	s.Require().Equal(`{"amount":900,"id":"o-900"}`, out.Matches[0].Value,
		"the match must be shown decoded, so the caller can read what was found")
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

func (s *SearchMessagesSuite) groupTopic() string {
	topic := s.env.CreateTopic(s.T(), "search-group")
	s.env.Produce(s.T(), topic,
		testenv.Message{Value: `{"error":"timeout"}`},
		testenv.Message{Value: `{"error":"timeout"}`},
		testenv.Message{Value: `{"error":"bad_schema"}`},
		testenv.Message{Value: `{"error":"timeout"}`},
		testenv.Message{Value: `{"ok":true}`},
	)

	return topic
}

func (s *SearchMessagesSuite) TestGroupsMatchesByAnExpression() {
	topic := s.groupTopic()

	out, err := searchmessages.Run(s.T().Context(), s.env.Admin(), s.env.Reader(), "", searchmessages.Input{
		Topic:   topic,
		Script:  `return value.error !== undefined`,
		GroupBy: `return value.error`,
	})
	s.Require().NoError(err, "grouping must succeed")

	s.Run("buckets are counted and sorted by size", func() {
		s.Require().Len(out.Groups, 2, "two distinct error values matched")
		s.Require().Equal("timeout", out.Groups[0].Key, "the largest bucket comes first, which is the answer to 'what is filling the DLQ'")
		s.Require().Equal(3, out.Groups[0].Count, "three messages carry timeout")
		s.Require().Equal("bad_schema", out.Groups[1].Key, "the smaller bucket follows")
		s.Require().Equal(1, out.Groups[1].Count, "one message carries bad_schema")
	})
	s.Run("each bucket points at an example", func() {
		s.Require().EqualValues(2, out.Groups[1].Example.Offset,
			"a bucket is only actionable if the caller can open one of its messages with get_message")
	})
	s.Run("every match is counted and no bodies are returned", func() {
		s.Require().Equal(4, out.MatchCount, "grouping counts every match in the range, like count_only")
		s.Require().Empty(out.Matches, "grouping answers with counts, so bodies would only cost context")
		s.Require().True(out.Complete, "the whole range was read")
	})
}

func (s *SearchMessagesSuite) TestGroupingWithoutAScriptGroupsEveryMessage() {
	topic := s.groupTopic()

	out, err := searchmessages.Run(s.T().Context(), s.env.Admin(), s.env.Reader(), "", searchmessages.Input{
		Topic:   topic,
		GroupBy: `return format`,
	})
	s.Require().NoError(err, "grouping without a filter must succeed")

	s.Require().Len(out.Groups, 1, "every message is JSON")
	s.Require().Equal("json", out.Groups[0].Key, "the script scope, including format, is available to group_by")
	s.Require().Equal(5, out.Groups[0].Count, "every message is grouped when there is no filter")
}

func (s *SearchMessagesSuite) TestGroupingCapsTheNumberOfBuckets() {
	topic := s.groupTopic()

	out, err := searchmessages.Run(s.T().Context(), s.env.Admin(), s.env.Reader(), "", searchmessages.Input{
		Topic:     topic,
		GroupBy:   `return String(offset)`,
		MaxGroups: 2,
	})
	s.Require().NoError(err, "grouping past the cap must still succeed")

	s.Require().Len(out.Groups, 2, "no more buckets than max_groups are returned")
	s.Require().True(out.GroupsTruncated, "the caller must be told buckets were dropped, or the list reads as complete")
	s.Require().Equal(5, out.MatchCount, "dropped buckets are still counted in match_count")
}

func (s *SearchMessagesSuite) TestGroupingReportsNonStringKeysAndErrors() {
	topic := s.groupTopic()

	out, err := searchmessages.Run(s.T().Context(), s.env.Admin(), s.env.Reader(), "", searchmessages.Input{
		Topic:   topic,
		GroupBy: `if (value.ok) { throw new Error("boom") } return value.error`,
	})
	s.Require().NoError(err, "a group_by that throws on some messages must not fail the search")

	s.Require().Equal(1, out.ScriptErrors, "a message the expression threw on is counted, not silently bucketed")
	total := 0
	for _, group := range out.Groups {
		total += group.Count
	}
	s.Require().Equal(4, total, "only messages the expression returned a key for are bucketed")
}

func (s *SearchMessagesSuite) TestGroupingRefusesConflictingOptions() {
	search := func(input searchmessages.Input) error {
		input.Topic = "t"
		_, err := searchmessages.Run(s.T().Context(), s.env.Admin(), s.env.Reader(), "", input)
		return err
	}

	s.Require().ErrorContains(search(searchmessages.Input{GroupBy: `return 1`, OutputFile: "x.jsonl"}), "group_by",
		"grouping returns counts, so there are no messages to write to a file")
	s.Require().ErrorContains(search(searchmessages.Input{GroupBy: `return 1`, MaxGroups: 5000}), "max_groups",
		"an unbounded bucket count turns a summary back into a dump")
	s.Require().ErrorContains(search(searchmessages.Input{GroupBy: `return ===`}), "compile",
		"a malformed expression must be refused before anything is read")
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

func (s *SearchMessagesSuite) TestRejectsUnboundedInputs() {
	search := func(input searchmessages.Input) error {
		input.Topic = "t"
		_, err := searchmessages.Run(s.T().Context(), s.env.Admin(), s.env.Reader(), "", input)
		return err
	}

	s.Run("max_matches beyond the limit", func() {
		s.Require().ErrorContains(search(searchmessages.Input{MaxMatches: 1 << 40}), "max_matches",
			"max_matches sizes an allocation from caller input, so an unbounded value can crash the server")
	})
	s.Run("max_messages_scanned beyond the limit", func() {
		s.Require().ErrorContains(search(searchmessages.Input{MaxScanned: 1 << 40}), "max_messages_scanned",
			"an unbounded scan turns one call into a full read of the cluster")
	})
	s.Run("timeout_seconds beyond the limit", func() {
		s.Require().ErrorContains(search(searchmessages.Input{TimeoutSecond: 1 << 30}), "timeout_seconds",
			"a timeout of years is no timeout, and the call would hold its readers that long")
	})
	s.Run("max_value_bytes beyond the limit", func() {
		s.Require().ErrorContains(search(searchmessages.Input{MaxValueBytes: 1 << 40}), "max_value_bytes",
			"a response is held in memory and sent to a model, so it must stay bounded")
	})
	s.Run("negative limits", func() {
		s.Require().ErrorContains(search(searchmessages.Input{MaxMatches: -1}), "max_matches",
			"a negative limit is a caller mistake, not a request for the default")
	})
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

func (s *SearchMessagesSuite) TestRunawayScriptStopsAtTheTimeout() {
	topic := s.env.CreateTopic(s.T(), "search-runaway")

	s.env.Produce(s.T(), topic, testenv.Message{Value: `{"order":"1"}`})

	// A script that never returns is valid JavaScript, and goja does not yield,
	// so nothing outside the runtime can observe the deadline: the context
	// cannot stop a call that never comes back. Without the interrupt the
	// scanning goroutine is lost for the lifetime of the process, and the call
	// never answers.
	done := make(chan struct{})

	go func() {
		defer close(done)

		_, _ = searchmessages.Run(
			s.T().Context(),
			s.env.Admin(),
			s.env.Reader(),
			"",
			searchmessages.Input{
				Topic:         topic,
				Script:        `while (true) {} return true`,
				TimeoutSecond: 1,
			},
		)
	}()

	select {
	case <-done:
	case <-time.After(20 * time.Second):
		s.Require().Fail(
			"a runaway script must be interrupted at the timeout: the tool promises callers that scripts are time-limited, and a scan that never returns holds a goroutine forever")
	}
}

func (s *SearchMessagesSuite) TestNewestFirstSpansEveryPartition() {
	// Kafka orders records within a partition, never across them, so "the
	// newest matches in this topic" can only mean newest by timestamp. A scan
	// that drains one partition before looking at the next answers with
	// whichever partition it happened to read first, which is a confidently
	// wrong answer rather than a slow one.
	topic := s.env.CreateTopicWithPartitions(s.T(), "search-newest-across", 3)

	old := time.Now().Add(-24 * time.Hour)
	recent := time.Now().Add(-1 * time.Minute)

	s.env.Produce(s.T(), topic,
		// Partition 0 holds enough stale matches to satisfy max_matches on its
		// own, so a per-partition scan stops here and never reads the rest.
		testenv.Message{Value: "hit day-old a", Partition: 0, Timestamp: old},
		testenv.Message{Value: "hit day-old b", Partition: 0, Timestamp: old.Add(time.Second)},
		testenv.Message{Value: "hit day-old c", Partition: 0, Timestamp: old.Add(2 * time.Second)},

		// The genuinely newest matches are on later partitions.
		testenv.Message{Value: "hit minutes-old p1", Partition: 1, Timestamp: recent},
		testenv.Message{Value: "hit minutes-old p2", Partition: 2, Timestamp: recent.Add(time.Second)},
	)

	out, err := searchmessages.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		"",
		searchmessages.Input{Topic: topic, Script: hitScript, MaxMatches: 2},
	)

	s.Require().NoError(err, "a newest-first search across partitions must succeed")
	s.Require().Len(out.Matches, 2, "the match limit of two must be respected")

	partitions := []int32{out.Matches[0].Partition, out.Matches[1].Partition}

	s.Run("the newest matches are returned whichever partition holds them", func() {
		s.Require().NotContains(partitions, int32(0),
			"partition 0 holds only day-old matches, so returning one means the search answered from the partition it read first rather than from the whole topic")
		s.Require().ElementsMatch([]int32{2, 1}, partitions,
			"the two newest matches live on partitions 2 and 1, and newest_first must find them wherever they are")
	})

	s.Run("matches are ordered newest first across partitions", func() {
		s.Require().False(out.Matches[0].Timestamp.Before(out.Matches[1].Timestamp),
			"newest_first must order the merged result by time, or the caller cannot tell which of two partitions holds the more recent message")
	})
}

func (s *SearchMessagesSuite) TestNewestFirstReadsEveryPartitionBeforeStopping() {
	// The observable symptom of the same bug: a partition that was never read
	// cannot appear in scanned_ranges, and a caller checking coverage would be
	// told the search was complete when most of the topic went unexamined.
	topic := s.env.CreateTopicWithPartitions(s.T(), "search-newest-coverage", 3)

	s.env.Produce(s.T(), topic,
		testenv.Message{Value: "hit a", Partition: 0},
		testenv.Message{Value: "hit b", Partition: 0},
		testenv.Message{Value: "hit c", Partition: 1},
		testenv.Message{Value: "hit d", Partition: 2},
	)

	out, err := searchmessages.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		"",
		searchmessages.Input{Topic: topic, Script: hitScript, MaxMatches: 1},
	)

	s.Require().NoError(err, "the search must succeed")

	scanned := make([]int32, 0, len(out.ScannedRanges))
	for _, rng := range out.ScannedRanges {
		scanned = append(scanned, rng.Partition)
	}

	s.Require().ElementsMatch([]int32{0, 1, 2}, scanned,
		"every partition must be examined before a newest-first search can claim to have found the newest match, since another partition may hold a more recent one")
}
