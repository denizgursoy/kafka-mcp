package searchmessages_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/denizgursoy/kafka-mcp/internal/domain/records"
	"github.com/denizgursoy/kafka-mcp/internal/domain/testenv"
	"github.com/denizgursoy/kafka-mcp/internal/tools/searchmessages"
)

// The tool scans a partition in chunks of 500 offsets, newest chunk first. The
// fixture is built around that boundary: matches sit near the start, either
// side of the 500 mark and near the end, so a chunking mistake shows up as a
// missing or misordered match rather than passing unnoticed.
const (
	totalMessages = 1000
	chunkSize     = 500
)

// matchOffsets are deliberately far apart. Consecutive matches would let a
// scan that stops early, or one that mishandles a chunk edge, still return the
// right answer by luck.
var matchOffsets = []int64{7, 493, 500, 987}

type HighVolumeSuite struct {
	suite.Suite

	env   *testenv.Environment
	topic string
}

func TestHighVolumeSuite(t *testing.T) {
	suite.Run(t, new(HighVolumeSuite))
}

func (s *HighVolumeSuite) SetupSuite() {
	s.env = testenv.Start(s.T())
	s.topic = s.env.CreateTopic(s.T(), "high-volume")

	wanted := make(map[int64]bool, len(matchOffsets))
	for _, offset := range matchOffsets {
		wanted[offset] = true
	}

	messages := make([]testenv.Message, 0, totalMessages)

	for offset := int64(0); offset < totalMessages; offset++ {
		if wanted[offset] {
			messages = append(messages, testenv.Message{
				Key: fmt.Sprintf("order-%d", offset),
				Value: fmt.Sprintf(
					`{"eventType":"NEW","payload":{"amount":900,"orderId":"order-%d","cancelledAt":null}}`,
					offset,
				),
			})

			continue
		}

		messages = append(messages, testenv.Message{
			Key: fmt.Sprintf("noise-%d", offset),
			Value: fmt.Sprintf(
				`{"eventType":"OTHER","payload":{"amount":10,"orderId":"noise-%d"}}`,
				offset,
			),
		})
	}

	produced := s.env.Produce(s.T(), s.topic, messages...)

	s.Require().Len(produced, totalMessages,
		"the whole fixture must be produced, or the offset expectations below are meaningless")

	for i, offset := range produced {
		s.Require().EqualValues(i, offset,
			"records must land on consecutive offsets from 0, because every assertion here names exact offsets")
	}
}

func (s *HighVolumeSuite) TearDownSuite() {
	s.env.Stop()
}

func (s *HighVolumeSuite) offsets(messages []records.Message) []int64 {
	s.T().Helper()

	out := make([]int64, 0, len(messages))

	for _, message := range messages {
		out = append(out, message.Offset)
	}

	return out
}

func (s *HighVolumeSuite) TestFindsEveryMatchAcrossAThousandMessages() {
	out, err := searchmessages.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		"",
		searchmessages.Input{
			Topic:      s.topic,
			Script:     `return value.eventType === "NEW"`,
			MaxMatches: 50,
		},
	)

	s.Require().NoError(err, "searching a thousand messages must succeed")
	s.Require().Equal(
		[]int64{987, 500, 493, 7},
		s.offsets(out.Matches),
		"every planted match must be found exactly once and returned newest first, which also proves no chunk was skipped or read twice",
	)
	s.Require().EqualValues(totalMessages, out.ScannedMessages,
		"finding all matches requires reading every message, since Kafka cannot filter server-side")
	s.Require().True(out.Complete,
		"the whole range was read, so the caller may trust that these are all the matches")
}

func (s *HighVolumeSuite) TestNewestFirstStopsAfterTheNewestChunk() {
	out, err := searchmessages.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		"",
		searchmessages.Input{
			Topic:      s.topic,
			Script:     `return value.eventType === "NEW"`,
			MaxMatches: 2,
		},
	)

	s.Require().NoError(err, "a limited newest-first search must succeed")
	s.Require().Equal(
		[]int64{987, 500},
		s.offsets(out.Matches),
		"a newest-first search must return the two newest matches, not the two the scan happened to reach first",
	)
	s.Require().Equal("max_matches", out.StoppedReason,
		"the scan stopped at the limit, and the caller must be told so it does not read this as the whole topic")
	s.Require().False(out.Complete,
		"an early stop must never be reported as a complete scan")
	s.Require().EqualValues(chunkSize, out.ScannedMessages,
		"only the newest chunk needed reading, so scanning more would mean the search failed to stop early")
}

func (s *HighVolumeSuite) TestOldestFirstStopsAtTheSecondMatch() {
	out, err := searchmessages.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		"",
		searchmessages.Input{
			Topic:      s.topic,
			Script:     `return value.eventType === "NEW"`,
			Direction:  "oldest_first",
			MaxMatches: 2,
		},
	)

	s.Require().NoError(err, "a limited oldest-first search must succeed")
	s.Require().Equal(
		[]int64{7, 493},
		s.offsets(out.Matches),
		"an oldest-first search must return the two oldest matches",
	)
	s.Require().EqualValues(494, out.ScannedMessages,
		"the scan must stop on the second match at offset 493 rather than finish the chunk")
}

func (s *HighVolumeSuite) TestMatchExactlyOnTheChunkBoundary() {
	out, err := searchmessages.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		"",
		searchmessages.Input{
			Topic:  s.topic,
			Script: `return key === "order-500"`,
		},
	)

	s.Require().NoError(err, "searching for a key on the chunk boundary must succeed")
	s.Require().Equal(
		[]int64{500},
		s.offsets(out.Matches),
		"the first offset of a chunk must be read: an off-by-one at the boundary would lose it silently",
	)
}

func (s *HighVolumeSuite) TestExactKeySearchIgnoresSubstringsInValues() {
	out, err := searchmessages.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		"",
		searchmessages.Input{
			Topic:  s.topic,
			Script: `return key === "order-7"`,
		},
	)

	s.Require().NoError(err, "an exact key search must succeed")
	s.Require().Equal(
		[]int64{7},
		s.offsets(out.Matches),
		"exact key matching must find only order-7, not the longer keys that contain it as a prefix",
	)
}

func (s *HighVolumeSuite) TestCountOnlyReportsTotalsWithoutBodies() {
	out, err := searchmessages.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		"",
		searchmessages.Input{
			Topic:     s.topic,
			Script:    `return value.eventType === "NEW"`,
			CountOnly: true,
		},
	)

	s.Require().NoError(err, "counting matches must succeed")
	s.Require().Equal(len(matchOffsets), out.MatchCount,
		"count_only must report every match, not just the ones that would have fitted in max_matches")
	s.Require().Empty(out.Matches,
		"count_only exists to answer how many without returning bodies, so returning any would defeat it")
	s.Require().Len(out.MatchesByPart, 1,
		"the per-partition breakdown must cover the single partition that was searched")
	s.Require().Equal(len(matchOffsets), out.MatchesByPart[0].Matches,
		"the breakdown must account for every match found")
	s.Require().True(out.Complete,
		"counting reads the whole range, so the total is only meaningful if the scan completed")
}

func (s *HighVolumeSuite) TestJSONFilterSelectsByFieldValue() {
	out, err := searchmessages.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		"",
		searchmessages.Input{
			Topic:      s.topic,
			Script:     `return value.eventType === "NEW" && value.payload.amount >= 500`,
			MaxMatches: 50,
		},
	)

	s.Require().NoError(err, "a structured filter over a thousand messages must succeed")
	s.Require().Equal(
		[]int64{987, 500, 493, 7},
		s.offsets(out.Matches),
		"the filter must select exactly the planted messages, since only those carry eventType NEW with an amount of 900",
	)
	s.Require().Zero(out.ScriptErrors,
		"every message in this topic is JSON, so the script must not have failed on any of them")
}

func (s *HighVolumeSuite) TestFilterOnNullField() {
	out, err := searchmessages.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		"",
		searchmessages.Input{
			Topic:      s.topic,
			Script:     `return value.payload.cancelledAt === null`,
			MaxMatches: 50,
		},
	)

	s.Require().NoError(err, "filtering on an explicit null must succeed")
	s.Require().Equal(
		[]int64{987, 500, 493, 7},
		s.offsets(out.Matches),
		"is_null must match only the messages that carry the field set to null, not the noise messages that omit it entirely",
	)
}

func (s *HighVolumeSuite) TestQueryAndFilterMustBothHold() {
	out, err := searchmessages.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		"",
		searchmessages.Input{
			Topic:      s.topic,
			Script:     `return key === "order-493" && value.payload.amount >= 500`,
			MaxMatches: 50,
		},
	)

	s.Require().NoError(err, "combining a query and a filter must succeed")
	s.Require().Equal(
		[]int64{493},
		s.offsets(out.Matches),
		"only the message satisfying both the key query and the amount filter may be returned",
	)
}

func (s *HighVolumeSuite) TestScanCeilingReportsAnIncompleteSearch() {
	out, err := searchmessages.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		"",
		searchmessages.Input{
			Topic:      s.topic,
			Script:     `return value.eventType === "NEW"`,
			MaxScanned: 100,
			MaxMatches: 50,
		},
	)

	s.Require().NoError(err, "hitting the scan ceiling is a bounded outcome, not a failure")
	s.Require().Equal("max_scanned", out.StoppedReason,
		"the caller must learn the ceiling was hit, or an empty result reads as proof of absence")
	s.Require().False(out.Complete,
		"a scan that stopped at a ceiling has not covered the range and must not claim to be complete")
	s.Require().LessOrEqual(out.ScannedMessages, 100+chunkSize,
		"the ceiling must actually bound the work done rather than being reported after a full scan")

	for _, message := range out.Matches {
		s.Require().Contains(matchOffsets, message.Offset,
			"a truncated scan may return fewer matches, but never one that was not planted")
	}
}

func (s *HighVolumeSuite) TestWritesEveryMatchToFile() {
	dir := s.T().TempDir()

	out, err := searchmessages.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		dir,
		searchmessages.Input{
			Topic:      s.topic,
			Script:     `return value.eventType === "NEW"`,
			OutputFile: "matches.jsonl",
		},
	)

	s.Require().NoError(err, "exporting matches to a file must succeed")
	s.Require().Equal(len(matchOffsets), out.WrittenMessages,
		"every match must be written, not only the ones that would have fitted in max_matches")
	s.Require().Equal(filepath.Join(dir, "matches.jsonl"), out.OutputFile,
		"the file must be written inside the configured output directory")
	s.Require().Empty(out.Matches,
		"an exported search returns a path instead of bodies, so the caller's context is not flooded")

	content, err := os.ReadFile(out.OutputFile)
	s.Require().NoError(err, "the reported path must exist and be readable")

	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	s.Require().Len(lines, len(matchOffsets),
		"the file must hold one JSON message per line, one line per match")

	for _, line := range lines {
		var message records.Message

		s.Require().NoError(json.Unmarshal([]byte(line), &message),
			"each line must be a complete message that the caller can parse back")
		s.Require().Contains(matchOffsets, message.Offset,
			"only planted matches may be written to the file")
	}
}

func (s *HighVolumeSuite) TestRejectsWritingOutsideTheOutputDirectory() {
	dir := s.T().TempDir()

	_, err := searchmessages.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		dir,
		searchmessages.Input{
			Topic:      s.topic,
			Script:     `return value.eventType === "NEW"`,
			OutputFile: "../escaped.jsonl",
		},
	)

	s.Require().Error(err,
		"a file name that escapes the output directory must be refused, because the server must not write wherever a caller asks")
}

func (s *HighVolumeSuite) TestParallelScanFindsTheSameMatches() {
	out, err := searchmessages.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		"",
		searchmessages.Input{
			Topic:       s.topic,
			Script:      `return value.eventType === "NEW"`,
			MaxMatches:  50,
			Parallelism: 4,
		},
	)

	s.Require().NoError(err, "a parallel search must succeed")
	s.Require().ElementsMatch(
		matchOffsets,
		s.offsets(out.Matches),
		"splitting a partition between readers must find exactly the same messages as scanning it sequentially, or the result depends on how the work was divided",
	)
	s.Require().EqualValues(totalMessages, out.ScannedMessages,
		"every message must still be read exactly once: a slice boundary that overlapped or left a gap would show up here")
}

func (s *HighVolumeSuite) TestParallelScanSplitsASinglePartition() {
	out, err := searchmessages.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		"",
		searchmessages.Input{
			Topic:       s.topic,
			Script:      `return value.eventType === "NEW"`,
			MaxMatches:  50,
			Parallelism: 2,
		},
	)

	s.Require().NoError(err, "a parallel search over one partition must succeed")
	s.Require().Len(out.ScannedRanges, 1,
		"the topic has a single partition, and the report is per partition however many readers covered it")
	s.Require().EqualValues(0, out.ScannedRanges[0].Start,
		"the readers together must cover the partition from its first offset")
	s.Require().EqualValues(totalMessages, out.ScannedRanges[0].End,
		"the readers together must cover the partition to its end, which is what proves a single partition really was split")
}

func (s *HighVolumeSuite) TestParallelCountMatchesSequentialCount() {
	sequential, err := searchmessages.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		"",
		searchmessages.Input{
			Topic:     s.topic,
			Script:    `return value.payload.amount >= 500`,
			CountOnly: true,
		},
	)
	s.Require().NoError(err, "the sequential count must succeed")

	parallel, err := searchmessages.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		"",
		searchmessages.Input{
			Topic:       s.topic,
			Script:      `return value.payload.amount >= 500`,
			CountOnly:   true,
			Parallelism: 8,
		},
	)
	s.Require().NoError(err, "the parallel count must succeed")

	s.Require().Equal(sequential.MatchCount, parallel.MatchCount,
		"counting is the case parallelism helps most, and a count that changes with the number of readers would be worthless")
	s.Require().Equal(sequential.ScannedMessages, parallel.ScannedMessages,
		"both must read the whole range exactly once")
}

func (s *HighVolumeSuite) TestSmallPartitionIsNotSplit() {
	topic := s.env.CreateTopic(s.T(), "high-volume-small")

	s.env.Produce(s.T(), topic,
		testenv.Message{Value: `{"eventType":"NEW"}`},
		testenv.Message{Value: `{"eventType":"OTHER"}`},
	)

	out, err := searchmessages.Run(
		s.T().Context(),
		s.env.Admin(),
		s.env.Reader(),
		"",
		searchmessages.Input{
			Topic:       topic,
			Script:      `return value.eventType === "NEW"`,
			Parallelism: 8,
		},
	)

	s.Require().NoError(err,
		"asking for more readers than a tiny partition can use must not fail")
	s.Require().Len(out.Matches, 1,
		"the result must be correct regardless of how many readers were requested: eight connections to read two messages costs more than it saves, so the split is skipped")
}
