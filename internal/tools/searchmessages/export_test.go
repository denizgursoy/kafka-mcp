package searchmessages_test

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/twmb/avro"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"

	"github.com/denizgursoy/kafka-mcp/internal/domain/testenv"
	"github.com/denizgursoy/kafka-mcp/internal/domain/transfer"
	"github.com/denizgursoy/kafka-mcp/internal/tools/searchmessages"
)

// ExportSuite checks a download writes the records exactly, in the layout
// kafka-console imports, so a file saved here can be loaded there unchanged.
type ExportSuite struct {
	suite.Suite

	env   *testenv.Environment
	topic string
	// schemaID is the Avro value schema the fourth record is framed with.
	schemaID int
}

func TestExportSuite(t *testing.T) {
	suite.Run(t, new(ExportSuite))
}

const exportSchema = `{"type":"record","name":"Order","namespace":"shop","fields":[
	{"name":"id","type":"string"},{"name":"amount","type":"long"}]}`

// longValue is far past max_value_bytes' default, which must not cut it.
var longValue = strings.Repeat("x", 2000)

func (s *ExportSuite) SetupSuite() {
	s.env = testenv.Start(s.T())
	s.topic = s.env.CreateTopic(s.T(), "export")
	s.schemaID = s.env.RegisterSchema(s.T(), s.topic+"-value", sr.Schema{Schema: exportSchema})

	payload, err := avro.MustParse(exportSchema).Encode(map[string]any{"id": "o-1", "amount": int64(900)})
	s.Require().NoError(err, "the Avro fixture must encode")

	// testenv.Produce cannot write a null key, value or header, which are
	// exactly what this suite checks, so the records are written directly.
	produced := s.env.Kafka().ProduceSync(s.T().Context(),
		&kgo.Record{Topic: s.topic, Key: nil, Value: nil,
			Headers: []kgo.RecordHeader{{Key: "gone", Value: nil}}},
		&kgo.Record{Topic: s.topic, Key: []byte(""), Value: []byte("")},
		&kgo.Record{Topic: s.topic, Key: []byte{0xff, 0x00}, Value: []byte{0x00, 0xfe},
			Headers: []kgo.RecordHeader{{Key: "raw", Value: []byte{0xff}}, {Key: "plain", Value: []byte("v")}}},
		&kgo.Record{Topic: s.topic, Key: []byte("o-1"),
			Value: []byte(testenv.WireFormat(s.T(), s.schemaID, nil, payload))},
		&kgo.Record{Topic: s.topic, Key: []byte("long"), Value: []byte(longValue)},
		&kgo.Record{Topic: s.topic, Key: []byte("doc"), Value: []byte(`{"id":9007199254740993}`)},
	)
	s.Require().NoError(produced.FirstErr(), "the fixture must be written")
}

func (s *ExportSuite) TearDownSuite() {
	s.env.Stop()
}

func (s *ExportSuite) export(format string) []byte {
	s.T().Helper()

	files, base := testenv.Files(s.T())
	out, err := searchmessages.Run(s.T().Context(), s.env.Admin(), s.env.Reader(), files, base, searchmessages.Input{
		Topic: s.topic, Direction: "oldest_first", Download: &searchmessages.Download{Format: format},
	})
	s.Require().NoError(err, "asking for the topic as a download must succeed")
	s.Require().NotNil(out.Download, "the response must carry the link")

	return testenv.Fetch(s.T(), *out.Download)
}

func (s *ExportSuite) lines() map[int64]searchmessages.ExportedMessage {
	s.T().Helper()

	byOffset := map[int64]searchmessages.ExportedMessage{}
	for line := range strings.Lines(string(s.export("jsonl"))) {
		var message searchmessages.ExportedMessage
		s.Require().NoError(json.Unmarshal([]byte(line), &message), "every line must be one JSON message")
		byOffset[message.Offset] = message
	}

	return byOffset
}

func (s *ExportSuite) TestJSONLinesKeepNullsDistinctFromEmpty() {
	lines := s.lines()

	s.Run("a null key and value are written as null", func() {
		s.Require().Nil(lines[0].Key, "a null key is a different record from an empty one, so it must stay null")
		s.Require().Nil(lines[0].Value, "a null value is a tombstone; writing it as empty would turn a delete into data")
		s.Require().Equal([]searchmessages.ExportedHeader{{Key: "gone", Encoding: "utf8", Null: true}}, lines[0].Headers,
			"a header with no value must say so, or an import gives it an empty one")
	})

	s.Run("an empty key and value are written as empty strings", func() {
		s.Require().NotNil(lines[1].Key, "an empty key must not be confused with a null one")
		s.Require().Equal("", *lines[1].Key, "an empty key is written as an empty string")
		s.Require().NotNil(lines[1].Value, "an empty value must not be confused with a tombstone")
		s.Require().Equal("", *lines[1].Value, "an empty value is written as an empty string")
	})
}

func (s *ExportSuite) TestJSONLinesKeepBinaryBytesExact() {
	message := s.lines()[2]

	s.Require().Equal("/wA=", *message.Key, "a key that is not UTF-8 must be written as base64 of its bytes")
	s.Require().Equal("base64", message.KeyEncoding, "the key's encoding must say base64, so an import writes the bytes back")
	s.Require().Equal("AP4=", *message.Value, "a binary value must be written as base64 of its bytes")
	s.Require().Equal("base64", message.ValueEncoding, "the value's encoding must say base64, so an import writes the bytes back")
	s.Require().Equal([]searchmessages.ExportedHeader{
		{Key: "raw", Value: "/w==", Encoding: "base64"},
		{Key: "plain", Value: "v", Encoding: "utf8"},
	}, message.Headers, "headers must keep their order and their exact bytes")
}

func (s *ExportSuite) TestJSONLinesKeepTheSchemaOfARegistryValue() {
	message := s.lines()[3]

	s.Require().Equal(`{"amount":900,"id":"o-1"}`, *message.Value,
		"a registry value is written decoded, which is the only form a person can read or edit")
	s.Require().Equal("avro", message.ValueFormat, "the format must name the schema type to encode with")
	s.Require().Equal(s.schemaID, message.ValueSchemaID,
		"the schema id must be kept, or an import writes plain JSON where consumers expect Avro")
	s.Require().Equal("shop.Order", message.ValueSchemaMessage, "the record type must be kept with the id")
	s.Require().Equal("utf8", message.KeyEncoding, "a text key is written as it is")
	s.Require().Zero(message.KeySchemaID, "a plain key next to an Avro value has no schema")
}

func (s *ExportSuite) TestJSONLinesNeverCutAValue() {
	lines := s.lines()

	s.Require().Equal(longValue, *lines[4].Value,
		"an export is a copy, so max_value_bytes, which bounds what reaches the conversation, must not cut it")
	s.Require().False(lines[4].Truncated, "a value written in full must not be flagged as truncated")
	s.Require().Equal(`{"id":9007199254740993}`, *lines[5].Value,
		"a JSON value must be written as its exact text, without rounding large integers")
	s.Require().Equal("json", lines[5].ValueFormat, "a JSON value is reported as json")
}

func (s *ExportSuite) TestCSVUsesKafkaConsoleColumns() {
	rows, err := csv.NewReader(strings.NewReader(string(s.export("csv")))).ReadAll()
	s.Require().NoError(err, "a csv download must be valid CSV")
	s.Require().Len(rows, 7, "the file holds a header row and one row per record")

	column := map[string]int{}
	for i, name := range rows[0] {
		column[name] = i
	}
	for _, name := range []string{"key", "value", "headers", "key_encoding", "value_encoding", "key_null", "value_null",
		"value_format", "value_schema_id", "value_schema_message", "truncated"} {
		s.Require().Contains(column, name, fmt.Sprintf("kafka-console's importer reads the %s column", name))
	}

	cell := func(row int, name string) string { return rows[row][column[name]] }

	s.Run("null and empty are told apart by the null columns", func() {
		s.Require().Equal("true", cell(1, "key_null"), "a CSV cell cannot be null, so the null column must carry it")
		s.Require().Equal("true", cell(1, "value_null"), "a tombstone must be marked as null, not left empty")
		s.Require().Equal("false", cell(2, "value_null"), "an empty value is not a tombstone")
	})

	s.Run("binary and schema values keep what an import needs", func() {
		s.Require().Equal("AP4=", cell(3, "value"), "a binary value is written as base64")
		s.Require().Equal("base64", cell(3, "value_encoding"), "and its encoding says so")
		s.Require().Equal(fmt.Sprint(s.schemaID), cell(4, "value_schema_id"), "the schema id must reach the CSV")
		s.Require().Equal("shop.Order", cell(4, "value_schema_message"), "with its record type")
	})

	s.Run("headers are a JSON list of exact bytes", func() {
		var headers []searchmessages.ExportedHeader
		s.Require().NoError(json.Unmarshal([]byte(cell(3, "headers")), &headers), "the headers cell must be JSON")
		s.Require().Equal("base64", headers[0].Encoding, "a binary header must keep its encoding in CSV too")
	})

	s.Run("long values are written in full", func() {
		s.Require().Equal(longValue, cell(5, "value"), "CSV exports must not cut values either")
	})
}

func (s *ExportSuite) TestDownloadRunsOnlyWhenFetchedAndOnlyOnce() {
	files, base := testenv.Files(s.T())
	out, err := searchmessages.Run(s.T().Context(), s.env.Admin(), s.env.Reader(), files, base, searchmessages.Input{
		Topic: s.topic, Download: &searchmessages.Download{Format: "csv", Filename: "orders.csv"},
	})
	s.Require().NoError(err, "asking for a download must succeed")

	s.Run("nothing is read until the link is fetched", func() {
		s.Require().Equal("not_started", out.StoppedReason, "the response must say the search has not run")
		s.Require().Zero(out.ScannedMessages, "a download link reads nothing by itself")
		s.Require().Empty(out.Matches, "a download returns a link, not messages")
	})

	s.Run("the command saves to the user's machine under the asked name", func() {
		s.Require().Equal(http.MethodGet, out.Download.Method, "a download is fetched with GET")
		s.Require().Contains(out.Download.Command, "-o 'orders.csv.part'",
			"curl must save to a partial file, so a cut-short download never looks finished")
		s.Require().Contains(out.Download.Command, "&& mv 'orders.csv.part' 'orders.csv'",
			"the file gets its name only once the download succeeded")
	})

	body := testenv.Fetch(s.T(), *out.Download)
	s.Require().True(strings.HasPrefix(string(body), "partition,offset,"), "the download must be the CSV asked for")

	s.Run("the status link reports the finished search", func() {
		code, reply := testenv.Send(s.T(), transfer.Ticket{Method: http.MethodGet, URL: out.Download.StatusURL}, "")
		s.Require().Equal(http.StatusOK, code, "the status of a used link must be readable")
		var status struct {
			State  string
			Result searchmessages.Output
		}
		s.Require().NoError(json.Unmarshal(reply, &status), "the status must be JSON")
		s.Require().Equal("done", status.State, "a finished download must report done")
		s.Require().Equal(6, status.Result.WrittenMessages, "the status must count what the file holds")
		s.Require().True(status.Result.Complete, "the whole range was read")
	})

	s.Run("a second fetch is refused", func() {
		code, _ := testenv.Send(s.T(), transfer.Ticket{Method: http.MethodGet, URL: out.Download.URL}, "")
		s.Require().Equal(http.StatusNotFound, code, "a link is a one-time credential")
	})
}

func (s *ExportSuite) TestADownloadCutShortFailsInsteadOfLookingComplete() {
	files, base := testenv.Files(s.T())
	out, err := searchmessages.Run(s.T().Context(), s.env.Admin(), s.env.Reader(), files, base, searchmessages.Input{
		Topic: s.topic, MaxScanned: 2, Download: &searchmessages.Download{},
	})
	s.Require().NoError(err, "asking for a download must succeed")

	response, err := http.Get(out.Download.URL) //nolint:noctx // test
	if err == nil {
		_, err = io.ReadAll(response.Body)
		response.Body.Close()
	}
	s.Require().Error(err,
		"a download stopped by max_messages_scanned must break the transfer, so curl -f fails rather than keeping part of the topic")

	code, reply := testenv.Send(s.T(), transfer.Ticket{Method: http.MethodGet, URL: out.Download.StatusURL}, "")
	s.Require().Equal(http.StatusOK, code, "the status must explain the failure")
	s.Require().Contains(string(reply), "incomplete", "the status must say the file is incomplete and why")
}

func (s *ExportSuite) TestDownloadMistakesAreReportedAtOnce() {
	files, base := testenv.Files(s.T())
	run := func(input searchmessages.Input) error {
		_, err := searchmessages.Run(s.T().Context(), s.env.Admin(), s.env.Reader(), files, base, input)
		return err
	}

	s.Run("an unknown topic", func() {
		s.Require().Error(run(searchmessages.Input{Topic: "no-such-topic", Download: &searchmessages.Download{}}),
			"a topic that does not exist must be refused before a link is handed out")
	})
	s.Run("a broken script", func() {
		s.Require().Error(run(searchmessages.Input{Topic: s.topic, Script: "return (", Download: &searchmessages.Download{}}),
			"a script that does not compile must be refused before a link is handed out")
	})
	s.Run("an unknown format", func() {
		s.Require().ErrorContains(run(searchmessages.Input{Topic: s.topic, Download: &searchmessages.Download{Format: "xml"}}),
			"not jsonl or csv", "only formats the importer reads are offered")
	})
	s.Run("a path as the file name", func() {
		s.Require().ErrorContains(run(searchmessages.Input{Topic: s.topic, Download: &searchmessages.Download{Filename: "../x.csv"}}),
			"not a path", "the suggested command must save where the user is working")
	})
	s.Run("no HTTP transport", func() {
		_, err := searchmessages.Run(s.T().Context(), s.env.Admin(), s.env.Reader(), nil, "", searchmessages.Input{
			Topic: s.topic, Download: &searchmessages.Download{},
		})
		s.Require().ErrorContains(err, "HTTP transport", "over stdio there is no address for a link")
	})
}
