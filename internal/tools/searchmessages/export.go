package searchmessages

import (
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/denizgursoy/kafka-mcp/internal/domain/serde"
	"github.com/denizgursoy/kafka-mcp/internal/domain/transfer"
)

// ExportedMessage is one line of a download. It is the record layout
// kafka-console exports and imports, so a downloaded file can be imported
// there as it is.
//
// The file is a copy of the data, not a view of it: values are never cut, a
// null key or value stays null, and a payload is either the exact bytes
// (encoding utf8 or base64) or, when it carries a Schema Registry schema id,
// the decoded document with that id, which re-encodes to the same schema.
type ExportedMessage struct {
	Partition          int32            `json:"partition"`
	Offset             int64            `json:"offset"`
	Timestamp          time.Time        `json:"timestamp"`
	Key                *string          `json:"key"`
	KeyEncoding        string           `json:"key_encoding,omitempty"`
	KeyFormat          string           `json:"key_format,omitempty"`
	KeySchemaID        int              `json:"key_schema_id,omitempty"`
	KeySchemaMessage   string           `json:"key_schema_message,omitempty"`
	Value              *string          `json:"value"`
	ValueEncoding      string           `json:"value_encoding,omitempty"`
	ValueFormat        string           `json:"value_format,omitempty"`
	ValueSchemaID      int              `json:"value_schema_id,omitempty"`
	ValueSchemaMessage string           `json:"value_schema_message,omitempty"`
	Headers            []ExportedHeader `json:"headers"`
	KeyBytes           int              `json:"key_bytes"`
	ValueBytes         int              `json:"value_bytes"`
	Truncated          bool             `json:"truncated"`
}

// ExportedHeader keeps a header's bytes exact, including a null value.
type ExportedHeader struct {
	Key      string `json:"key"`
	Value    string `json:"value"`
	Encoding string `json:"encoding"`
	Null     bool   `json:"null,omitempty"`
}

// exportPart is one key or value as written to the file.
type exportPart struct {
	text     *string
	encoding string
	format   string
	schemaID int
	message  string
}

// schemaFormats are the formats a schema id lets an importer encode again.
var schemaFormats = map[string]bool{
	serde.FormatAvro:       true,
	serde.FormatProtobuf:   true,
	serde.FormatJSONSchema: true,
}

func exportedPart(data []byte, decoded serde.Decoded) exportPart {
	if data == nil {
		return exportPart{}
	}

	if decoded.Error == "" && decoded.SchemaID > 0 && schemaFormats[decoded.Format] {
		text := decoded.Text
		format := decoded.Format
		// kafka-console spells it with a hyphen.
		if format == serde.FormatJSONSchema {
			format = "json-schema"
		}

		return exportPart{text: &text, encoding: "utf8", format: format, schemaID: decoded.SchemaID, message: decoded.MessageType}
	}

	// Anything else is written as its bytes. A document decoded from a
	// configured format, or bytes that failed to decode, could not be
	// turned back into the same bytes from the decoded text.
	text, encoding := exactText(data)
	format := serde.FormatText

	switch {
	case encoding == "base64":
		format = serde.FormatBinary
	case json.Valid(data):
		format = serde.FormatJSON
	}

	return exportPart{text: &text, encoding: encoding, format: format}
}

func exactText(data []byte) (string, string) {
	if utf8.Valid(data) {
		return string(data), "utf8"
	}

	return base64.StdEncoding.EncodeToString(data), "base64"
}

func exportMessage(record *kgo.Record, key, value serde.Decoded) ExportedMessage {
	k, v := exportedPart(record.Key, key), exportedPart(record.Value, value)

	message := ExportedMessage{
		Partition:          record.Partition,
		Offset:             record.Offset,
		Timestamp:          record.Timestamp.UTC(),
		Key:                k.text,
		KeyEncoding:        k.encoding,
		KeyFormat:          k.format,
		KeySchemaID:        k.schemaID,
		KeySchemaMessage:   k.message,
		Value:              v.text,
		ValueEncoding:      v.encoding,
		ValueFormat:        v.format,
		ValueSchemaID:      v.schemaID,
		ValueSchemaMessage: v.message,
		Headers:            make([]ExportedHeader, 0, len(record.Headers)),
		KeyBytes:           len(record.Key),
		ValueBytes:         len(record.Value),
	}

	for _, header := range record.Headers {
		if header.Value == nil {
			message.Headers = append(message.Headers, ExportedHeader{Key: header.Key, Encoding: "utf8", Null: true})

			continue
		}

		text, encoding := exactText(header.Value)
		message.Headers = append(message.Headers, ExportedHeader{Key: header.Key, Value: text, Encoding: encoding})
	}

	return message
}

// csvColumns are kafka-console's CSV export columns, in its order.
var csvColumns = []string{
	"partition", "offset", "timestamp", "key", "value", "headers", "key_encoding", "value_encoding",
	"key_null", "value_null", "truncated", "key_format", "value_format", "key_schema_id", "value_schema_id",
	"key_schema_message", "value_schema_message", "decode_error",
}

func csvRow(message ExportedMessage) ([]string, error) {
	headers, err := json.Marshal(message.Headers)
	if err != nil {
		return nil, fmt.Errorf("encode headers: %w", err)
	}

	id := func(v int) string {
		if v == 0 {
			return ""
		}

		return strconv.Itoa(v)
	}

	text := func(v *string) string {
		if v == nil {
			return ""
		}

		return *v
	}

	return []string{
		strconv.FormatInt(int64(message.Partition), 10),
		strconv.FormatInt(message.Offset, 10),
		message.Timestamp.Format(time.RFC3339Nano),
		text(message.Key),
		text(message.Value),
		string(headers),
		message.KeyEncoding,
		message.ValueEncoding,
		strconv.FormatBool(message.Key == nil),
		strconv.FormatBool(message.Value == nil),
		strconv.FormatBool(message.Truncated),
		message.KeyFormat,
		message.ValueFormat,
		id(message.KeySchemaID),
		id(message.ValueSchemaID),
		message.KeySchemaMessage,
		message.ValueSchemaMessage,
		"",
	}, nil
}

// Export formats.
const (
	formatJSONL = "jsonl"
	formatCSV   = "csv"
)

// exporter writes matches as JSON lines or CSV to the caller's download.
type exporter struct {
	lines   *json.Encoder
	csv     *csv.Writer
	written int
}

func newExporter(w io.Writer, format string) (*exporter, error) {
	if format == formatCSV {
		c := csv.NewWriter(w)
		if err := c.Write(csvColumns); err != nil {
			return nil, err
		}

		return &exporter{csv: c}, nil
	}

	return &exporter{lines: json.NewEncoder(w)}, nil
}

func (e *exporter) write(record *kgo.Record, key, value serde.Decoded) error {
	message := exportMessage(record, key, value)
	if e.lines != nil {
		if err := e.lines.Encode(message); err != nil {
			return err
		}
		e.written++

		return nil
	}

	row, err := csvRow(message)
	if err != nil {
		return err
	}
	if err := e.csv.Write(row); err != nil {
		return err
	}
	e.written++

	return nil
}

// flush pushes buffered CSV rows out, so a download progresses as it reads.
func (e *exporter) flush() error {
	if e.csv == nil {
		return nil
	}
	e.csv.Flush()

	return e.csv.Error()
}

// downloadOptions is a validated Download.
type downloadOptions struct {
	format   string
	filename string
}

func newDownload(topic string, d *Download, local bool) (*downloadOptions, error) {
	if d == nil {
		return nil, nil
	}

	format := strings.ToLower(strings.TrimSpace(d.Format))
	switch format {
	case "", "json", "jsonl", "ndjson":
		format = formatJSONL
	case formatCSV:
	default:
		return nil, fmt.Errorf("download format %q is not jsonl or csv", d.Format)
	}

	filename := strings.TrimSpace(d.Filename)
	if filename == "" {
		filename = topic + "." + format
	}
	if err := transfer.CheckFilename(filename, local); err != nil {
		return nil, fmt.Errorf("download filename: %w", err)
	}

	return &downloadOptions{format: format, filename: filename}, nil
}
