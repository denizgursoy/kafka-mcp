package script

import (
	"encoding/json"
	"fmt"
	"unicode/utf8"

	"github.com/dop251/goja"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/denizgursoy/kafka-mcp/internal/domain/serde"
)

// RecordParameters are the variables a script over a Kafka record sees, in the
// order RecordArguments supplies them.
//
// search_messages and transform_messages both bind a record this way, so a
// predicate written for one reads the same message the same way in the other.
var RecordParameters = []string{
	"value", "key", "headers", "partition", "offset", "timestamp",
	"value_bytes", "key_bytes", "format", "schema_id", "decode_error",
}

// RecordArguments renders a record as the values a script compiled with
// RecordParameters sees, given its value and key already decoded.
func RecordArguments(s *Script, record *kgo.Record, value, key serde.Decoded) ([]goja.Value, error) {
	runtime := s.Runtime()

	// A time.Time passed through ToValue arrives as a wrapped Go value with no
	// Date methods, so the timestamp is constructed as a real JavaScript Date.
	timestamp, err := runtime.New(
		runtime.Get("Date").ToObject(runtime),
		runtime.ToValue(record.Timestamp.UnixMilli()),
	)
	if err != nil {
		return nil, fmt.Errorf("build timestamp for partition %d offset %d: %w",
			record.Partition, record.Offset, err)
	}

	return []goja.Value{
		s.Value(RecordValue(record.Value, value)),
		s.Value(RecordKey(record.Key, key)),
		s.Value(RecordHeaders(record.Headers)),
		s.Value(record.Partition),
		s.Value(record.Offset),
		timestamp,
		s.Value(len(record.Value)),
		s.Value(len(record.Key)),
		s.Value(value.Format),
		s.Value(nullIfZero(value.SchemaID)),
		s.Value(nullIfEmpty(value.Error)),
	}, nil
}

// RecordValue turns a record value into what a script sees: the decoded
// document for JSON and every schema format, the raw text otherwise, so topics
// that do not hold JSON remain scriptable.
func RecordValue(raw []byte, decoded serde.Decoded) any {
	if len(raw) == 0 {
		return nil
	}

	if decoded.Document != nil {
		return decoded.Document
	}

	if decoded.Format == serde.FormatText {
		// A bare JSON scalar such as 42 or "abc" is text to the renderer but
		// has always reached scripts parsed, and a predicate written as
		// value === 42 must keep working.
		var parsed any
		if err := json.Unmarshal(raw, &parsed); err == nil {
			return parsed
		}

		return decoded.Text
	}

	// Binary payloads cannot be rendered as text without corrupting them, so
	// the script sees the bytes and can inspect their length or contents.
	return raw
}

// RecordKey renders a key for a script: the decoded document when the key has
// a schema, the string otherwise, and null when there is none, so a script can
// tell a missing key from an empty one.
func RecordKey(raw []byte, decoded serde.Decoded) any {
	if raw == nil {
		return nil
	}

	switch decoded.Format {
	case serde.FormatText, serde.FormatJSON:
		return string(raw)
	case serde.FormatBinary:
		return raw
	}

	if decoded.Document != nil {
		return decoded.Document
	}

	return string(raw)
}

// RecordHeaders renders headers as an object of name to string, keeping the
// bytes of a header that is not valid UTF-8.
func RecordHeaders(headers []kgo.RecordHeader) map[string]any {
	decoded := make(map[string]any, len(headers))

	for _, header := range headers {
		if utf8.Valid(header.Value) {
			decoded[header.Key] = string(header.Value)

			continue
		}

		decoded[header.Key] = header.Value
	}

	return decoded
}

// nullIfZero and nullIfEmpty hand the script null for an absent value, so a
// predicate can test for presence without knowing the zero value's meaning.
func nullIfZero(value int) any {
	if value == 0 {
		return nil
	}

	return value
}

func nullIfEmpty(value string) any {
	if value == "" {
		return nil
	}

	return value
}
