package searchmessages

import (
	"context"
	"encoding/json"
	"fmt"
	"unicode/utf8"

	"github.com/dop251/goja"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/denizgursoy/kafka-mcp/internal/domain/script"
	"github.com/denizgursoy/kafka-mcp/internal/domain/serde"
)

// scriptParameters are the variables a search predicate sees, in the order
// match supplies them.
var scriptParameters = []string{
	"value", "key", "headers", "partition", "offset", "timestamp",
	"value_bytes", "key_bytes", "format", "schema_id", "decode_error",
}

// filter is a compiled user predicate bound to Kafka records.
//
// The runtime and its hardening live in internal/domain/script, which
// list_topics shares. What stays here is the part that is specific to a
// message: how a record's fields are rendered for JavaScript.
type filter struct {
	*script.Script
}

// compileScript prepares a user script for evaluation against records.
func compileScript(source string) (*filter, error) {
	compiled, err := script.Compile(source, scriptParameters...)
	if err != nil {
		return nil, err
	}

	return &filter{Script: compiled}, nil
}

// guard interrupts the script when ctx is done, so a predicate that never
// returns cannot hold a scanning goroutine forever.
func (f *filter) guard(ctx context.Context) func() {
	if f == nil {
		return func() {}
	}

	return script.Guard(ctx, f.Script, "search stopped")
}

// match reports whether a record satisfies the script, given its value and key
// already decoded, so a message is decoded once for both matching and showing.
//
// An error means the script failed on this message, which is different from
// the message not matching, and the caller counts the two separately.
func (f *filter) match(record *kgo.Record, value, key serde.Decoded) (bool, error) {
	arguments, err := f.arguments(record, value, key)
	if err != nil {
		return false, err
	}

	matched, err := f.Call(arguments...)
	if err != nil {
		return false, fmt.Errorf("script failed on partition %d offset %d: %w",
			record.Partition, record.Offset, err)
	}

	return matched, nil
}

// group evaluates a group_by expression and returns the bucket name. null and
// undefined become "null", so a missing field is its own visible bucket.
func (f *filter) group(record *kgo.Record, value, key serde.Decoded) (string, error) {
	arguments, err := f.arguments(record, value, key)
	if err != nil {
		return "", err
	}

	result, err := f.Evaluate(arguments...)
	if err != nil {
		return "", fmt.Errorf("group_by failed on partition %d offset %d: %w",
			record.Partition, record.Offset, err)
	}

	if goja.IsNull(result) || goja.IsUndefined(result) {
		return "null", nil
	}

	if _, isObject := result.(*goja.Object); isObject {
		encoded, err := json.Marshal(result.Export())
		if err == nil {
			return string(encoded), nil
		}
	}

	return result.String(), nil
}

// arguments renders a record as the values a script sees, in the order of
// scriptParameters.
func (f *filter) arguments(record *kgo.Record, value, key serde.Decoded) ([]goja.Value, error) {
	runtime := f.Runtime()

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
		f.Value(scriptValue(record.Value, value)),
		f.Value(scriptKey(record.Key, key)),
		f.Value(decodeHeaders(record.Headers)),
		f.Value(record.Partition),
		f.Value(record.Offset),
		timestamp,
		f.Value(len(record.Value)),
		f.Value(len(record.Key)),
		f.Value(value.Format),
		f.Value(nullIfZero(value.SchemaID)),
		f.Value(nullIfEmpty(value.Error)),
	}, nil
}

// scriptValue turns a record value into what the script sees: the decoded
// document for JSON and every schema format, the raw text otherwise, so topics
// that do not hold JSON remain searchable.
func scriptValue(raw []byte, decoded serde.Decoded) any {
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

// scriptKey renders a key for the script: the decoded document when the key
// has a schema, the string otherwise, and null when there is none, so a script
// can tell a missing key from an empty one.
func scriptKey(raw []byte, decoded serde.Decoded) any {
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

func decodeHeaders(headers []kgo.RecordHeader) map[string]any {
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
